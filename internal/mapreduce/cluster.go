package mapreduce

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type RPCMapRequest struct {
	Chunk           ChunkSpec     `json:"chunk"`
	Attempt         int           `json:"attempt"`
	Speculative     bool          `json:"speculative"`
	SimulateCrash   bool          `json:"simulate_crash"`
	StragglerDelay  time.Duration `json:"straggler_delay"`
	UseHistogram    bool          `json:"use_histogram"`
}

type RPCMapResponse struct {
	ChunkID     int                        `json:"chunk_id"`
	Attempt     int                        `json:"attempt"`
	Speculative bool                       `json:"speculative"`
	Records     int64                      `json:"records"`
	KVs         map[string]*IntermediateKV `json:"kvs"`
	Error       string                     `json:"error,omitempty"`
}

type ClusterJobRequest struct {
	Records         int  `json:"records"`
	Speculative     bool `json:"speculative"`
	InjectCrash     bool `json:"inject_crash"`
	InjectStraggler bool `json:"inject_straggler"`
}

type ClusterJobResponse struct {
	JobID             string              `json:"job_id"`
	Scenario          string              `json:"scenario"`
	Records           int64               `json:"records"`
	Chunks            int                 `json:"chunks"`
	Workers           int                 `json:"workers"`
	WallTimeMs        float64             `json:"wall_time_ms"`
	ThroughputRecSec  float64             `json:"throughput_rec_sec"`
	Speedup           float64             `json:"speedup"`
	FailuresRecovered int                 `json:"failures_recovered"`
	SpeculativeWins   int                 `json:"speculative_wins"`
	DataLossPct       float64             `json:"data_loss_pct"`
	Endpoints         []EndpointAggregate `json:"endpoints"`
}

func NewWorkerMux(workerID string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"worker_id": workerID,
			"status":    "ONLINE",
		})
	})

	mux.HandleFunc("/map", func(w http.ResponseWriter, r *http.Request) {
		var req RPCMapRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		res := executeMapChunk(
			r.Context(),
			chunkTask{
				spec:        req.Chunk,
				attempt:     req.Attempt,
				speculative: req.Speculative,
			},
			req.SimulateCrash,
			false,
			req.StragglerDelay,
			2*time.Second,
			req.UseHistogram,
		)

		w.Header().Set("Content-Type", "application/json")
		if res.err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(RPCMapResponse{
				ChunkID:     res.chunkID,
				Attempt:     res.attempt,
				Speculative: res.speculative,
				Error:       res.err.Error(),
			})
			return
		}

		_ = json.NewEncoder(w).Encode(RPCMapResponse{
			ChunkID:     res.chunkID,
			Attempt:     res.attempt,
			Speculative: res.speculative,
			Records:     res.records,
			KVs:         res.kvs,
		})
	})
	return mux
}

func ExecuteHTTPClusterJob(
	logPath string,
	workerURLs []string,
	req ClusterJobRequest,
	expectedRecords int64,
	baselineDur time.Duration,
) (*ClusterJobResponse, error) {
	if len(workerURLs) == 0 {
		return nil, fmt.Errorf("no worker URLs configured")
	}

	startWall := time.Now()
	chunks, err := SplitFileIntoChunks(logPath, 256*1024)
	if err != nil {
		return nil, err
	}

	client := &http.Client{Timeout: 2 * time.Second}
	var rrCounter uint64

	type rpcOutcome struct {
		resp RPCMapResponse
		err  error
	}

	taskCh := make(chan chunkTask, len(chunks)*4+4)
	specTaskCh := make(chan chunkTask, len(chunks)*2+4)
	doneCh := make(chan struct{})
	outcomeCh := make(chan rpcOutcome, len(chunks)*4+4)

	var flightMu sync.Mutex
	inFlight := make(map[int]*inFlightState, len(chunks))

	for _, c := range chunks {
		taskCh <- chunkTask{spec: c, attempt: 1, speculative: false}
	}

	var wg sync.WaitGroup
	for w := 0; w < len(workerURLs)*3; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				var task chunkTask
				select {
				case <-doneCh:
					return
				case task = <-specTaskCh:
				default:
					select {
					case <-doneCh:
						return
					case task = <-specTaskCh:
					case task = <-taskCh:
					}
				}

				flightMu.Lock()
				st := inFlight[task.spec.ID]
				if st != nil && st.completed {
					flightMu.Unlock()
					continue
				}
				ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
				if st == nil {
					inFlight[task.spec.ID] = &inFlightState{
						startedAt:     time.Now(),
						cancelPrimary: cancel,
					}
				}
				flightMu.Unlock()

				wIdx := int(atomic.AddUint64(&rrCounter, 1)-1) % len(workerURLs)
				targetURL := workerURLs[wIdx] + "/map"

				simCrash := req.InjectCrash && (task.spec.ID == 2 || task.spec.ID == 5) && task.attempt == 1 && !task.speculative
				var stragglerDelay time.Duration
				if req.InjectStraggler && (task.spec.ID == 1 || task.spec.ID == 4) && task.attempt == 1 && !task.speculative {
					stragglerDelay = 120 * time.Millisecond
				}

				payload, _ := json.Marshal(RPCMapRequest{
					Chunk:          task.spec,
					Attempt:        task.attempt,
					Speculative:    task.speculative,
					SimulateCrash:  simCrash,
					StragglerDelay: stragglerDelay,
					UseHistogram:   false,
				})

				httpReq, _ := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(payload))
				httpReq.Header.Set("Content-Type", "application/json")
				httpResp, err := client.Do(httpReq)
				if err != nil {
					cancel()
					outcomeCh <- rpcOutcome{
						resp: RPCMapResponse{ChunkID: task.spec.ID, Attempt: task.attempt, Speculative: task.speculative},
						err:  err,
					}
					continue
				}

				var rpcResp RPCMapResponse
				decodeErr := json.NewDecoder(httpResp.Body).Decode(&rpcResp)
				_ = httpResp.Body.Close()
				cancel()

				if httpResp.StatusCode != http.StatusOK || decodeErr != nil || rpcResp.Error != "" {
					outcomeCh <- rpcOutcome{
						resp: RPCMapResponse{ChunkID: task.spec.ID, Attempt: task.attempt, Speculative: task.speculative},
						err:  fmt.Errorf("worker HTTP %d: %s", httpResp.StatusCode, rpcResp.Error),
					}
					continue
				}

				outcomeCh <- rpcOutcome{resp: rpcResp, err: nil}
			}
		}()
	}

	stopSpec := make(chan struct{})
	specDone := make(chan struct{})
	if req.Speculative {
		go func() {
			defer close(specDone)
			ticker := time.NewTicker(2 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-stopSpec:
					return
				case <-ticker.C:
					now := time.Now()
					flightMu.Lock()
					for cid, st := range inFlight {
						if !st.completed && !st.backupLaunched && now.Sub(st.startedAt) >= 20*time.Millisecond {
							st.backupLaunched = true
							specTaskCh <- chunkTask{
								spec:        chunks[cid],
								attempt:     2,
								speculative: true,
							}
						}
					}
					flightMu.Unlock()
				}
			}
		}()
	} else {
		close(specDone)
	}

	committed := make(map[int]map[string]*IntermediateKV, len(chunks))
	var committedRecords int64
	var failuresRecovered int
	var speculativeWins int

	for len(committed) < len(chunks) {
		out := <-outcomeCh
		cid := out.resp.ChunkID
		if _, done := committed[cid]; done {
			continue
		}
		if out.err != nil {
			flightMu.Lock()
			st := inFlight[cid]
			backedUp := st != nil && st.backupLaunched
			flightMu.Unlock()
			if backedUp {
				continue
			}
			failuresRecovered++
			specTaskCh <- chunkTask{
				spec:        chunks[cid],
				attempt:     out.resp.Attempt + 1,
				speculative: false,
			}
			continue
		}

		flightMu.Lock()
		st := inFlight[cid]
		if st != nil {
			st.completed = true
			if out.resp.Speculative && st.cancelPrimary != nil {
				st.cancelPrimary()
			}
		}
		flightMu.Unlock()

		if out.resp.Speculative {
			speculativeWins++
		}
		committed[cid] = out.resp.KVs
		committedRecords += out.resp.Records
	}

	close(stopSpec)
	<-specDone
	close(doneCh)
	wg.Wait()

	// Reduce across endpoints
	mergedByEp := make(map[string]*IntermediateKV)
	for _, cmap := range committed {
		for ep, kv := range cmap {
			m, exists := mergedByEp[ep]
			if !exists {
				m = &IntermediateKV{
					Endpoint:        ep,
					LatenciesMicros: make([]int64, 0, 8192),
				}
				mergedByEp[ep] = m
			}
			m.Count += kv.Count
			m.ErrorCount += kv.ErrorCount
			m.BytesSum += kv.BytesSum
			m.LatencySumMicros += kv.LatencySumMicros
			m.SignatureXor ^= kv.SignatureXor
			m.LatenciesMicros = append(m.LatenciesMicros, kv.LatenciesMicros...)
		}
	}

	summaries := make([]EndpointAggregate, 0, len(mergedByEp))
	for ep, kv := range mergedByEp {
		summaries = append(summaries, finalizeEndpointAggregate(ep, kv))
	}
	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].Endpoint < summaries[j].Endpoint
	})

	wallElapsed := time.Since(startWall)
	wallMs := float64(wallElapsed.Nanoseconds()) / 1e6
	wallSec := math.Max(1e-9, wallElapsed.Seconds())
	speedup := 1.0
	if wallElapsed > 0 && baselineDur > 0 {
		speedup = float64(baselineDur) / float64(wallElapsed)
	}

	var dataLossPct float64
	if expectedRecords > 0 && committedRecords != expectedRecords {
		dataLossPct = math.Abs(float64(expectedRecords-committedRecords)) / float64(expectedRecords) * 100.0
	}

	return &ClusterJobResponse{
		JobID:             fmt.Sprintf("mr-%d", time.Now().UnixMilli()%100000),
		Scenario:          "HTTP Multi-Node Cluster",
		Records:           committedRecords,
		Chunks:            len(chunks),
		Workers:           len(workerURLs),
		WallTimeMs:        math.Round(wallMs*100) / 100,
		ThroughputRecSec:  math.Round(float64(committedRecords)/wallSec*100) / 100,
		Speedup:           math.Round(speedup*100) / 100,
		FailuresRecovered: failuresRecovered,
		SpeculativeWins:   speculativeWins,
		DataLossPct:       dataLossPct,
		Endpoints:         summaries,
	}, nil
}

func StartEmbeddedCluster(coordinatorAddr string, workerAddrs []string) (func(), error) {
	var servers []*http.Server
	var workerURLs []string

	for i, wAddr := range workerAddrs {
		ln, err := net.Listen("tcp", wAddr)
		if err != nil {
			return nil, err
		}
		srv := &http.Server{Handler: NewWorkerMux(fmt.Sprintf("worker-%d", i+1))}
		go func() { _ = srv.Serve(ln) }()
		servers = append(servers, srv)
		workerURLs = append(workerURLs, "http://"+ln.Addr().String())
	}

	tmpDir, err := os.MkdirTemp("", "mr-http-cluster-*")
	if err != nil {
		return nil, err
	}

	var cachedMu sync.Mutex
	cachedFiles := make(map[int]struct {
		path    string
		records int64
		baseDur time.Duration
	})

	coordMux := http.NewServeMux()
	coordMux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":  "ONLINE",
			"workers": workerURLs,
		})
	})

	coordMux.HandleFunc("/api/job", func(w http.ResponseWriter, r *http.Request) {
		var req ClusterJobRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if req.Records <= 0 {
			req.Records = 200_000
		}

		cachedMu.Lock()
		entry, ok := cachedFiles[req.Records]
		if !ok {
			p := filepath.Join(tmpDir, fmt.Sprintf("cluster_%d.log", req.Records))
			_, _ = GenerateSyntheticLogFile(p, req.Records, 2026)
			_, recs, bDur, _ := RunSingleThreadedBaseline(p)
			entry = struct {
				path    string
				records int64
				baseDur time.Duration
			}{path: p, records: recs, baseDur: bDur}
			cachedFiles[req.Records] = entry
		}
		cachedMu.Unlock()

		resp, err := ExecuteHTTPClusterJob(entry.path, workerURLs, req, entry.records, entry.baseDur)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})

	cLn, err := net.Listen("tcp", coordinatorAddr)
	if err != nil {
		return nil, err
	}
	cSrv := &http.Server{Handler: coordMux}
	go func() { _ = cSrv.Serve(cLn) }()
	servers = append(servers, cSrv)

	cleanup := func() {
		for _, s := range servers {
			_ = s.Close()
		}
		_ = os.RemoveAll(tmpDir)
	}
	return cleanup, nil
}
