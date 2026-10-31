package mapreduce

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"sync"
	"time"
)

type chunkTask struct {
	spec        ChunkSpec
	attempt     int
	speculative bool
}

type mapAttemptResult struct {
	chunkID     int
	attempt     int
	speculative bool
	records     int64
	kvs         map[string]*IntermediateKV
	err         error
	failure     FailureType
}

type inFlightState struct {
	startedAt      time.Time
	cancelPrimary  context.CancelFunc
	backupLaunched bool
	completed      bool
}

func partitionForKey(key string, numReducers int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(numReducers))
}

func executeMapChunk(
	ctx context.Context,
	task chunkTask,
	simulateCrash bool,
	simulateTimeout bool,
	stragglerDelay time.Duration,
	taskTimeout time.Duration,
	useHistogram bool,
) mapAttemptResult {
	f, err := os.Open(task.spec.FilePath)
	if err != nil {
		return mapAttemptResult{
			chunkID:     task.spec.ID,
			attempt:     task.attempt,
			speculative: task.speculative,
			err:         err,
		}
	}
	defer f.Close()

	section := io.NewSectionReader(f, task.spec.Offset, task.spec.Length)
	reader := bufio.NewReaderSize(section, 256*1024)

	localMap := make(map[string]*IntermediateKV, 16)
	var records int64
	var bytesRead int64
	halfChunk := task.spec.Length / 2
	stragglerInjected := false

	for {
		select {
		case <-ctx.Done():
			return mapAttemptResult{
				chunkID:     task.spec.ID,
				attempt:     task.attempt,
				speculative: task.speculative,
				err:         ctx.Err(),
				failure:     FailureTimeout,
			}
		default:
		}

		if bytesRead >= halfChunk && task.attempt == 1 && !task.speculative {
			if simulateCrash {
				return mapAttemptResult{
					chunkID:     task.spec.ID,
					attempt:     task.attempt,
					speculative: task.speculative,
					err:         errors.New("simulated worker process crash mid-chunk"),
					failure:     FailureCrash,
				}
			}
			if simulateTimeout {
				select {
				case <-time.After(taskTimeout + 50*time.Millisecond):
				case <-ctx.Done():
				}
				return mapAttemptResult{
					chunkID:     task.spec.ID,
					attempt:     task.attempt,
					speculative: task.speculative,
					err:         errors.New("simulated worker lease timeout"),
					failure:     FailureTimeout,
				}
			}
			if stragglerDelay > 0 && !stragglerInjected {
				stragglerInjected = true
				select {
				case <-time.After(stragglerDelay):
				case <-ctx.Done():
					return mapAttemptResult{
						chunkID:     task.spec.ID,
						attempt:     task.attempt,
						speculative: task.speculative,
						err:         ctx.Err(),
						failure:     FailureTimeout,
					}
				}
			}
		}

		line, err := reader.ReadSlice('\n')
		if len(line) > 0 {
			bytesRead += int64(len(line))
			ep, status, lat, bSent, sig, ok := parseLogLine(line)
			if ok {
				records++
				kv, exists := localMap[ep]
				if !exists {
					kv = &IntermediateKV{Endpoint: ep}
					if useHistogram {
						kv.LatencyBuckets = make([]uint32, MaxHistogramMicros)
					} else {
						kv.LatenciesMicros = make([]int64, 0, 512)
					}
					localMap[ep] = kv
				}
				kv.Count++
				if status >= 400 {
					kv.ErrorCount++
				}
				kv.BytesSum += bSent
				kv.LatencySumMicros += lat
				if useHistogram {
					bIdx := int(lat)
					if bIdx < 0 {
						bIdx = 0
					} else if bIdx >= MaxHistogramMicros {
						bIdx = MaxHistogramMicros - 1
					}
					kv.LatencyBuckets[bIdx]++
				} else {
					kv.LatenciesMicros = append(kv.LatenciesMicros, lat)
				}
				kv.SignatureXor ^= sig
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil && !errors.Is(err, bufio.ErrBufferFull) {
			return mapAttemptResult{
				chunkID:     task.spec.ID,
				attempt:     task.attempt,
				speculative: task.speculative,
				err:         err,
			}
		}
	}

	return mapAttemptResult{
		chunkID:     task.spec.ID,
		attempt:     task.attempt,
		speculative: task.speculative,
		records:     records,
		kvs:         localMap,
	}
}

func RunMapReduceJob(
	filePath string,
	cfg EngineConfig,
	expectedRecords int64,
	baselineDuration time.Duration,
) (*JobMetrics, error) {
	if cfg.NumMapWorkers <= 0 {
		cfg.NumMapWorkers = runtime.NumCPU()
	}
	if cfg.NumReduceWorkers <= 0 {
		cfg.NumReduceWorkers = runtime.NumCPU()
	}
	if cfg.TargetChunkBytes <= 0 {
		cfg.TargetChunkBytes = 2 * 1024 * 1024
	}
	if cfg.TaskTimeout <= 0 {
		cfg.TaskTimeout = 250 * time.Millisecond
	}
	if cfg.SpeculativeThreshold <= 0 {
		cfg.SpeculativeThreshold = 6 * time.Millisecond
	}

	var profFile *os.File
	if cfg.EnableCPUProfile && cfg.CPUProfilePath != "" {
		var err error
		profFile, err = os.Create(cfg.CPUProfilePath)
		if err != nil {
			return nil, fmt.Errorf("create cpu profile: %w", err)
		}
		if err := pprof.StartCPUProfile(profFile); err != nil {
			_ = profFile.Close()
			return nil, fmt.Errorf("start cpu profile: %w", err)
		}
	}

	startWall := time.Now()

	peakGoroutines := runtime.NumGoroutine()
	var goroutineSum int64
	var goroutineSamples int64
	stopSampler := make(chan struct{})
	samplerDone := make(chan struct{})
	go func() {
		defer close(samplerDone)
		ticker := time.NewTicker(1 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopSampler:
				return
			case <-ticker.C:
				g := runtime.NumGoroutine()
				if g > peakGoroutines {
					peakGoroutines = g
				}
				goroutineSum += int64(g)
				goroutineSamples++
			}
		}
	}()

	chunks, err := SplitFileIntoChunks(filePath, cfg.TargetChunkBytes)
	if err != nil {
		close(stopSampler)
		<-samplerDone
		if profFile != nil {
			pprof.StopCPUProfile()
			_ = profFile.Close()
		}
		return nil, err
	}

	var totalBytes int64
	for _, c := range chunks {
		totalBytes += c.Length
	}

	taskCh := make(chan chunkTask, len(chunks)*4+4)
	resultCh := make(chan mapAttemptResult, len(chunks)*4+4)

	var flightMu sync.Mutex
	inFlight := make(map[int]*inFlightState, len(chunks))

	for _, c := range chunks {
		taskCh <- chunkTask{spec: c, attempt: 1, speculative: false}
	}

	var mapWg sync.WaitGroup
	for w := 0; w < cfg.NumMapWorkers; w++ {
		mapWg.Add(1)
		go func() {
			defer mapWg.Done()
			for task := range taskCh {
				flightMu.Lock()
				st := inFlight[task.spec.ID]
				if st != nil && st.completed {
					flightMu.Unlock()
					continue
				}
				ctx, cancel := context.WithTimeout(context.Background(), cfg.TaskTimeout)
				if st == nil {
					inFlight[task.spec.ID] = &inFlightState{
						startedAt:     time.Now(),
						cancelPrimary: cancel,
					}
				}
				flightMu.Unlock()

				simCrash := cfg.CrashChunks != nil && cfg.CrashChunks[task.spec.ID] && task.attempt == 1 && !task.speculative
				simTimeout := cfg.TimeoutChunks != nil && cfg.TimeoutChunks[task.spec.ID] && task.attempt == 1 && !task.speculative
				var stragglerDelay time.Duration
				if cfg.StragglerChunks != nil && task.attempt == 1 && !task.speculative {
					stragglerDelay = cfg.StragglerChunks[task.spec.ID]
				}

				res := executeMapChunk(
					ctx,
					task,
					simCrash,
					simTimeout,
					stragglerDelay,
					cfg.TaskTimeout,
					cfg.UseHistogramCombiner,
				)
				cancel()
				resultCh <- res
			}
		}()
	}

	var speculativeBackupsLaunched int
	stopSpeculator := make(chan struct{})
	speculatorDone := make(chan struct{})
	if cfg.EnableSpeculativeExec {
		go func() {
			defer close(speculatorDone)
			ticker := time.NewTicker(1 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-stopSpeculator:
					return
				case <-ticker.C:
					now := time.Now()
					flightMu.Lock()
					for cid, st := range inFlight {
						if !st.completed && !st.backupLaunched && now.Sub(st.startedAt) >= cfg.SpeculativeThreshold {
							st.backupLaunched = true
							speculativeBackupsLaunched++
							taskCh <- chunkTask{
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
		close(speculatorDone)
	}

	committedChunks := make(map[int]map[string]*IntermediateKV, len(chunks))
	var committedRecords int64
	var failureEvents []FailureEvent
	var speculativeWins int

	for len(committedChunks) < len(chunks) {
		res := <-resultCh
		if _, alreadyDone := committedChunks[res.chunkID]; alreadyDone {
			continue
		}
		if res.err != nil {
			flightMu.Lock()
			st := inFlight[res.chunkID]
			alreadyBackedUp := st != nil && st.backupLaunched
			flightMu.Unlock()
			if alreadyBackedUp {
				continue
			}

			detectedNs := time.Now().UnixNano()
			nextAttempt := res.attempt + 1
			taskCh <- chunkTask{
				spec:        chunks[res.chunkID],
				attempt:     nextAttempt,
				speculative: false,
			}
			reassignedNs := time.Now().UnixNano()
			recoveryMicros := float64(reassignedNs-detectedNs) / 1000.0
			if recoveryMicros < 0.1 {
				recoveryMicros = 0.1
			}
			failureEvents = append(failureEvents, FailureEvent{
				ChunkID:               res.chunkID,
				Attempt:               res.attempt,
				FailureType:           res.failure,
				DetectedAtNs:          detectedNs,
				ReassignedAtNs:        reassignedNs,
				RecoveryLatencyMicros: recoveryMicros,
			})
			continue
		}

		flightMu.Lock()
		st := inFlight[res.chunkID]
		if st != nil {
			st.completed = true
			if res.speculative && st.cancelPrimary != nil {
				st.cancelPrimary()
			}
		}
		flightMu.Unlock()

		if res.speculative {
			speculativeWins++
		}
		committedChunks[res.chunkID] = res.kvs
		committedRecords += res.records
	}

	close(stopSpeculator)
	<-speculatorDone
	close(taskCh)
	mapWg.Wait()

	// Shuffle Phase: Partition intermediate KVs by hash(endpoint) % NumReduceWorkers
	shuffledBuckets := make([]map[string][]*IntermediateKV, cfg.NumReduceWorkers)
	for r := 0; r < cfg.NumReduceWorkers; r++ {
		shuffledBuckets[r] = make(map[string][]*IntermediateKV)
	}
	for _, chunkMap := range committedChunks {
		for ep, kv := range chunkMap {
			part := partitionForKey(ep, cfg.NumReduceWorkers)
			shuffledBuckets[part][ep] = append(shuffledBuckets[part][ep], kv)
		}
	}

	// Parallel Reduce Phase
	var reduceMu sync.Mutex
	reducedSummaries := make([]EndpointAggregate, 0, 16)
	var reduceWg sync.WaitGroup

	for r := 0; r < cfg.NumReduceWorkers; r++ {
		reduceWg.Add(1)
		go func(bucket map[string][]*IntermediateKV) {
			defer reduceWg.Done()
			localOut := make([]EndpointAggregate, 0, len(bucket))
			for ep, partials := range bucket {
				merged := &IntermediateKV{Endpoint: ep}
				if cfg.UseHistogramCombiner {
					merged.LatencyBuckets = make([]uint32, MaxHistogramMicros)
					for _, p := range partials {
						merged.Count += p.Count
						merged.ErrorCount += p.ErrorCount
						merged.BytesSum += p.BytesSum
						merged.LatencySumMicros += p.LatencySumMicros
						merged.SignatureXor ^= p.SignatureXor
						for bIdx, cnt := range p.LatencyBuckets {
							merged.LatencyBuckets[bIdx] += cnt
						}
					}
				} else {
					var totalLatCount int
					for _, p := range partials {
						totalLatCount += len(p.LatenciesMicros)
					}
					merged.LatenciesMicros = make([]int64, 0, totalLatCount)
					for _, p := range partials {
						merged.Count += p.Count
						merged.ErrorCount += p.ErrorCount
						merged.BytesSum += p.BytesSum
						merged.LatencySumMicros += p.LatencySumMicros
						merged.SignatureXor ^= p.SignatureXor
						merged.LatenciesMicros = append(merged.LatenciesMicros, p.LatenciesMicros...)
					}
				}
				localOut = append(localOut, finalizeEndpointAggregate(ep, merged))
			}
			reduceMu.Lock()
			reducedSummaries = append(reducedSummaries, localOut...)
			reduceMu.Unlock()
		}(shuffledBuckets[r])
	}
	reduceWg.Wait()

	sort.Slice(reducedSummaries, func(i, j int) bool {
		return reducedSummaries[i].Endpoint < reducedSummaries[j].Endpoint
	})

	wallElapsed := time.Since(startWall)
	close(stopSampler)
	<-samplerDone

	var cpuProfBytes int64
	if profFile != nil {
		pprof.StopCPUProfile()
		_ = profFile.Close()
		if st, err := os.Stat(cfg.CPUProfilePath); err == nil {
			cpuProfBytes = st.Size()
		}
	}

	wallSec := math.Max(1e-9, wallElapsed.Seconds())
	wallMs := float64(wallElapsed.Nanoseconds()) / 1e6
	baseMs := float64(baselineDuration.Nanoseconds()) / 1e6
	speedup := 1.0
	if wallElapsed > 0 && baselineDuration > 0 {
		speedup = float64(baselineDuration) / float64(wallElapsed)
	}

	var avgRecoveryMs, maxRecoveryMs float64
	if len(failureEvents) > 0 {
		var sumMicros float64
		for _, fe := range failureEvents {
			sumMicros += fe.RecoveryLatencyMicros
			ms := fe.RecoveryLatencyMicros / 1000.0
			if ms > maxRecoveryMs {
				maxRecoveryMs = ms
			}
		}
		avgRecoveryMs = (sumMicros / float64(len(failureEvents))) / 1000.0
	}

	var dataLossPct float64
	if expectedRecords > 0 {
		diff := expectedRecords - committedRecords
		if diff < 0 {
			diff = -diff
		}
		dataLossPct = float64(diff) / float64(expectedRecords) * 100.0
	}

	avgGoroutines := float64(peakGoroutines)
	if goroutineSamples > 0 {
		avgGoroutines = float64(goroutineSum) / float64(goroutineSamples)
	}

	return &JobMetrics{
		ScenarioName:               cfg.ScenarioName,
		TotalRecords:               committedRecords,
		ExpectedRecords:            expectedRecords,
		TotalBytes:                 totalBytes,
		NumMapWorkers:              cfg.NumMapWorkers,
		NumReduceWorkers:           cfg.NumReduceWorkers,
		NumChunks:                  len(chunks),
		WallTimeMs:                 math.Round(wallMs*1000) / 1000,
		BaselineWallTimeMs:         math.Round(baseMs*1000) / 1000,
		ThroughputRecordsSec:       math.Round(float64(committedRecords)/wallSec*100) / 100,
		ThroughputMBPerSec:         math.Round((float64(totalBytes)/(1024*1024))/wallSec*100) / 100,
		SpeedupFactor:              math.Round(speedup*100) / 100,
		SimulatedFailures:          len(failureEvents),
		AvgFailureRecoveryMs:       math.Round(avgRecoveryMs*10000) / 10000,
		MaxFailureRecoveryMs:       math.Round(maxRecoveryMs*10000) / 10000,
		SpeculativeBackupsLaunched: speculativeBackupsLaunched,
		SpeculativeWins:            speculativeWins,
		HistogramCombinerUsed:      cfg.UseHistogramCombiner,
		DataLossRatePct:            math.Round(dataLossPct*10000) / 10000,
		PeakGoroutines:             peakGoroutines,
		AvgActiveGoroutines:        math.Round(avgGoroutines*100) / 100,
		CPUProfilePath:             cfg.CPUProfilePath,
		CPUProfileBytes:            cpuProfBytes,
		FailureEvents:              failureEvents,
		EndpointSummaries:          reducedSummaries,
	}, nil
}
