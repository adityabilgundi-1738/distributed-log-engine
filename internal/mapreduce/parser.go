package mapreduce

import (
	"bufio"
	"bytes"
	"math"
	"os"
	"sort"
	"time"
)

func computeLineSignature(line []byte) uint64 {
	var h uint64 = 14695981039346656037
	for i := 0; i < len(line); i++ {
		h ^= uint64(line[i])
		h *= 1099511628211
	}
	for round := 0; round < 16; round++ {
		h ^= h >> 33
		h *= 0xff51afd7ed558ccd
		h ^= h >> 33
		h *= 0xc4ceb9fe1a85ec53
	}
	return h
}

func parseFastInt(b []byte) (int64, bool) {
	if len(b) == 0 {
		return 0, false
	}
	var val int64
	for i := 0; i < len(b); i++ {
		c := b[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		val = val*10 + int64(c-'0')
	}
	return val, true
}

func parseLogLine(line []byte) (endpoint string, status int64, latencyMicros int64, bytesSent int64, sig uint64, ok bool) {
	line = bytes.TrimRight(line, "\r\n")
	if len(line) == 0 {
		return "", 0, 0, 0, 0, false
	}
	sig = computeLineSignature(line)

	sp1 := bytes.IndexByte(line, ' ')
	if sp1 < 0 {
		return "", 0, 0, 0, 0, false
	}
	rest := line[sp1+1:]

	sp2 := bytes.IndexByte(rest, ' ')
	if sp2 < 0 {
		return "", 0, 0, 0, 0, false
	}
	rest = rest[sp2+1:]

	sp3 := bytes.IndexByte(rest, ' ')
	if sp3 < 0 {
		return "", 0, 0, 0, 0, false
	}
	endpoint = string(rest[:sp3])
	rest = rest[sp3+1:]

	sp4 := bytes.IndexByte(rest, ' ')
	if sp4 < 0 {
		return "", 0, 0, 0, 0, false
	}
	status, ok = parseFastInt(rest[:sp4])
	if !ok {
		return "", 0, 0, 0, 0, false
	}
	rest = rest[sp4+1:]

	sp5 := bytes.IndexByte(rest, ' ')
	if sp5 < 0 {
		return "", 0, 0, 0, 0, false
	}
	latencyMicros, ok = parseFastInt(rest[:sp5])
	if !ok {
		return "", 0, 0, 0, 0, false
	}
	rest = rest[sp5+1:]

	sp6 := bytes.IndexByte(rest, ' ')
	var bytesToken []byte
	if sp6 < 0 {
		bytesToken = rest
	} else {
		bytesToken = rest[:sp6]
	}
	bytesSent, ok = parseFastInt(bytesToken)
	if !ok {
		return "", 0, 0, 0, 0, false
	}
	return endpoint, status, latencyMicros, bytesSent, sig, true
}

func percentileFromSorted(sorted []int64, q float64) float64 {
	if len(sorted) == 0 {
		return 0.0
	}
	idx := int(math.Round(q * float64(len(sorted)-1)))
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return float64(sorted[idx]) / 1000.0
}

func percentileFromHistogram(buckets []uint32, totalCount int64, q float64) float64 {
	if totalCount <= 0 || len(buckets) == 0 {
		return 0.0
	}
	targetIdx := int64(math.Round(q * float64(totalCount-1)))
	var cumulative int64
	for microVal, count := range buckets {
		if count == 0 {
			continue
		}
		cumulative += int64(count)
		if cumulative > targetIdx {
			return float64(microVal) / 1000.0
		}
	}
	return float64(len(buckets)-1) / 1000.0
}

func finalizeEndpointAggregate(ep string, kv *IntermediateKV) EndpointAggregate {
	var errRate, avgMs, p50, p95, p99 float64
	if kv.Count > 0 {
		errRate = float64(kv.ErrorCount) / float64(kv.Count) * 100.0
		avgMs = (float64(kv.LatencySumMicros) / float64(kv.Count)) / 1000.0
	}
	if len(kv.LatencyBuckets) > 0 {
		p50 = percentileFromHistogram(kv.LatencyBuckets, kv.Count, 0.50)
		p95 = percentileFromHistogram(kv.LatencyBuckets, kv.Count, 0.95)
		p99 = percentileFromHistogram(kv.LatencyBuckets, kv.Count, 0.99)
	} else {
		sort.Slice(kv.LatenciesMicros, func(i, j int) bool {
			return kv.LatenciesMicros[i] < kv.LatenciesMicros[j]
		})
		p50 = percentileFromSorted(kv.LatenciesMicros, 0.50)
		p95 = percentileFromSorted(kv.LatenciesMicros, 0.95)
		p99 = percentileFromSorted(kv.LatenciesMicros, 0.99)
	}

	return EndpointAggregate{
		Endpoint:         ep,
		TotalRequests:    kv.Count,
		ErrorRequests:    kv.ErrorCount,
		ErrorRatePct:     math.Round(errRate*100) / 100,
		TotalBytes:       kv.BytesSum,
		LatencySumMicros: kv.LatencySumMicros,
		AvgLatencyMs:     math.Round(avgMs*1000) / 1000,
		P50LatencyMs:     math.Round(p50*1000) / 1000,
		P95LatencyMs:     math.Round(p95*1000) / 1000,
		P99LatencyMs:     math.Round(p99*1000) / 1000,
		SignatureXor:     kv.SignatureXor,
	}
}

func RunSingleThreadedBaseline(path string) (map[string]EndpointAggregate, int64, time.Duration, error) {
	start := time.Now()
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, 0, err
	}
	defer f.Close()

	acc := make(map[string]*IntermediateKV, 16)
	scanner := bufio.NewScanner(f)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	var totalRecords int64
	for scanner.Scan() {
		ep, status, lat, bSent, sig, ok := parseLogLine(scanner.Bytes())
		if !ok {
			continue
		}
		totalRecords++
		item, exists := acc[ep]
		if !exists {
			item = &IntermediateKV{
				Endpoint:        ep,
				LatenciesMicros: make([]int64, 0, 4096),
			}
			acc[ep] = item
		}
		item.Count++
		if status >= 400 {
			item.ErrorCount++
		}
		item.BytesSum += bSent
		item.LatencySumMicros += lat
		item.LatenciesMicros = append(item.LatenciesMicros, lat)
		item.SignatureXor ^= sig
	}
	if err := scanner.Err(); err != nil {
		return nil, 0, 0, err
	}

	out := make(map[string]EndpointAggregate, len(acc))
	for ep, kv := range acc {
		out[ep] = finalizeEndpointAggregate(ep, kv)
	}
	return out, totalRecords, time.Since(start), nil
}
