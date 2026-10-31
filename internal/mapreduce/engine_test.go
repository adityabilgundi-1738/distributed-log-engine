package mapreduce

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSplitFileIntoChunksAlignsOnNewlines(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test_access.log")

	totalBytes, err := GenerateSyntheticLogFile(logPath, 10_000, 123)
	if err != nil {
		t.Fatalf("GenerateSyntheticLogFile failed: %v", err)
	}

	chunks, err := SplitFileIntoChunks(logPath, 32*1024)
	if err != nil {
		t.Fatalf("SplitFileIntoChunks failed: %v", err)
	}
	if len(chunks) < 5 {
		t.Fatalf("expected multiple chunks, got %d", len(chunks))
	}

	var sumLen int64
	f, err := os.Open(logPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer f.Close()

	for i, c := range chunks {
		if i > 0 && c.Offset != chunks[i-1].Offset+chunks[i-1].Length {
			t.Fatalf("chunk %d offset gap: got %d", i, c.Offset)
		}
		sumLen += c.Length

		lastByte := make([]byte, 1)
		if _, err := f.ReadAt(lastByte, c.Offset+c.Length-1); err != nil {
			t.Fatalf("ReadAt failed on chunk %d: %v", i, err)
		}
		if lastByte[0] != '\n' {
			t.Fatalf("chunk %d does not end on newline: %q", i, lastByte[0])
		}
	}

	if sumLen != totalBytes {
		t.Fatalf("chunk length sum %d != file size %d", sumLen, totalBytes)
	}
}

func TestMapReduceMatchesSingleThreadedBaselineWithFailures(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "server.log")
	profPath := filepath.Join(tmpDir, "cpu.pprof")

	const numRecords = 50_000
	_, err := GenerateSyntheticLogFile(logPath, numRecords, 42)
	if err != nil {
		t.Fatalf("GenerateSyntheticLogFile failed: %v", err)
	}

	baselineMap, baseRecords, baseDur, err := RunSingleThreadedBaseline(logPath)
	if err != nil {
		t.Fatalf("RunSingleThreadedBaseline failed: %v", err)
	}
	if baseRecords != numRecords {
		t.Fatalf("expected %d baseline records, got %d", numRecords, baseRecords)
	}

	cfg := EngineConfig{
		ScenarioName:     "Unit Test With Crash + Timeout Recovery",
		NumMapWorkers:    8,
		NumReduceWorkers: 4,
		TargetChunkBytes: 128 * 1024,
		TaskTimeout:      25 * time.Millisecond,
		CrashChunks:      map[int]bool{1: true, 4: true, 7: true},
		TimeoutChunks:    map[int]bool{2: true, 5: true},
		EnableCPUProfile: true,
		CPUProfilePath:   profPath,
	}

	metrics, err := RunMapReduceJob(logPath, cfg, baseRecords, baseDur)
	if err != nil {
		t.Fatalf("RunMapReduceJob failed: %v", err)
	}

	if metrics.TotalRecords != baseRecords {
		t.Fatalf("record count mismatch: got %d, want %d", metrics.TotalRecords, baseRecords)
	}
	if metrics.DataLossRatePct != 0.0 {
		t.Fatalf("expected 0%% data loss, got %f%%", metrics.DataLossRatePct)
	}
	if metrics.SimulatedFailures != 5 {
		t.Fatalf("expected 5 simulated failures, got %d", metrics.SimulatedFailures)
	}

	for _, got := range metrics.EndpointSummaries {
		want, ok := baselineMap[got.Endpoint]
		if !ok {
			t.Fatalf("unexpected endpoint %s", got.Endpoint)
		}
		if got.TotalRequests != want.TotalRequests ||
			got.ErrorRequests != want.ErrorRequests ||
			got.TotalBytes != want.TotalBytes ||
			got.LatencySumMicros != want.LatencySumMicros ||
			got.P50LatencyMs != want.P50LatencyMs ||
			got.P95LatencyMs != want.P95LatencyMs ||
			got.P99LatencyMs != want.P99LatencyMs ||
			got.SignatureXor != want.SignatureXor {
			t.Fatalf("aggregate mismatch for %s:\ngot:  %+v\nwant: %+v", got.Endpoint, got, want)
		}
	}
}

func TestSpeculativeExecutionAndHistogramCombiner(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "straggler.log")

	const numRecords = 40_000
	_, err := GenerateSyntheticLogFile(logPath, numRecords, 99)
	if err != nil {
		t.Fatalf("GenerateSyntheticLogFile failed: %v", err)
	}

	baselineMap, baseRecords, baseDur, err := RunSingleThreadedBaseline(logPath)
	if err != nil {
		t.Fatalf("RunSingleThreadedBaseline failed: %v", err)
	}

	stragglers := map[int]time.Duration{
		1: 120 * time.Millisecond,
		3: 120 * time.Millisecond,
	}

	noSpecCfg := EngineConfig{
		ScenarioName:          "Stragglers Without Speculative Execution",
		NumMapWorkers:         8,
		NumReduceWorkers:      4,
		TargetChunkBytes:      256 * 1024,
		TaskTimeout:           500 * time.Millisecond,
		StragglerChunks:       stragglers,
		EnableSpeculativeExec: false,
		UseHistogramCombiner:  true,
	}
	noSpecMetrics, err := RunMapReduceJob(logPath, noSpecCfg, baseRecords, baseDur)
	if err != nil {
		t.Fatalf("noSpec RunMapReduceJob failed: %v", err)
	}

	specCfg := EngineConfig{
		ScenarioName:          "Stragglers With Speculative Backup Tasks",
		NumMapWorkers:         8,
		NumReduceWorkers:      4,
		TargetChunkBytes:      256 * 1024,
		TaskTimeout:           500 * time.Millisecond,
		StragglerChunks:       stragglers,
		EnableSpeculativeExec: true,
		SpeculativeThreshold:  5 * time.Millisecond,
		UseHistogramCombiner:  true,
	}
	specMetrics, err := RunMapReduceJob(logPath, specCfg, baseRecords, baseDur)
	if err != nil {
		t.Fatalf("spec RunMapReduceJob failed: %v", err)
	}

	if specMetrics.SpeculativeWins < 2 {
		t.Fatalf("expected at least 2 speculative backup task wins, got %d", specMetrics.SpeculativeWins)
	}
	if specMetrics.WallTimeMs >= noSpecMetrics.WallTimeMs*0.75 {
		t.Fatalf(
			"expected speculative execution to cut tail latency significantly: spec=%.2fms, noSpec=%.2fms",
			specMetrics.WallTimeMs,
			noSpecMetrics.WallTimeMs,
		)
	}
	if specMetrics.DataLossRatePct != 0.0 {
		t.Fatalf("expected 0%% data loss, got %f%%", specMetrics.DataLossRatePct)
	}

	for _, got := range specMetrics.EndpointSummaries {
		want := baselineMap[got.Endpoint]
		if got.TotalRequests != want.TotalRequests ||
			got.P50LatencyMs != want.P50LatencyMs ||
			got.P95LatencyMs != want.P95LatencyMs ||
			got.P99LatencyMs != want.P99LatencyMs ||
			got.SignatureXor != want.SignatureXor {
			t.Fatalf("histogram/speculative mismatch for %s:\ngot:  %+v\nwant: %+v", got.Endpoint, got, want)
		}
	}
}
