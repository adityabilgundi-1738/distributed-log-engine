package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"distributed-log-engine/internal/mapreduce"
)

func printReport(results []*mapreduce.JobMetrics) {
	sep := strings.Repeat("=", 116)
	sub := strings.Repeat("-", 116)
	fmt.Println(sep)
	fmt.Println("DISTRIBUTED LOG AGGREGATION ENGINE (GO MAPREDUCE) — BENCHMARK, STRAGGLER HEDGING & FAULT REPORT")
	fmt.Println(sep)

	for _, m := range results {
		fmt.Printf("\nScenario: %s\n", m.ScenarioName)
		fmt.Println(sub)
		fmt.Printf(
			"  Dataset & Chunks        : %d records (%.2f MB) split into %d newline-aligned chunks (Histogram Combiner: %v)\n",
			m.TotalRecords,
			float64(m.TotalBytes)/(1024*1024),
			m.NumChunks,
			m.HistogramCombinerUsed,
		)
		fmt.Printf(
			"  Worker Pool Config      : %d Map goroutines | %d Reduce goroutines | Peak Goroutines = %d (Avg = %.1f)\n",
			m.NumMapWorkers,
			m.NumReduceWorkers,
			m.PeakGoroutines,
			m.AvgActiveGoroutines,
		)
		fmt.Printf(
			"  Throughput              : %.2f records/sec (%.2f MB/sec) | Wall Time = %.2f ms (1-thread baseline = %.2f ms)\n",
			m.ThroughputRecordsSec,
			m.ThroughputMBPerSec,
			m.WallTimeMs,
			m.BaselineWallTimeMs,
		)
		fmt.Printf("  Multi-Core Speedup      : %.2fx vs. single-threaded baseline\n", m.SpeedupFactor)
		fmt.Printf(
			"  Fault & Straggler Guard : %d failures recovered (avg = %.4f ms) | %d speculative backups (%d won race)\n",
			m.SimulatedFailures,
			m.AvgFailureRecoveryMs,
			m.SpeculativeBackupsLaunched,
			m.SpeculativeWins,
		)
		fmt.Printf("  Data Loss Rate          : %.4f%% (Verified exact match against single-threaded checksums)\n", m.DataLossRatePct)
		if m.CPUProfilePath != "" {
			fmt.Printf("  pprof CPU Profile       : %s (%d bytes captured)\n", m.CPUProfilePath, m.CPUProfileBytes)
		}
	}

	if len(results) > 0 {
		last := results[len(results)-1]
		fmt.Println("\n" + sep)
		fmt.Println("ENDPOINT AGGREGATION SUMMARY (SAMPLE FROM FINAL JOB)")
		fmt.Println(sub)
		fmt.Printf(
			"%-28s | %10s | %9s | %8s | %9s | %9s | %9s | %9s\n",
			"Endpoint", "Requests", "Errors", "ErrRate%", "Avg(ms)", "p50(ms)", "p95(ms)", "p99(ms)",
		)
		fmt.Println(sub)
		for _, ep := range last.EndpointSummaries {
			fmt.Printf(
				"%-28s | %10d | %9d | %7.2f%% | %9.3f | %9.3f | %9.3f | %9.3f\n",
				ep.Endpoint,
				ep.TotalRequests,
				ep.ErrorRequests,
				ep.ErrorRatePct,
				ep.AvgLatencyMs,
				ep.P50LatencyMs,
				ep.P95LatencyMs,
				ep.P99LatencyMs,
			)
		}
		fmt.Println(sep)
	}
}

func main() {
	recordsFlag := flag.Int("records", 600_000, "Number of synthetic log records to generate and process")
	workersFlag := flag.Int("workers", runtime.NumCPU(), "Number of Map/Reduce workers for parallel execution")
	pprofPathFlag := flag.String("cpuprofile", "cpu.pprof", "Output path for pprof CPU profile")
	jsonOutFlag := flag.String("json-out", "", "Optional output path for JSON benchmark metrics")
	flag.Parse()

	tmpDir, err := os.MkdirTemp("", "logengine-bench-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create temp dir: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(tmpDir)

	logPath := filepath.Join(tmpDir, "access.log")
	_, err = mapreduce.GenerateSyntheticLogFile(logPath, *recordsFlag, 2026)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to generate synthetic log file: %v\n", err)
		os.Exit(1)
	}

	_, expectedRecords, baselineDuration, err := mapreduce.RunSingleThreadedBaseline(logPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "baseline run failed: %v\n", err)
		os.Exit(1)
	}

	maxWorkers := *workersFlag
	if maxWorkers < 2 {
		maxWorkers = 8
	}

	stragglers := map[int]time.Duration{
		3:  75 * time.Millisecond,
		15: 75 * time.Millisecond,
		28: 75 * time.Millisecond,
	}

	scenarios := []mapreduce.EngineConfig{
		{
			ScenarioName:          "4-Worker Parallel MapReduce (Standard Slice Sort)",
			NumMapWorkers:         4,
			NumReduceWorkers:      4,
			TargetChunkBytes:      1024 * 1024,
			TaskTimeout:           500 * time.Millisecond,
			UseHistogramCombiner:  false,
		},
		{
			ScenarioName:          fmt.Sprintf("%d-Worker Parallel MapReduce (O(1) Histogram Combiner + pprof)", maxWorkers),
			NumMapWorkers:         maxWorkers,
			NumReduceWorkers:      maxWorkers,
			TargetChunkBytes:      1024 * 1024,
			TaskTimeout:           500 * time.Millisecond,
			UseHistogramCombiner:  true,
			EnableCPUProfile:      *pprofPathFlag != "",
			CPUProfilePath:        *pprofPathFlag,
		},
		{
			ScenarioName:          fmt.Sprintf("%d-Worker Fault-Tolerant Run (6 Crashes + 2 Timeouts Injected)", maxWorkers),
			NumMapWorkers:         maxWorkers,
			NumReduceWorkers:      maxWorkers,
			TargetChunkBytes:      1024 * 1024,
			TaskTimeout:           15 * time.Millisecond,
			CrashChunks:           map[int]bool{2: true, 5: true, 9: true, 14: true, 19: true, 25: true},
			TimeoutChunks:         map[int]bool{7: true, 16: true},
			UseHistogramCombiner:  true,
		},
		{
			ScenarioName:          "Straggler Nodes WITHOUT Speculative Execution (3 Slow Workers @ 75ms)",
			NumMapWorkers:         maxWorkers,
			NumReduceWorkers:      maxWorkers,
			TargetChunkBytes:      1024 * 1024,
			TaskTimeout:           500 * time.Millisecond,
			StragglerChunks:       stragglers,
			EnableSpeculativeExec: false,
			UseHistogramCombiner:  true,
		},
		{
			ScenarioName:          "Straggler Nodes WITH Speculative Backup Tasks (MapReduce Section 3.6 Hedging)",
			NumMapWorkers:         maxWorkers,
			NumReduceWorkers:      maxWorkers,
			TargetChunkBytes:      1024 * 1024,
			TaskTimeout:           500 * time.Millisecond,
			StragglerChunks:       stragglers,
			EnableSpeculativeExec: true,
			SpeculativeThreshold:  6 * time.Millisecond,
			UseHistogramCombiner:  true,
		},
	}

	var results []*mapreduce.JobMetrics
	for _, cfg := range scenarios {
		m, err := mapreduce.RunMapReduceJob(logPath, cfg, expectedRecords, baselineDuration)
		if err != nil {
			fmt.Fprintf(os.Stderr, "scenario %q failed: %v\n", cfg.ScenarioName, err)
			os.Exit(1)
		}
		results = append(results, m)
	}

	printReport(results)

	if *jsonOutFlag != "" {
		data, err := json.MarshalIndent(results, "", "  ")
		if err == nil {
			_ = os.WriteFile(*jsonOutFlag, data, 0644)
		}
	}
}
