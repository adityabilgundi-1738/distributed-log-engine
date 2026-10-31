package mapreduce

import "time"

const MaxHistogramMicros = 75000

type ChunkSpec struct {
	ID       int
	FilePath string
	Offset   int64
	Length   int64
}

type IntermediateKV struct {
	Endpoint         string
	Count            int64
	ErrorCount       int64
	BytesSum         int64
	LatencySumMicros int64
	LatenciesMicros  []int64
	LatencyBuckets   []uint32
	SignatureXor     uint64
}

type EndpointAggregate struct {
	Endpoint         string  `json:"endpoint"`
	TotalRequests    int64   `json:"total_requests"`
	ErrorRequests    int64   `json:"error_requests"`
	ErrorRatePct     float64 `json:"error_rate_pct"`
	TotalBytes       int64   `json:"total_bytes"`
	LatencySumMicros int64   `json:"latency_sum_micros"`
	AvgLatencyMs     float64 `json:"avg_latency_ms"`
	P50LatencyMs     float64 `json:"p50_latency_ms"`
	P95LatencyMs     float64 `json:"p95_latency_ms"`
	P99LatencyMs     float64 `json:"p99_latency_ms"`
	SignatureXor     uint64  `json:"signature_xor"`
}

type FailureType string

const (
	FailureCrash   FailureType = "CRASH"
	FailureTimeout FailureType = "TIMEOUT"
)

type FailureEvent struct {
	ChunkID               int         `json:"chunk_id"`
	Attempt               int         `json:"attempt"`
	FailureType           FailureType `json:"failure_type"`
	DetectedAtNs          int64       `json:"detected_at_ns"`
	ReassignedAtNs        int64       `json:"reassigned_at_ns"`
	RecoveryLatencyMicros float64     `json:"recovery_latency_micros"`
}

type JobMetrics struct {
	ScenarioName               string              `json:"scenario_name"`
	TotalRecords               int64               `json:"total_records"`
	ExpectedRecords            int64               `json:"expected_records"`
	TotalBytes                 int64               `json:"total_bytes"`
	NumMapWorkers              int                 `json:"num_map_workers"`
	NumReduceWorkers           int                 `json:"num_reduce_workers"`
	NumChunks                  int                 `json:"num_chunks"`
	WallTimeMs                 float64             `json:"wall_time_ms"`
	BaselineWallTimeMs         float64             `json:"baseline_wall_time_ms"`
	ThroughputRecordsSec       float64             `json:"throughput_records_per_sec"`
	ThroughputMBPerSec         float64             `json:"throughput_mb_per_sec"`
	SpeedupFactor              float64             `json:"speedup_factor"`
	SimulatedFailures          int                 `json:"simulated_failures"`
	AvgFailureRecoveryMs       float64             `json:"avg_failure_recovery_ms"`
	MaxFailureRecoveryMs       float64             `json:"max_failure_recovery_ms"`
	SpeculativeBackupsLaunched int                 `json:"speculative_backups_launched"`
	SpeculativeWins            int                 `json:"speculative_wins"`
	HistogramCombinerUsed      bool                `json:"histogram_combiner_used"`
	DataLossRatePct            float64             `json:"data_loss_rate_pct"`
	PeakGoroutines             int                 `json:"peak_goroutines"`
	AvgActiveGoroutines        float64             `json:"avg_active_goroutines"`
	CPUProfilePath             string              `json:"cpu_profile_path,omitempty"`
	CPUProfileBytes            int64               `json:"cpu_profile_bytes,omitempty"`
	FailureEvents              []FailureEvent      `json:"failure_events,omitempty"`
	EndpointSummaries          []EndpointAggregate `json:"endpoint_summaries"`
}

type EngineConfig struct {
	ScenarioName          string
	NumMapWorkers         int
	NumReduceWorkers      int
	TargetChunkBytes      int64
	TaskTimeout           time.Duration
	CrashChunks           map[int]bool
	TimeoutChunks         map[int]bool
	StragglerChunks       map[int]time.Duration
	EnableSpeculativeExec bool
	SpeculativeThreshold  time.Duration
	UseHistogramCombiner  bool
	EnableCPUProfile      bool
	CPUProfilePath        string
}
