package mapreduce

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"
)

func TestHTTPClusterFaultToleranceAndSpeculativeExecution(t *testing.T) {
	cleanup, err := StartEmbeddedCluster(
		"127.0.0.1:19090",
		[]string{"127.0.0.1:19091", "127.0.0.1:19092", "127.0.0.1:19093", "127.0.0.1:19094"},
	)
	if err != nil {
		t.Fatalf("StartEmbeddedCluster failed: %v", err)
	}
	defer cleanup()

	reqBody, _ := json.Marshal(ClusterJobRequest{
		Records:         30_000,
		Speculative:     true,
		InjectCrash:     true,
		InjectStraggler: true,
	})
	resp, err := http.Post("http://127.0.0.1:19090/api/job", "application/json", bytes.NewReader(reqBody))
	if err != nil {
		t.Fatalf("POST /api/job failed: %v", err)
	}
	defer resp.Body.Close()

	var jobResp ClusterJobResponse
	if err := json.NewDecoder(resp.Body).Decode(&jobResp); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if jobResp.Records != 30_000 {
		t.Fatalf("expected 30000 records, got %d", jobResp.Records)
	}
	if jobResp.DataLossPct != 0.0 {
		t.Fatalf("expected 0%% data loss, got %f%%", jobResp.DataLossPct)
	}
	if jobResp.FailuresRecovered < 2 {
		t.Fatalf("expected at least 2 crash recoveries, got %d", jobResp.FailuresRecovered)
	}
}
