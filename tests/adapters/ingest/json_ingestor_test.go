package ingest_test

import (
	"context"
	"strings"
	"testing"

	"github.com/RenJieWeia/prism-core/pkg/adapters/ingest"
	"github.com/RenJieWeia/prism-core/pkg/core/domain"
)

func collectDownstream() (func(context.Context, []domain.Reading) error, *[]domain.Reading) {
	var received []domain.Reading
	return func(_ context.Context, readings []domain.Reading) error {
		received = append(received, readings...)
		return nil
	}, &received
}

func TestJsonIngestorArray(t *testing.T) {
	down, received := collectDownstream()
	ing := ingest.NewJsonUniversalIngestor(down)

	input := `[
		{"device_id":"d1","timestamp":"2023-01-01T10:00:00Z","value":100},
		{"device_id":"d2","timestamp":"2023-01-01 11:00:00","value":50.5}
	]`
	res, err := ing.IngestStream(context.Background(), strings.NewReader(input))
	if err != nil {
		t.Fatalf("IngestStream failed: %v", err)
	}
	if res.Total != 2 || res.Success != 2 || res.Failed != 0 {
		t.Fatalf("unexpected result: %+v", res)
	}
	if len(*received) != 2 {
		t.Fatalf("expected 2 readings downstream, got %d", len(*received))
	}
	// 两种时间格式都解析成功
	if (*received)[0].DeviceInfo.ID != "d1" || (*received)[0].Value != 100 {
		t.Errorf("bad first reading: %+v", (*received)[0])
	}
	if (*received)[1].DeviceInfo.ID != "d2" || (*received)[1].Value != 50.5 {
		t.Errorf("bad second reading: %+v", (*received)[1])
	}
}

func TestJsonIngestorSingleObject(t *testing.T) {
	down, received := collectDownstream()
	ing := ingest.NewJsonUniversalIngestor(down)

	input := `{"device_id":"d1","model":"AX-1","type":"ELEC","timestamp":"2023-01-01T10:00:00Z","value":42.5}`
	res, err := ing.IngestStream(context.Background(), strings.NewReader(input))
	if err != nil {
		t.Fatalf("IngestStream failed: %v", err)
	}
	if res.Total != 1 || res.Success != 1 {
		t.Fatalf("unexpected result: %+v", res)
	}
	r := (*received)[0]
	if r.DeviceInfo.ID != "d1" || r.DeviceInfo.Model != "AX-1" || r.DeviceInfo.Type != domain.DeviceTypeElec || r.Value != 42.5 {
		t.Errorf("bad reading: %+v", r)
	}
}

func TestJsonIngestorBadRows(t *testing.T) {
	down, received := collectDownstream()
	ing := ingest.NewJsonUniversalIngestor(down)

	// 第二条时间格式非法 -> 记录失败并继续
	input := `[
		{"device_id":"d1","timestamp":"2023-01-01T10:00:00Z","value":1},
		{"device_id":"d2","timestamp":"not-a-time","value":2}
	]`
	res, err := ing.IngestStream(context.Background(), strings.NewReader(input))
	if err != nil {
		t.Fatalf("IngestStream should not fail on row errors: %v", err)
	}
	if res.Total != 2 || res.Success != 1 || res.Failed != 1 {
		t.Fatalf("unexpected result: %+v", res)
	}
	if len(*received) != 1 {
		t.Fatalf("expected 1 valid reading downstream, got %d", len(*received))
	}
}

func TestJsonIngestorEmpty(t *testing.T) {
	down, _ := collectDownstream()
	ing := ingest.NewJsonUniversalIngestor(down)
	res, err := ing.IngestStream(context.Background(), strings.NewReader(""))
	if err != nil {
		t.Fatalf("empty stream should not error: %v", err)
	}
	if res.Total != 0 {
		t.Fatalf("expected 0 total, got %d", res.Total)
	}
}

func TestJsonIngestorBadFormat(t *testing.T) {
	down, _ := collectDownstream()
	ing := ingest.NewJsonUniversalIngestor(down)
	if _, err := ing.IngestStream(context.Background(), strings.NewReader("hello")); err == nil {
		t.Fatalf("expected error for non-JSON input")
	}
}

func TestJsonIngestorBatchFormatGuard(t *testing.T) {
	down, _ := collectDownstream()
	ing := ingest.NewJsonUniversalIngestor(down)
	if _, err := ing.IngestBatch(context.Background(), strings.NewReader("[]"), "csv"); err == nil {
		t.Fatalf("expected error for non-json batch format")
	}
}
