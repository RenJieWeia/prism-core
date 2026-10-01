package ingest_test

import (
	"context"
	"strings"
	"testing"

	"github.com/RenJieWeia/prism-core/pkg/adapters/ingest"
	"github.com/RenJieWeia/prism-core/pkg/core/domain"
)

func TestCsvIngestor(t *testing.T) {
	down, received := collectDownstream()
	ing := ingest.NewCsvUniversalIngestor(down)

	input := "device_id,timestamp,value,model,type\n" +
		"m1,2023-01-01T10:00:00Z,100.5,AX-1,ELEC\n" +
		"m2,2023-01-01 11:00:00,50,GAS-2,GAS\n"

	res, err := ing.IngestStream(context.Background(), strings.NewReader(input))
	if err != nil {
		t.Fatalf("IngestStream failed: %v", err)
	}
	if res.Total != 2 || res.Success != 2 || res.Failed != 0 {
		t.Fatalf("unexpected result: %+v", res)
	}
	if len(*received) != 2 {
		t.Fatalf("expected 2 readings, got %d", len(*received))
	}
	if (*received)[0].DeviceInfo.Model != "AX-1" || (*received)[0].Value != 100.5 {
		t.Errorf("bad first reading: %+v", (*received)[0])
	}
	if (*received)[1].DeviceInfo.Type != domain.DeviceTypeGas || (*received)[1].Value != 50 {
		t.Errorf("bad second reading: %+v", (*received)[1])
	}
}

func TestCsvIngestorMissingHeader(t *testing.T) {
	down, _ := collectDownstream()
	ing := ingest.NewCsvUniversalIngestor(down)
	input := "device_id,timestamp\nm1,2023-01-01T10:00:00Z\n"
	if _, err := ing.IngestStream(context.Background(), strings.NewReader(input)); err == nil {
		t.Fatalf("expected error for missing value header")
	}
}

func TestCsvIngestorBadValue(t *testing.T) {
	down, _ := collectDownstream()
	ing := ingest.NewCsvUniversalIngestor(down)
	input := "device_id,timestamp,value\nm1,2023-01-01T10:00:00Z,abc\n"
	res, err := ing.IngestStream(context.Background(), strings.NewReader(input))
	if err != nil {
		t.Fatalf("row errors should not fail the whole stream: %v", err)
	}
	if res.Failed != 1 || res.Success != 0 {
		t.Fatalf("unexpected result: %+v", res)
	}
}

func TestCsvIngestorBatchFormatGuard(t *testing.T) {
	down, _ := collectDownstream()
	ing := ingest.NewCsvUniversalIngestor(down)
	if _, err := ing.IngestBatch(context.Background(), strings.NewReader(""), "json"); err == nil {
		t.Fatalf("expected error for non-csv batch format")
	}
}
