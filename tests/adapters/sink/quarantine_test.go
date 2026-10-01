package sink_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/RenJieWeia/prism-core/pkg/adapters/sink"
	"github.com/RenJieWeia/prism-core/pkg/core/domain"
	"github.com/RenJieWeia/prism-core/pkg/core/ports"
)

// quarantineRecorder 记录保存的隔离数据并支持注入错误/检测 ctx
type quarantineRecorder struct {
	mu     sync.Mutex
	saved  []domain.QuarantineReading
	fail   error
	sawCtx context.Context
}

func (q *quarantineRecorder) Save(ctx context.Context, record domain.QuarantineReading) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.saved = append(q.saved, record)
	if q.fail != nil {
		return q.fail
	}
	q.sawCtx = ctx
	return nil
}
func (q *quarantineRecorder) FindPending(_ context.Context, _ int) ([]domain.QuarantineReading, error) {
	return nil, nil
}

func quarantinedReading(deviceID string, ts time.Time, reason string) domain.QuarantineReading {
	return domain.QuarantineReading{
		Reading: domain.Reading{DeviceInfo: domain.DeviceInfo{ID: deviceID}, Timestamp: ts},
		Reason:  reason,
	}
}

// TestQuarantineSinkOnlySavesRejected QuarantineSink 只处理 Rejected
func TestQuarantineSinkOnlySavesRejected(t *testing.T) {
	base, _ := time.Parse(time.RFC3339, "2026-08-04T10:00:00Z")
	repo := &quarantineRecorder{}
	qs := sink.NewQuarantineSink(repo)
	if qs.ID() == "" {
		t.Error("expected non-empty ID")
	}

	result := domain.ProcessingResult{
		Accepted: []domain.StandardReading{{DeviceID: "D1"}},
		Rejected: []domain.QuarantineReading{
			quarantinedReading("D3", base, "bad1"),
			quarantinedReading("D4", base, "bad2"),
		},
	}
	if err := qs.Deliver(context.Background(), result); err != nil {
		t.Fatalf("deliver failed: %v", err)
	}

	repo.mu.Lock()
	defer repo.mu.Unlock()
	if len(repo.saved) != 2 {
		t.Fatalf("expected 2 saved rejected records, got %d", len(repo.saved))
	}
	if repo.saved[0].Reading.DeviceInfo.ID != "D3" || repo.saved[1].Reading.DeviceInfo.ID != "D4" {
		t.Errorf("unexpected saved records: %+v", repo.saved)
	}
}

// TestQuarantineSinkEmptyRejected 空 Rejected 不应调用仓储
func TestQuarantineSinkEmptyRejected(t *testing.T) {
	repo := &quarantineRecorder{}
	qs := sink.NewQuarantineSink(repo)
	if err := qs.Deliver(context.Background(), domain.ProcessingResult{}); err != nil {
		t.Fatalf("deliver failed: %v", err)
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if len(repo.saved) != 0 {
		t.Errorf("expected no saves for empty rejected, got %d", len(repo.saved))
	}
}

// TestQuarantineSinkUsesCallerContext QuarantineSink 使用调用方传入的 ctx
func TestQuarantineSinkUsesCallerContext(t *testing.T) {
	base, _ := time.Parse(time.RFC3339, "2026-08-04T10:00:00Z")
	repo := &quarantineRecorder{}
	qs := sink.NewQuarantineSink(repo)

	ctx := context.WithValue(context.Background(), "k", "v")
	result := domain.ProcessingResult{Rejected: []domain.QuarantineReading{
		quarantinedReading("D1", base, "bad"),
	}}
	if err := qs.Deliver(ctx, result); err != nil {
		t.Fatalf("deliver failed: %v", err)
	}

	repo.mu.Lock()
	defer repo.mu.Unlock()
	if repo.sawCtx != ctx {
		t.Errorf("expected sink to use the caller ctx, got %v", repo.sawCtx)
	}
}

// TestQuarantineSinkErrorContent 保存失败返回明确错误 (Sink ID / 设备 ID / 时间戳 / 原始错误)
func TestQuarantineSinkErrorContent(t *testing.T) {
	base, _ := time.Parse(time.RFC3339, "2026-08-04T10:00:00Z")
	repo := &quarantineRecorder{fail: errors.New("db down")}
	qs := sink.NewQuarantineSink(repo)

	result := domain.ProcessingResult{Rejected: []domain.QuarantineReading{
		quarantinedReading("D9", base, "bad"),
	}}
	err := qs.Deliver(context.Background(), result)
	if err == nil {
		t.Fatal("expected save error to propagate")
	}
	for _, want := range []string{`sink "quarantine"`, "D9", base.Format(time.RFC3339), "db down"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("expected error to contain %q, got: %v", want, err)
		}
	}
}

var _ ports.QuarantineRepository = (*quarantineRecorder)(nil)
