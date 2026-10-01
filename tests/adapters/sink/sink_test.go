package sink_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/RenJieWeia/prism-core/pkg/adapters/sink"
	"github.com/RenJieWeia/prism-core/pkg/core/domain"
	"github.com/RenJieWeia/prism-core/pkg/core/ports"
)

func sampleResult() domain.ProcessingResult {
	return domain.ProcessingResult{
		Accepted: []domain.StandardReading{
			{DeviceID: "D1", ValueScaled: 1000000},
			{DeviceID: "D2", ValueScaled: 500000},
		},
		Rejected: []domain.QuarantineReading{
			{Reading: domain.Reading{DeviceInfo: domain.DeviceInfo{ID: "D3"}}, Reason: "bad"},
		},
		Evaluations: []domain.RuleEvaluation{
			{RuleID: "r1", Outcome: "PASS"},
		},
	}
}

// TestMemorySinkReceivesResult MemorySink 能收到结果
func TestMemorySinkReceivesResult(t *testing.T) {
	ms := sink.NewMemorySink()
	if ms.ID() == "" {
		t.Error("expected non-empty ID")
	}

	result := sampleResult()
	if err := ms.Deliver(context.Background(), result); err != nil {
		t.Fatalf("deliver failed: %v", err)
	}

	got := ms.Results()
	if ms.Count() != 1 {
		t.Fatalf("expected 1 result, got %d", ms.Count())
	}
	if len(got[0].Accepted) != 2 || len(got[0].Rejected) != 1 || len(got[0].Evaluations) != 1 {
		t.Errorf("unexpected stored result: %+v", got[0])
	}
}

// TestCallbackSinkReceivesResult CallbackSink 能收到结果
func TestCallbackSinkReceivesResult(t *testing.T) {
	var mu sync.Mutex
	var got *domain.ProcessingResult

	cb := sink.NewCallbackSink("cb", func(_ context.Context, result domain.ProcessingResult) error {
		cp := result
		mu.Lock()
		defer mu.Unlock()
		got = &cp
		return nil
	})

	if err := cb.Deliver(context.Background(), sampleResult()); err != nil {
		t.Fatalf("deliver failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if got == nil {
		t.Fatal("expected callback to receive result")
	}
	if len(got.Accepted) != 2 {
		t.Errorf("expected 2 accepted, got %d", len(got.Accepted))
	}
}

// recordingRepo 记录 SaveBatch 保存的标准数据
type recordingRepo struct {
	mu     sync.Mutex
	saved  []domain.StandardReading
	strats []ports.UpsertStrategy
}

func (r *recordingRepo) Save(_ context.Context, _ domain.StandardReading, _ ports.UpsertStrategy) error {
	return nil
}
func (r *recordingRepo) SaveBatch(_ context.Context, rs []domain.StandardReading, strategy ports.UpsertStrategy) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.saved = append(r.saved, rs...)
	r.strats = append(r.strats, strategy)
	return nil
}
func (r *recordingRepo) FindExact(_ context.Context, _ string, _ time.Time) (*domain.StandardReading, error) {
	return nil, nil
}
func (r *recordingRepo) FindRange(_ context.Context, _ string, _, _ time.Time) ([]domain.StandardReading, error) {
	return nil, nil
}

// TestRepositorySinkOnlySavesAccepted RepositorySink 只保存 Accepted 数据
func TestRepositorySinkOnlySavesAccepted(t *testing.T) {
	repo := &recordingRepo{}
	rs := sink.NewRepositorySink(repo, ports.UpsertStrategyHighPriorityWins)

	result := sampleResult()
	if err := rs.Deliver(context.Background(), result); err != nil {
		t.Fatalf("deliver failed: %v", err)
	}

	repo.mu.Lock()
	defer repo.mu.Unlock()
	if len(repo.saved) != 2 {
		t.Fatalf("expected 2 saved accepted readings, got %d", len(repo.saved))
	}
	if len(repo.strats) != 1 || repo.strats[0] != ports.UpsertStrategyHighPriorityWins {
		t.Errorf("expected HighPriorityWins strategy, got %v", repo.strats)
	}
}

// TestRepositorySinkEmptyAccepted 空 Accepted 不应调用仓储
func TestRepositorySinkEmptyAccepted(t *testing.T) {
	repo := &recordingRepo{}
	rs := sink.NewRepositorySink(repo, ports.UpsertStrategyLastWriteWins)

	if err := rs.Deliver(context.Background(), domain.ProcessingResult{}); err != nil {
		t.Fatalf("deliver failed: %v", err)
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if len(repo.saved) != 0 {
		t.Errorf("expected no saves for empty accepted, got %d", len(repo.saved))
	}
}

var _ ports.StandardReadingRepository = (*recordingRepo)(nil)
