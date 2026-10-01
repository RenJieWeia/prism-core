package pipeline_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/RenJieWeia/prism-core/pkg/adapters/sink"
	"github.com/RenJieWeia/prism-core/pkg/application/pipeline"
	"github.com/RenJieWeia/prism-core/pkg/core/domain"
	"github.com/RenJieWeia/prism-core/pkg/core/ports"
	"github.com/RenJieWeia/prism-core/pkg/core/services"
	"github.com/RenJieWeia/prism-core/pkg/core/services/rules"
)

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

// recordingQuarantine 记录 QuarantineRepository.Save 保存的隔离数据
type recordingQuarantine struct {
	mu    sync.Mutex
	saved []domain.QuarantineReading
}

func (q *recordingQuarantine) Save(_ context.Context, record domain.QuarantineReading) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.saved = append(q.saved, record)
	return nil
}
func (q *recordingQuarantine) FindPending(_ context.Context, _ int) ([]domain.QuarantineReading, error) {
	return nil, nil
}

// TestPipelinePersistsAccepted Pipeline 保存标准数据 (与 Accepted 一致, 恰好一次)
func TestPipelinePersistsAccepted(t *testing.T) {
	base, _ := time.Parse(time.RFC3339, "2026-08-04T10:00:00Z")
	repo := &recordingRepo{}
	processor := services.NewEnergyDataProcessor()
	pl := pipeline.NewProcessingPipeline(
		processor,
		sink.NewRepositorySink(repo, ports.UpsertStrategyHighPriorityWins),
		nil,
	)

	raw := []domain.Reading{
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base, Value: 100},
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base.Add(15 * time.Minute), Value: 105},
	}
	result, err := pl.Execute(context.Background(), raw)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Core 返回 Accepted
	if len(result.Accepted) != 2 {
		t.Fatalf("expected 2 accepted, got %d", len(result.Accepted))
	}
	// RepositorySink 保存一次
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if len(repo.strats) != 1 {
		t.Fatalf("expected 1 SaveBatch, got %d", len(repo.strats))
	}
	if len(repo.saved) != 2 {
		t.Fatalf("expected 2 saved, got %d", len(repo.saved))
	}
	// 保存内容与 Accepted 一致
	for i := range result.Accepted {
		if repo.saved[i].DeviceID != result.Accepted[i].DeviceID ||
			repo.saved[i].Timestamp.Unix() != result.Accepted[i].Timestamp.Unix() ||
			repo.saved[i].ValueScaled != result.Accepted[i].ValueScaled {
			t.Errorf("saved[%d]=%+v != accepted[%d]=%+v", i, repo.saved[i], i, result.Accepted[i])
		}
	}
	if repo.strats[0] != ports.UpsertStrategyHighPriorityWins {
		t.Errorf("expected HighPriorityWins strategy, got %v", repo.strats[0])
	}
}

// TestPipelinePersistsRejected Pipeline 保存隔离数据
func TestPipelinePersistsRejected(t *testing.T) {
	base, _ := time.Parse(time.RFC3339, "2026-08-04T10:00:00Z")
	qRepo := &recordingQuarantine{}
	processor := services.NewEnergyDataProcessor(
		services.WithCleaningRules(&rules.MonotonicRule{Action: domain.ActionReject}),
	)
	pl := pipeline.NewProcessingPipeline(
		processor,
		nil,
		sink.NewQuarantineSink(qRepo),
	)

	raw := []domain.Reading{
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base, Value: 100},
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base.Add(15 * time.Minute), Value: 90}, // 回退被拒
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base.Add(30 * time.Minute), Value: 80}, // 回退被拒
	}
	result, err := pl.Execute(context.Background(), raw)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if len(result.Rejected) != 2 {
		t.Fatalf("expected 2 rejected, got %d", len(result.Rejected))
	}
	// QuarantineSink 保存次数与 Rejected 一致
	qRepo.mu.Lock()
	defer qRepo.mu.Unlock()
	if len(qRepo.saved) != 2 {
		t.Fatalf("expected 2 quarantine saves, got %d", len(qRepo.saved))
	}
	for i := range result.Rejected {
		if qRepo.saved[i].Reading.DeviceInfo.ID != result.Rejected[i].Reading.DeviceInfo.ID {
			t.Errorf("saved quarantine[%d] mismatch", i)
		}
	}
}

// TestPipelineDeliversAllSinks 多个 Sink 都收到相同 ProcessingResult
func TestPipelineDeliversAllSinks(t *testing.T) {
	base, _ := time.Parse(time.RFC3339, "2026-08-04T10:00:00Z")
	ms1 := sink.NewMemorySink()
	ms2 := sink.NewMemorySink()
	processor := services.NewEnergyDataProcessor()
	pl := pipeline.NewProcessingPipeline(processor, nil, nil, ms1, ms2)

	raw := []domain.Reading{
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base, Value: 100},
	}
	result, err := pl.Execute(context.Background(), raw)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if ms1.Count() != 1 || ms2.Count() != 1 {
		t.Fatalf("expected both sinks to receive 1 result, got %d / %d", ms1.Count(), ms2.Count())
	}
	// 所有 Sink 收到相同结果
	r1 := ms1.Results()[0]
	r2 := ms2.Results()[0]
	if len(r1.Accepted) != len(r2.Accepted) || len(r1.Accepted) != len(result.Accepted) {
		t.Errorf("unexpected delivered results: %+v vs %+v", r1, r2)
	}
}

// TestPipelineReturnsResultOnDeliveryFailure 一个 Sink 失败时其他 Sink 仍执行,
// 错误包含失败 Sink ID, 仍返回完整 ProcessingResult
func TestPipelineReturnsResultOnDeliveryFailure(t *testing.T) {
	base, _ := time.Parse(time.RFC3339, "2026-08-04T10:00:00Z")
	good := sink.NewMemorySink()
	bad := sink.NewCallbackSink("bad", func(_ context.Context, _ domain.ProcessingResult) error {
		return errors.New("boom")
	})
	processor := services.NewEnergyDataProcessor()
	pl := pipeline.NewProcessingPipeline(processor, nil, nil, bad, good)

	raw := []domain.Reading{
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base, Value: 100},
	}
	result, err := pl.Execute(context.Background(), raw)
	if err == nil {
		t.Fatal("expected delivery error")
	}
	if !strings.Contains(err.Error(), `sink "bad"`) || !strings.Contains(err.Error(), "boom") {
		t.Errorf("expected error with sink ID and original error, got: %v", err)
	}
	// 其余 Sink 仍执行
	if good.Count() != 1 {
		t.Errorf("expected good sink to still receive result, got %d", good.Count())
	}
	// 仍返回完整 ProcessingResult
	if len(result.Accepted) != 1 {
		t.Errorf("expected full result returned, got %d accepted", len(result.Accepted))
	}
}

// stubProcessor 忽略 ctx 的固定处理器 (用于隔离 Pipeline 上下文传播测试)
type stubProcessor struct{}

func (s *stubProcessor) Process(_ context.Context, _ []domain.Reading) (domain.ProcessingResult, error) {
	return domain.ProcessingResult{
		Accepted: []domain.StandardReading{{DeviceID: "D1"}},
		Rejected: []domain.QuarantineReading{
			{Reading: domain.Reading{DeviceInfo: domain.DeviceInfo{ID: "D9"}}},
		},
	}, nil
}
func (s *stubProcessor) ProcessAndStandardize(_ context.Context, _ []domain.Reading) ([]domain.StandardReading, error) {
	return nil, nil
}
func (s *stubProcessor) GetStandardReading(_ context.Context, _ string, _ time.Time) (*domain.StandardReading, error) {
	return nil, nil
}

// ctxCheckingRepo 记录收到的 context 是否已取消 (用于验证 Sink 使用原 ctx)
type ctxCheckingRepo struct {
	mu        sync.Mutex
	sawCancel bool
}

func (c *ctxCheckingRepo) Save(ctx context.Context, _ domain.StandardReading, _ ports.UpsertStrategy) error {
	if ctx.Err() != nil {
		c.mu.Lock()
		c.sawCancel = true
		c.mu.Unlock()
		return ctx.Err()
	}
	return nil
}
func (c *ctxCheckingRepo) SaveBatch(ctx context.Context, _ []domain.StandardReading, _ ports.UpsertStrategy) error {
	if ctx.Err() != nil {
		c.mu.Lock()
		c.sawCancel = true
		c.mu.Unlock()
		return ctx.Err()
	}
	return nil
}
func (c *ctxCheckingRepo) FindExact(_ context.Context, _ string, _ time.Time) (*domain.StandardReading, error) {
	return nil, nil
}
func (c *ctxCheckingRepo) FindRange(_ context.Context, _ string, _, _ time.Time) ([]domain.StandardReading, error) {
	return nil, nil
}
func (c *ctxCheckingRepo) sawCancelled() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sawCancel
}

// TestPipelinePropagatesContext Pipeline 将调用方 ctx 传递给 Sink,
// 不创建独立 Background Context; Context 错误正确返回。
func TestPipelinePropagatesContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 已取消的 Context

	repo := &ctxCheckingRepo{}
	qRepo := &ctxCheckingQuarantine{}
	pl := pipeline.NewProcessingPipeline(
		&stubProcessor{}, // 固定返回结果，隔离 Core 行为
		sink.NewRepositorySink(repo, ports.UpsertStrategyHighPriorityWins),
		sink.NewQuarantineSink(qRepo),
	)

	result, err := pl.Execute(ctx, []domain.Reading{{DeviceInfo: domain.DeviceInfo{ID: "D1"}}})
	// Sink 收到的是已取消的原 ctx -> 保存失败返回 context.Canceled -> 错误正确向上返回
	if err == nil {
		t.Fatal("expected context error to propagate")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected error to wrap context.Canceled, got: %v", err)
	}
	// 仓储确实收到了原 ctx (已取消)，而非新建的 Background
	if !repo.sawCancelled() {
		t.Errorf("expected RepositorySink to receive the original cancelled context")
	}
	if !qRepo.sawCancelled() {
		t.Errorf("expected QuarantineSink to receive the original cancelled context")
	}
	// 仍返回 ProcessingResult
	if len(result.Accepted) != 1 {
		t.Errorf("expected result returned, got %d accepted", len(result.Accepted))
	}
}

// ctxCheckingQuarantine 记录收到的 context 是否已取消
type ctxCheckingQuarantine struct {
	mu        sync.Mutex
	sawCancel bool
}

func (q *ctxCheckingQuarantine) Save(ctx context.Context, _ domain.QuarantineReading) error {
	if ctx.Err() != nil {
		q.mu.Lock()
		q.sawCancel = true
		q.mu.Unlock()
		return ctx.Err()
	}
	return nil
}
func (q *ctxCheckingQuarantine) FindPending(_ context.Context, _ int) ([]domain.QuarantineReading, error) {
	return nil, nil
}
func (q *ctxCheckingQuarantine) sawCancelled() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.sawCancel
}

var (
	_ ports.EnergyDataProcessor       = (*stubProcessor)(nil)
	_ ports.StandardReadingRepository = (*recordingRepo)(nil)
	_ ports.StandardReadingRepository = (*ctxCheckingRepo)(nil)
	_ ports.QuarantineRepository      = (*recordingQuarantine)(nil)
	_ ports.QuarantineRepository      = (*ctxCheckingQuarantine)(nil)
)
