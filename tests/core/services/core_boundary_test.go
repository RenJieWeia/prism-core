package services_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/RenJieWeia/prism-core/pkg/adapters/sink"
	"github.com/RenJieWeia/prism-core/pkg/core/domain"
	"github.com/RenJieWeia/prism-core/pkg/core/ports"
	"github.com/RenJieWeia/prism-core/pkg/core/services"
	"github.com/RenJieWeia/prism-core/pkg/core/services/rules"
)

// saveCounter 统计 Save / SaveBatch 调用次数，并模拟查询。
type saveCounter struct {
	mu          sync.Mutex
	saveBatch   int
	saves       int
	standardRow *domain.StandardReading // GetStandardReading 查询用
}

func (c *saveCounter) Save(_ context.Context, _ domain.StandardReading, _ ports.UpsertStrategy) error {
	c.mu.Lock()
	c.saves++
	c.mu.Unlock()
	return nil
}
func (c *saveCounter) SaveBatch(_ context.Context, _ []domain.StandardReading, _ ports.UpsertStrategy) error {
	c.mu.Lock()
	c.saveBatch++
	c.mu.Unlock()
	return nil
}
func (c *saveCounter) FindExact(_ context.Context, _ string, _ time.Time) (*domain.StandardReading, error) {
	row := c.standardRow
	return row, nil
}
func (c *saveCounter) FindRange(_ context.Context, _ string, _, _ time.Time) ([]domain.StandardReading, error) {
	return nil, nil
}
func (c *saveCounter) saveBatchCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.saveBatch
}
func (c *saveCounter) saveCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.saves
}

// quarantineCounter 统计 QuarantineRepository.Save 调用次数
type quarantineCounter struct {
	mu    sync.Mutex
	saves int
}

func (q *quarantineCounter) Save(_ context.Context, _ domain.QuarantineReading) error {
	q.mu.Lock()
	q.saves++
	q.mu.Unlock()
	return nil
}
func (q *quarantineCounter) FindPending(_ context.Context, _ int) ([]domain.QuarantineReading, error) {
	return nil, nil
}
func (q *quarantineCounter) saveCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.saves
}

// TestCoreProcessDoesNotPersistAccepted Core 不产生标准数据持久化副作用
func TestCoreProcessDoesNotPersistAccepted(t *testing.T) {
	base, _ := time.Parse(time.RFC3339, "2026-08-04T10:00:00Z")
	repo := &saveCounter{standardRow: &domain.StandardReading{DeviceID: "D1"}}

	processor := services.NewEnergyDataProcessor(services.WithRepository(repo))
	raw := []domain.Reading{
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base, Value: 100},
	}

	result, err := processor.Process(context.Background(), raw)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}
	if len(result.Accepted) != 1 {
		t.Fatalf("expected 1 accepted, got %d", len(result.Accepted))
	}
	// Core 不得调用 SaveBatch
	if got := repo.saveBatchCount(); got != 0 {
		t.Fatalf("expected 0 SaveBatch calls, got %d", got)
	}

	// GetStandardReading 仍可通过该仓储查询
	sr, err := processor.GetStandardReading(context.Background(), "D1", base)
	if err != nil {
		t.Fatalf("GetStandardReading failed: %v", err)
	}
	if sr == nil || sr.DeviceID != "D1" {
		t.Fatalf("expected GetStandardReading to return the stored row, got %+v", sr)
	}
}

// TestCoreProcessDoesNotPersistRejected Core 不保存隔离数据
func TestCoreProcessDoesNotPersistRejected(t *testing.T) {
	base, _ := time.Parse(time.RFC3339, "2026-08-04T10:00:00Z")
	qRepo := &quarantineCounter{}

	processor := services.NewEnergyDataProcessor(
		services.WithQuarantineRepository(qRepo), // 弃用选项: Core 不再自动保存隔离数据
		services.WithCleaningRules(&rules.MonotonicRule{Action: domain.ActionReject}),
	)

	raw := []domain.Reading{
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base, Value: 100},
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base.Add(15 * time.Minute), Value: 90}, // 回退被拒
	}

	result, err := processor.Process(context.Background(), raw)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}
	// Rejected 包含异常数据
	if len(result.Rejected) != 1 || result.Rejected[0].Reading.Value != 90 {
		t.Fatalf("expected 1 rejected reading (90), got %+v", result.Rejected)
	}
	// Core 不得调用 QuarantineRepository.Save
	if got := qRepo.saveCount(); got != 0 {
		t.Fatalf("expected 0 quarantine Save calls, got %d", got)
	}
}

// TestCoreProcessDoesNotDeliverSinks Core 不投递 Sink
func TestCoreProcessDoesNotDeliverSinks(t *testing.T) {
	base, _ := time.Parse(time.RFC3339, "2026-08-04T10:00:00Z")
	ms := sink.NewMemorySink()

	processor := services.NewEnergyDataProcessor(services.WithResultSinks(ms)) // 弃用选项: 无操作
	raw := []domain.Reading{
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base, Value: 100},
	}

	result, err := processor.Process(context.Background(), raw)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}
	if len(result.Accepted) != 1 {
		t.Fatalf("expected 1 accepted, got %d", len(result.Accepted))
	}
	if got := ms.Count(); got != 0 {
		t.Fatalf("expected MemorySink to receive 0 results from Core, got %d", got)
	}
}

var (
	_ ports.StandardReadingRepository = (*saveCounter)(nil)
	_ ports.QuarantineRepository      = (*quarantineCounter)(nil)
)
