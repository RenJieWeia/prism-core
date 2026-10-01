package services_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/RenJieWeia/prism-core/pkg/adapters/factory"
	"github.com/RenJieWeia/prism-core/pkg/core/domain"
	"github.com/RenJieWeia/prism-core/pkg/core/ports"
	"github.com/RenJieWeia/prism-core/pkg/core/services"
)

// fakeRuleRepo 内存版规则仓储 (测试用)
type fakeRuleRepo struct {
	rules map[domain.DeviceType][]domain.CleaningRule
}

func (f *fakeRuleRepo) Save(_ context.Context, rule domain.CleaningRule) error { return nil }
func (f *fakeRuleRepo) GetByID(_ context.Context, id string) (*domain.CleaningRule, error) {
	return nil, nil
}
func (f *fakeRuleRepo) ListByDeviceType(_ context.Context, dt domain.DeviceType) ([]domain.CleaningRule, error) {
	return f.rules[dt], nil
}
func (f *fakeRuleRepo) ListEnabledByDeviceType(_ context.Context, dt domain.DeviceType) ([]domain.CleaningRule, error) {
	var out []domain.CleaningRule
	for _, r := range f.rules[dt] {
		if r.Enabled {
			out = append(out, r)
		}
	}
	return out, nil
}
func (f *fakeRuleRepo) Delete(_ context.Context, id string) error { return nil }

// quarantineRecorder 记录隔离区数据保存 (Core 不再自动保存，用于断言无副作用)
type quarantineRecorder struct {
	mu    sync.Mutex
	saves []domain.QuarantineReading
}

func (q *quarantineRecorder) Save(_ context.Context, record domain.QuarantineReading) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.saves = append(q.saves, record)
	return nil
}
func (q *quarantineRecorder) FindPending(_ context.Context, limit int) ([]domain.QuarantineReading, error) {
	return nil, nil
}
func (q *quarantineRecorder) count() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.saves)
}

func TestProcessWithDynamicRules(t *testing.T) {
	base, _ := time.Parse(time.RFC3339, "2023-01-01T10:00:00Z")
	ruleRepo := &fakeRuleRepo{rules: map[domain.DeviceType][]domain.CleaningRule{
		domain.DeviceTypeElec: {
			{ID: "r1", Type: domain.RuleTypeRange, Enabled: true, Parameters: map[string]interface{}{"min": float64(0), "max": float64(1000)}},
		},
	}}
	quarantine := &quarantineRecorder{}

	standardizer := services.NewEnergyDataProcessor(
		services.WithRuleRepository(ruleRepo),
		services.WithRuleFactory(factory.NewRuleFactory()),
		services.WithQuarantineRepository(quarantine), // 弃用选项: Core 不再自动保存隔离数据
	)

	readings := []domain.Reading{
		{DeviceInfo: domain.DeviceInfo{ID: "D1", Type: domain.DeviceTypeElec}, Timestamp: base, Value: 100},
		{DeviceInfo: domain.DeviceInfo{ID: "D2", Type: domain.DeviceTypeElec}, Timestamp: base.Add(time.Minute), Value: -5}, // 越界
	}

	result, err := standardizer.Process(context.Background(), readings)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}
	if len(result.Accepted) != 1 {
		t.Fatalf("expected 1 clean reading, got %d", len(result.Accepted))
	}
	// 越界数据进入 Rejected
	if len(result.Rejected) != 1 || result.Rejected[0].Reading.Value != -5 {
		t.Fatalf("expected 1 rejected reading (-5), got %+v", result.Rejected)
	}
	// Core 不再保存隔离数据
	if quarantine.count() != 0 {
		t.Fatalf("expected 0 quarantine saves from Core, got %d", quarantine.count())
	}
}

func TestDynamicCleaningRequiresFactory(t *testing.T) {
	// 配置了规则仓储但未注入规则工厂 -> 应报错 (依赖倒置保护)
	ruleRepo := &fakeRuleRepo{rules: map[domain.DeviceType][]domain.CleaningRule{}}
	standardizer := services.NewCoreStandardizer(
		services.WithRuleRepository(ruleRepo),
	)
	base, _ := time.Parse(time.RFC3339, "2023-01-01T10:00:00Z")
	readings := []domain.Reading{
		{DeviceInfo: domain.DeviceInfo{ID: "D1", Type: domain.DeviceTypeElec}, Timestamp: base, Value: 100},
	}
	if _, err := standardizer.ProcessAndStandardize(context.Background(), readings); err == nil {
		t.Fatalf("expected error when rule factory is missing")
	}
}

var _ ports.CleaningRuleRepository = (*fakeRuleRepo)(nil)
var _ ports.QuarantineRepository = (*quarantineRecorder)(nil)
