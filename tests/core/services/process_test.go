package services_test

import (
	"context"
	"testing"
	"time"

	"github.com/RenJieWeia/prism-core/pkg/core/domain"
	"github.com/RenJieWeia/prism-core/pkg/core/services"
	"github.com/RenJieWeia/prism-core/pkg/core/services/rules"
)

// TestProcessReturnsFullResult 验证 Process 返回完整处理结果
func TestProcessReturnsFullResult(t *testing.T) {
	base, _ := time.Parse(time.RFC3339, "2026-08-04T10:00:00Z")
	std := services.NewEnergyDataProcessor(
		services.WithAlignment(15*time.Minute, 5*time.Minute),
		services.WithCleaningRules(&rules.MonotonicRule{Action: domain.ActionReject}),
	)

	raw := []domain.Reading{
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base, Value: 100},
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base.Add(15 * time.Minute), Value: 90},
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base.Add(30 * time.Minute), Value: 110},
	}

	result, err := std.Process(context.Background(), raw)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	// Accepted: 10:00 100 与 10:30 110 (10:15 的回退被拒绝)
	if len(result.Accepted) != 2 {
		t.Fatalf("expected 2 accepted, got %d", len(result.Accepted))
	}
	// Rejected: 1 条
	if len(result.Rejected) != 1 {
		t.Fatalf("expected 1 rejected, got %d", len(result.Rejected))
	}
	if result.Rejected[0].Reading.Value != 90 {
		t.Errorf("expected rejected value 90, got %v", result.Rejected[0].Reading.Value)
	}
	// Evaluations: 每条读数对每条规则一次评估 = 3 次
	if len(result.Evaluations) != 3 {
		t.Fatalf("expected 3 evaluations, got %d", len(result.Evaluations))
	}
	var rejectFound bool
	for _, ev := range result.Evaluations {
		if ev.Outcome == string(domain.RuleOutcomeReject) && ev.Reason != "" {
			rejectFound = true
		}
	}
	if !rejectFound {
		t.Errorf("expected a REJECT evaluation with reason, got %+v", result.Evaluations)
	}
}

// TestProcessAndStandardizeCompatibility 向后兼容: 原方法只返回 Accepted
func TestProcessAndStandardizeCompatibility(t *testing.T) {
	base, _ := time.Parse(time.RFC3339, "2026-08-04T10:00:00Z")
	std := services.NewEnergyDataProcessor(
		services.WithCleaningRules(&rules.MonotonicRule{Action: domain.ActionReject}),
	)

	raw := []domain.Reading{
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base, Value: 100},
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base.Add(15 * time.Minute), Value: 90},
	}

	accepted, err := std.ProcessAndStandardize(context.Background(), raw)
	if err != nil {
		t.Fatalf("ProcessAndStandardize failed: %v", err)
	}

	// 与 Process().Accepted 一致
	full, err := std.Process(context.Background(), raw)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}
	if len(accepted) != len(full.Accepted) {
		t.Fatalf("expected same accepted count, got %d vs %d", len(accepted), len(full.Accepted))
	}
	if len(full.Rejected) != 1 {
		t.Fatalf("expected 1 rejected, got %d", len(full.Rejected))
	}
}

// TestProcessEmptyInput 空输入不应报错
func TestProcessEmptyInput(t *testing.T) {
	std := services.NewEnergyDataProcessor()
	result, err := std.Process(context.Background(), nil)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}
	if len(result.Accepted) != 0 || len(result.Rejected) != 0 {
		t.Errorf("expected empty result for empty input")
	}
}
