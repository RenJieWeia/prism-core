package services_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/RenJieWeia/prism-core/pkg/adapters/reference"
	"github.com/RenJieWeia/prism-core/pkg/core/domain"
	"github.com/RenJieWeia/prism-core/pkg/core/ports"
	"github.com/RenJieWeia/prism-core/pkg/core/services"
)

// negativeOffsetSpec 负 Offset 的 RELATIVE 参考对象 (非法配置)
func negativeOffsetSpec() domain.ReferenceSpec {
	return domain.ReferenceSpec{
		ID:            "neg",
		Source:        domain.ReferenceSourceCurrentBatch,
		Binding:       domain.ReferenceBindingSameDevice,
		Time:          domain.ReferenceTimeSelector{Mode: domain.ReferenceTimeRelative, Offset: -72 * time.Hour},
		MissingPolicy: domain.MissingReferenceSkip,
	}
}

func standardRepoSrcSpec() domain.ReferenceSpec {
	return domain.ReferenceSpec{
		ID:            "repo-src",
		Source:        domain.ReferenceSourceStandardRepo,
		Binding:       domain.ReferenceBindingSameDevice,
		Time:          domain.ReferenceTimeSelector{Mode: domain.ReferenceTimeRelative, Offset: 72 * time.Hour, Tolerance: time.Minute},
		MissingPolicy: domain.MissingReferenceSkip,
	}
}

// TestReferenceRuleMissingSourceConfigError 参考规则声明 STANDARD_REPO 但未配置仓储源 -> 明确配置错误
func TestReferenceRuleMissingSourceConfigError(t *testing.T) {
	base, _ := time.Parse(time.RFC3339, "2026-08-04T10:00:00Z")
	rule := &referenceCompareRule{id: "needs-repo", specs: []domain.ReferenceSpec{standardRepoSrcSpec()}, op: "ge"}
	sanitizer := services.NewSanitizerWithReferences(rule)

	cleanRes, err := sanitizer.CleanWithReferences(context.Background(), []domain.Reading{
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base, Value: 100},
	}, nil)
	if err == nil {
		t.Fatal("expected config error when STANDARD_REPO source missing")
	}
	if !strings.Contains(err.Error(), "no repository reference source is configured") {
		t.Errorf("expected clear config error, got: %v", err)
	}
	if len(cleanRes.Clean) != 0 {
		t.Errorf("expected no data cleaned on config error, got %d", len(cleanRes.Clean))
	}
}

// TestProcessReferenceRuleMissingSourceConfigError 经 Process 配置参考规则但缺源 -> 报错而非静默通过
func TestProcessReferenceRuleMissingSourceConfigError(t *testing.T) {
	base, _ := time.Parse(time.RFC3339, "2026-08-04T10:00:00Z")
	rule := &referenceCompareRule{id: "needs-repo", specs: []domain.ReferenceSpec{standardRepoSrcSpec()}, op: "ge"}
	std := services.NewEnergyDataProcessor(services.WithReferenceRules(rule))

	_, err := std.Process(context.Background(), []domain.Reading{
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base, Value: 100},
	})
	if err == nil {
		t.Fatal("expected error when reference rule configured without repository reference source")
	}
	if !strings.Contains(err.Error(), "no repository reference source is configured") {
		t.Errorf("expected clear config error, got: %v", err)
	}
}

// TestReferenceRuleNegativeOffsetConfigError 负 Offset 在验证阶段报错
func TestReferenceRuleNegativeOffsetConfigError(t *testing.T) {
	base, _ := time.Parse(time.RFC3339, "2026-08-04T10:00:00Z")
	rule := &referenceCompareRule{id: "neg-offset", specs: []domain.ReferenceSpec{negativeOffsetSpec()}, op: "ge"}
	sanitizer := services.NewSanitizerWithReferences(rule)

	_, err := sanitizer.CleanWithReferences(context.Background(), []domain.Reading{
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base, Value: 100},
	}, nil)
	if err == nil {
		t.Fatal("expected config error for negative Offset")
	}
	if !strings.Contains(err.Error(), "negative Offset") {
		t.Errorf("expected negative Offset error, got: %v", err)
	}
}

// TestProcessReferenceRuleNegativeOffsetConfigError 经 Process 校验负 Offset
func TestProcessReferenceRuleNegativeOffsetConfigError(t *testing.T) {
	base, _ := time.Parse(time.RFC3339, "2026-08-04T10:00:00Z")
	rule := &referenceCompareRule{id: "neg-offset", specs: []domain.ReferenceSpec{negativeOffsetSpec()}, op: "ge"}
	std := services.NewEnergyDataProcessor(services.WithReferenceRules(rule))

	_, err := std.Process(context.Background(), []domain.Reading{
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base, Value: 100},
	})
	if err == nil {
		t.Fatal("expected error for negative Offset")
	}
	if !strings.Contains(err.Error(), "negative Offset") {
		t.Errorf("expected negative Offset error, got: %v", err)
	}
}

// TestProcessReferenceRuleWithSourceWorks 配置了仓储源时参考规则正常执行 (对照: 非静默退化)
func TestProcessReferenceRuleWithSourceWorks(t *testing.T) {
	base, _ := time.Parse(time.RFC3339, "2026-08-04T10:00:00Z")
	target, _ := time.Parse(time.RFC3339, "2026-08-01T10:00:00Z")

	repo := &countingRepo{rows: map[string][]domain.StandardReading{
		"D1": {{DeviceID: "D1", Timestamp: target, ValueDisplay: 300}},
	}}
	src := reference.NewRepositoryReferenceSource(repo, time.Minute)

	rule := &referenceCompareRule{id: "needs-repo", specs: []domain.ReferenceSpec{standardRepoSrcSpec()}, op: "ge"}
	std := services.NewEnergyDataProcessor(
		services.WithReferenceRules(rule),
		services.WithReferenceSource(src),
	)

	result, err := std.Process(context.Background(), []domain.Reading{
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base, Value: 320},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Accepted) != 1 {
		t.Fatalf("expected reading accepted when source configured, got %d", len(result.Accepted))
	}
}

var _ ports.ReferenceCleaningRule = (*referenceCompareRule)(nil)
