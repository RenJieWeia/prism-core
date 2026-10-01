package services_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/RenJieWeia/prism-core/pkg/adapters/reference"
	"github.com/RenJieWeia/prism-core/pkg/core/domain"
	"github.com/RenJieWeia/prism-core/pkg/core/ports"
	"github.com/RenJieWeia/prism-core/pkg/core/services"
	"github.com/RenJieWeia/prism-core/pkg/core/services/rules"
)

// referenceCompareRule 测试用参考规则: 当前值 与 参考值 比较
// op = "ge": 当前 >= 参考; op = "le": 当前 <= 参考
type referenceCompareRule struct {
	id    string
	specs []domain.ReferenceSpec
	op    string
}

func (r *referenceCompareRule) RuleID() string { return r.id }

func (r *referenceCompareRule) ReferenceSpecs() []domain.ReferenceSpec { return r.specs }

func (r *referenceCompareRule) CheckWithReferences(in domain.RuleInput) ports.CheckResult {
	ref, ok := in.References[r.specs[0].ID]
	if !ok || !ref.Found {
		return ports.CheckResult{Reading: in.Current, Passed: false, Reason: "reference not found"}
	}
	switch r.op {
	case "ge":
		if in.Current.Value < ref.Value {
			return ports.CheckResult{
				Reading: in.Current, Passed: false,
				Reason: "current below reference",
			}
		}
	case "le":
		if in.Current.Value > ref.Value {
			return ports.CheckResult{
				Reading: in.Current, Passed: false,
				Reason: "current above reference",
			}
		}
	}
	return ports.CheckResult{Reading: in.Current, Passed: true}
}

func previousSpec(policy domain.MissingReferencePolicy) domain.ReferenceSpec {
	return domain.ReferenceSpec{
		ID:            "prev",
		Source:        domain.ReferenceSourceCurrentBatch,
		Binding:       domain.ReferenceBindingSameDevice,
		Time:          domain.ReferenceTimeSelector{Mode: domain.ReferenceTimePrevious},
		MissingPolicy: policy,
	}
}

func relativeSpec(policy domain.MissingReferencePolicy) domain.ReferenceSpec {
	return domain.ReferenceSpec{
		ID:            "d3",
		Source:        domain.ReferenceSourceCurrentBatch,
		Binding:       domain.ReferenceBindingSameDevice,
		Time:          domain.ReferenceTimeSelector{Mode: domain.ReferenceTimeRelative, Offset: 72 * time.Hour},
		MissingPolicy: policy,
	}
}

// TestReferenceRulePreviousValue 上一条数据参考: 当前数据可获取同一设备上一条有效数据
func TestReferenceRulePreviousValue(t *testing.T) {
	base, _ := time.Parse(time.RFC3339, "2026-08-04T10:00:00Z")
	rule := &referenceCompareRule{id: "prev-ge", specs: []domain.ReferenceSpec{previousSpec(domain.MissingReferenceSkip)}, op: "ge"}
	sanitizer := services.NewSanitizerWithReferences(rule)

	readings := []domain.Reading{
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base, Value: 100},
		{DeviceInfo: domain.DeviceInfo{ID: "D2"}, Timestamp: base.Add(1 * time.Minute), Value: 50},
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base.Add(2 * time.Minute), Value: 90},
	}

	cleanRes, err := sanitizer.CleanWithReferences(context.Background(), readings, nil)
	if err != nil {
		t.Fatalf("clean failed: %v", err)
	}

	// D1 第二条 (90) 与 D1 第一条 (100) 比较 -> 拒绝
	if len(cleanRes.Clean) != 2 {
		t.Fatalf("expected 2 clean readings, got %d", len(cleanRes.Clean))
	}
	if len(cleanRes.Quarantined) != 1 {
		t.Fatalf("expected 1 quarantined reading, got %d", len(cleanRes.Quarantined))
	}
	if cleanRes.Quarantined[0].Reading.DeviceInfo.ID != "D1" {
		t.Fatalf("expected D1 to be quarantined, got %+v", cleanRes.Quarantined[0].Reading)
	}

	// 评估记录应包含参考对象 ID
	var foundEval bool
	for _, ev := range cleanRes.Evaluations {
		if ev.RuleID == "prev-ge" && ev.Outcome == string(domain.RuleOutcomeReject) {
			foundEval = true
			if len(ev.ReferenceIDs) == 0 || ev.ReferenceIDs[0] != "prev" {
				t.Errorf("expected reference ID 'prev' in evaluation, got %v", ev.ReferenceIDs)
			}
		}
	}
	if !foundEval {
		t.Errorf("expected a REJECT evaluation from rule prev-ge, got %+v", cleanRes.Evaluations)
	}
}

// TestReferenceMissingPolicySkip SKIP_RULE: 规则被跳过，数据继续通过
func TestReferenceMissingPolicySkip(t *testing.T) {
	base, _ := time.Parse(time.RFC3339, "2026-08-04T10:00:00Z")
	rule := &referenceCompareRule{id: "skip-rule", specs: []domain.ReferenceSpec{relativeSpec(domain.MissingReferenceSkip)}, op: "ge"}
	sanitizer := services.NewSanitizerWithReferences(rule)

	// 只有一条当前数据，无三天前数据 -> 参考缺失
	readings := []domain.Reading{
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base, Value: 100},
	}
	cleanRes, err := sanitizer.CleanWithReferences(context.Background(), readings, nil)
	if err != nil {
		t.Fatalf("clean failed: %v", err)
	}

	if len(cleanRes.Clean) != 1 {
		t.Fatalf("expected reading accepted under SKIP_RULE, got clean=%d quarantined=%d",
			len(cleanRes.Clean), len(cleanRes.Quarantined))
	}
	var skipFound bool
	for _, ev := range cleanRes.Evaluations {
		if ev.RuleID == "skip-rule" && ev.Outcome == string(domain.RuleOutcomeSkip) {
			skipFound = true
			if ev.Reason == "" {
				t.Error("expected skip reason")
			}
		}
	}
	if !skipFound {
		t.Errorf("expected SKIP evaluation, got %+v", cleanRes.Evaluations)
	}
}

// TestReferenceMissingPolicyReject REJECT: 参考缺失时直接拒绝当前数据
func TestReferenceMissingPolicyReject(t *testing.T) {
	base, _ := time.Parse(time.RFC3339, "2026-08-04T10:00:00Z")
	rule := &referenceCompareRule{id: "reject-rule", specs: []domain.ReferenceSpec{relativeSpec(domain.MissingReferenceReject)}, op: "ge"}
	sanitizer := services.NewSanitizerWithReferences(rule)

	readings := []domain.Reading{
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base, Value: 100},
	}
	cleanRes, err := sanitizer.CleanWithReferences(context.Background(), readings, nil)
	if err != nil {
		t.Fatalf("clean failed: %v", err)
	}

	if len(cleanRes.Clean) != 0 {
		t.Errorf("expected 0 clean readings under REJECT, got %d", len(cleanRes.Clean))
	}
	if len(cleanRes.Quarantined) != 1 {
		t.Fatalf("expected 1 quarantined reading under REJECT, got %d", len(cleanRes.Quarantined))
	}
	var rejectFound bool
	for _, ev := range cleanRes.Evaluations {
		if ev.RuleID == "reject-rule" && ev.Outcome == string(domain.RuleOutcomeReject) {
			rejectFound = true
		}
	}
	if !rejectFound {
		t.Errorf("expected REJECT evaluation, got %+v", cleanRes.Evaluations)
	}
}

// TestReferenceMissingPolicyQuarantine QUARANTINE: 参考缺失时隔离当前数据
func TestReferenceMissingPolicyQuarantine(t *testing.T) {
	base, _ := time.Parse(time.RFC3339, "2026-08-04T10:00:00Z")
	rule := &referenceCompareRule{id: "quarantine-rule", specs: []domain.ReferenceSpec{relativeSpec(domain.MissingReferenceQuarantine)}, op: "ge"}
	sanitizer := services.NewSanitizerWithReferences(rule)

	readings := []domain.Reading{
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base, Value: 100},
	}
	cleanRes, err := sanitizer.CleanWithReferences(context.Background(), readings, nil)
	if err != nil {
		t.Fatalf("clean failed: %v", err)
	}

	if len(cleanRes.Clean) != 0 {
		t.Errorf("expected 0 clean readings under QUARANTINE, got %d", len(cleanRes.Clean))
	}
	if len(cleanRes.Quarantined) != 1 {
		t.Fatalf("expected 1 quarantined reading, got %d", len(cleanRes.Quarantined))
	}
	var quarantineFound bool
	for _, ev := range cleanRes.Evaluations {
		if ev.RuleID == "quarantine-rule" && ev.Outcome == string(domain.RuleOutcomeQuarantine) {
			quarantineFound = true
		}
	}
	if !quarantineFound {
		t.Errorf("expected QUARANTINE evaluation, got %+v", cleanRes.Evaluations)
	}
}

// countingRepo 计数仓储: 统计 FindRange 调用次数 (查询去重验证)
type countingRepo struct {
	mu         sync.Mutex
	rangeCalls int
	exactCalls int
	rows       map[string][]domain.StandardReading // key: deviceID
}

func (c *countingRepo) Save(_ context.Context, _ domain.StandardReading, _ ports.UpsertStrategy) error {
	return nil
}
func (c *countingRepo) SaveBatch(_ context.Context, _ []domain.StandardReading, _ ports.UpsertStrategy) error {
	return nil
}
func (c *countingRepo) FindExact(_ context.Context, _ string, _ time.Time) (*domain.StandardReading, error) {
	c.mu.Lock()
	c.exactCalls++
	c.mu.Unlock()
	return nil, nil
}
func (c *countingRepo) FindRange(_ context.Context, deviceID string, start, end time.Time) ([]domain.StandardReading, error) {
	c.mu.Lock()
	c.rangeCalls++
	c.mu.Unlock()

	var out []domain.StandardReading
	for _, r := range c.rows[deviceID] {
		if !r.Timestamp.Before(start) && !r.Timestamp.After(end) {
			out = append(out, r)
		}
	}
	return out, nil
}
func (c *countingRepo) rangeCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rangeCalls
}

// TestReferenceQueryDedup 查询去重: 两个规则引用相同参考对象时，参考数据源只执行一次相同查询
func TestReferenceQueryDedup(t *testing.T) {
	base, _ := time.Parse(time.RFC3339, "2026-08-04T10:00:00Z")
	target, _ := time.Parse(time.RFC3339, "2026-08-01T10:00:00Z")

	repo := &countingRepo{rows: map[string][]domain.StandardReading{
		"D1": {{DeviceID: "D1", Timestamp: target, ValueDisplay: 300}},
	}}
	repoRefs := reference.NewRepositoryReferenceSource(repo, time.Minute)

	sharedSpec := domain.ReferenceSpec{
		ID:            "shared",
		Source:        domain.ReferenceSourceStandardRepo,
		Binding:       domain.ReferenceBindingSameDevice,
		Time:          domain.ReferenceTimeSelector{Mode: domain.ReferenceTimeRelative, Offset: 72 * time.Hour, Tolerance: time.Minute},
		MissingPolicy: domain.MissingReferenceSkip,
	}
	rule1 := &referenceCompareRule{id: "r1", specs: []domain.ReferenceSpec{sharedSpec}, op: "ge"}
	rule2 := &referenceCompareRule{id: "r2", specs: []domain.ReferenceSpec{sharedSpec}, op: "ge"}

	sanitizer := services.NewSanitizerWithReferences(rule1, rule2)
	readings := []domain.Reading{
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base, Value: 320},
	}
	cleanRes, err := sanitizer.CleanWithReferences(context.Background(), readings, repoRefs)
	if err != nil {
		t.Fatalf("clean failed: %v", err)
	}

	// 相同查询只执行一次
	if got := repo.rangeCount(); got != 1 {
		t.Fatalf("expected 1 FindRange call for shared reference, got %d", got)
	}
	// 两个规则都执行通过
	if len(cleanRes.Clean) != 1 {
		t.Fatalf("expected reading accepted, got clean=%d", len(cleanRes.Clean))
	}
	var passCount int
	for _, ev := range cleanRes.Evaluations {
		if ev.Outcome == string(domain.RuleOutcomePass) {
			passCount++
		}
	}
	if passCount != 2 {
		t.Errorf("expected 2 PASS evaluations (r1, r2), got %d", passCount)
	}
}

// TestPreviousReferenceDoesNotUseRejectedReading CURRENT_BATCH PREVIOUS 必须使用同设备
// 已通过规则链的有效数据，不得从包含被拒绝数据的原始批次中选择 PREVIOUS。
// 场景: D1 10:00=100(通过) / 10:15=90(被 Range 规则拒绝) / 10:30=95。
// PREVIOUS 应指向有效值 100 (而非被拒绝的 90)，因此 95 < 100 被参考规则拒绝;
// 若错误地使用原始批次的上一条 (90)，则 95 >= 90 会误通过。
func TestPreviousReferenceDoesNotUseRejectedReading(t *testing.T) {
	base, _ := time.Parse(time.RFC3339, "2026-08-04T10:00:00Z")

	// 先拒绝中间读数 90 的旧规则，与参考规则组合 (由 CoreStandardizer 内部适配器混合)
	rejectMid := &rules.RangeRule{Min: 93, Max: 200, Action: domain.ActionReject}
	refRule := &referenceCompareRule{
		id:    "prev-ge",
		specs: []domain.ReferenceSpec{previousSpec(domain.MissingReferenceSkip)},
		op:    "ge",
	}
	std := services.NewEnergyDataProcessor(
		services.WithCleaningRules(rejectMid),
		services.WithReferenceRules(refRule),
	)

	readings := []domain.Reading{
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base, Value: 100},
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base.Add(15 * time.Minute), Value: 90},
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base.Add(30 * time.Minute), Value: 95},
	}

	result, err := std.Process(context.Background(), readings)
	if err != nil {
		t.Fatalf("process failed: %v", err)
	}

	// 10:30 的 PREVIOUS 必须是有效值 100 -> 95 < 100 被拒绝
	var rejected30 bool
	for _, ev := range result.Evaluations {
		if ev.RuleID == "prev-ge" && ev.Outcome == string(domain.RuleOutcomeReject) {
			rejected30 = true
		}
	}
	if !rejected30 {
		t.Fatalf("expected 10:30 reading rejected against valid previous 100, got evaluations %+v", result.Evaluations)
	}
	// 10:15 的 90 被 Range 规则拒绝 + 10:30 的 95 被参考规则拒绝 -> 隔离 2 条
	if len(result.Rejected) != 2 {
		t.Fatalf("expected 2 rejected (90 by Range, 95 by ref rule), got %d: %+v", len(result.Rejected), result.Rejected)
	}
	// 仅 10:00 通过
	if len(result.Accepted) != 1 {
		t.Fatalf("expected only 10:00 accepted, got %+v", result.Accepted)
	}
}

var _ ports.StandardReadingRepository = (*countingRepo)(nil)
