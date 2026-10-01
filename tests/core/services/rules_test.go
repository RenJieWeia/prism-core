package services_test

import (
	"testing"
	"time"

	"github.com/RenJieWeia/prism-core/pkg/core/domain"
	"github.com/RenJieWeia/prism-core/pkg/core/ports"
	"github.com/RenJieWeia/prism-core/pkg/core/services/rules"
)

func helperReading(ts time.Time, v float64) domain.Reading {
	return domain.Reading{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: ts, Value: v}
}

func TestMonotonicRule(t *testing.T) {
	base, _ := time.Parse(time.RFC3339, "2023-01-01T10:00:00Z")
	prev := helperReading(base, 100)
	curr := helperReading(base.Add(15*time.Minute), 90)

	t.Run("REJECT 回退", func(t *testing.T) {
		r := &rules.MonotonicRule{Action: domain.ActionReject}
		res := r.Check(ports.CleaningContext{Previous: &prev}, curr)
		if res.Passed {
			t.Fatalf("expected regression to be rejected")
		}
		if res.Reason == "" {
			t.Errorf("expected rejection reason")
		}
	})

	t.Run("CORRECT 钳制", func(t *testing.T) {
		r := &rules.MonotonicRule{Action: domain.ActionCorrect}
		res := r.Check(ports.CleaningContext{Previous: &prev}, curr)
		if !res.Passed || !res.Corrected {
			t.Fatalf("expected corrected pass, got passed=%v corrected=%v", res.Passed, res.Corrected)
		}
		if res.Reading.Value != 100 {
			t.Errorf("expected clamped value 100, got %v", res.Reading.Value)
		}
	})

	t.Run("首条数据放行", func(t *testing.T) {
		r := &rules.MonotonicRule{}
		if res := r.Check(ports.CleaningContext{}, curr); !res.Passed {
			t.Fatalf("expected first reading to pass")
		}
	})

	t.Run("正常递增放行", func(t *testing.T) {
		r := &rules.MonotonicRule{}
		up := helperReading(base.Add(15*time.Minute), 110)
		if res := r.Check(ports.CleaningContext{Previous: &prev}, up); !res.Passed {
			t.Fatalf("expected increase to pass")
		}
	})
}

func TestRateRule(t *testing.T) {
	base, _ := time.Parse(time.RFC3339, "2023-01-01T10:00:00Z")
	prev := helperReading(base, 100)
	// 10 秒内允许最大变化 10 (即每秒 1)
	r := &rules.RateRule{MaxRatePerSecond: 1}

	t.Run("尖峰拒绝", func(t *testing.T) {
		spike := helperReading(base.Add(10*time.Second), 130) // delta 30 > 10
		res := r.Check(ports.CleaningContext{Previous: &prev}, spike)
		if res.Passed {
			t.Fatalf("expected spike to be rejected")
		}
	})

	t.Run("正常变化放行", func(t *testing.T) {
		ok := helperReading(base.Add(10*time.Second), 108) // delta 8 <= 10
		if res := r.Check(ports.CleaningContext{Previous: &prev}, ok); !res.Passed {
			t.Fatalf("expected normal change to pass, got %s", res.Reason)
		}
	})

	t.Run("CORRECT 钳制", func(t *testing.T) {
		corr := &rules.RateRule{MaxRatePerSecond: 1, Action: domain.ActionCorrect}
		spike := helperReading(base.Add(10*time.Second), 130)
		res := corr.Check(ports.CleaningContext{Previous: &prev}, spike)
		if !res.Passed || !res.Corrected {
			t.Fatalf("expected corrected pass")
		}
		if res.Reading.Value != 110 { // 100 + 10
			t.Errorf("expected clamped value 110, got %v", res.Reading.Value)
		}
	})

	t.Run("无效参数放行", func(t *testing.T) {
		bad := &rules.RateRule{MaxRatePerSecond: 0}
		spike := helperReading(base.Add(10*time.Second), 130)
		if res := bad.Check(ports.CleaningContext{Previous: &prev}, spike); !res.Passed {
			t.Fatalf("expected disabled rule to pass")
		}
	})
}

func TestStagnationRule(t *testing.T) {
	base, _ := time.Parse(time.RFC3339, "2023-01-01T10:00:00Z")
	prev := helperReading(base, 100)
	repeat := helperReading(base.Add(15*time.Minute), 100)

	t.Run("REJECT 停滞", func(t *testing.T) {
		r := &rules.StagnationRule{Action: domain.ActionReject}
		res := r.Check(ports.CleaningContext{Previous: &prev}, repeat)
		if res.Passed {
			t.Fatalf("expected stagnation to be rejected")
		}
	})

	t.Run("FLAG_ONLY 保留并标记", func(t *testing.T) {
		r := &rules.StagnationRule{Action: domain.ActionFlagOnly}
		res := r.Check(ports.CleaningContext{Previous: &prev}, repeat)
		if !res.Passed || !res.Corrected {
			t.Fatalf("expected flag-only pass, got passed=%v corrected=%v", res.Passed, res.Corrected)
		}
		if res.Reading.Value != 100 {
			t.Errorf("expected value preserved, got %v", res.Reading.Value)
		}
	})

	t.Run("变化放行", func(t *testing.T) {
		r := &rules.StagnationRule{}
		changed := helperReading(base.Add(15*time.Minute), 101)
		if res := r.Check(ports.CleaningContext{Previous: &prev}, changed); !res.Passed {
			t.Fatalf("expected changed value to pass")
		}
	})
}

func TestRangeRule(t *testing.T) {
	base, _ := time.Parse(time.RFC3339, "2023-01-01T10:00:00Z")
	curr := helperReading(base, 50)

	t.Run("范围内放行", func(t *testing.T) {
		r := &rules.RangeRule{Min: 0, Max: 100}
		if res := r.Check(ports.CleaningContext{}, curr); !res.Passed {
			t.Fatalf("expected in-range value to pass")
		}
	})

	t.Run("REJECT 越界", func(t *testing.T) {
		r := &rules.RangeRule{Min: 0, Max: 100, Action: domain.ActionReject}
		low := helperReading(base, -1)
		if res := r.Check(ports.CleaningContext{}, low); res.Passed {
			t.Fatalf("expected out-of-range value to be rejected")
		}
	})

	t.Run("CORRECT 钳制", func(t *testing.T) {
		r := &rules.RangeRule{Min: 0, Max: 100, Action: domain.ActionCorrect}
		high := helperReading(base, 150)
		res := r.Check(ports.CleaningContext{}, high)
		if !res.Passed || !res.Corrected || res.Reading.Value != 100 {
			t.Fatalf("expected clamp to max, got %+v", res)
		}
	})
}
