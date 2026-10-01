package domain_test

import (
	"testing"
	"time"

	"github.com/RenJieWeia/prism-core/pkg/core/domain"
)

func TestAlignerFindSnapshot(t *testing.T) {
	base, _ := time.Parse(time.RFC3339, "2023-01-01T10:00:00Z")
	readings := []domain.Reading{
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base, Value: 100},
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base.Add(15 * time.Minute), Value: 105},
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base.Add(30 * time.Minute), Value: 110},
	}

	aligner := domain.NewAligner(5 * time.Minute)

	t.Run("精确匹配", func(t *testing.T) {
		snap := aligner.FindSnapshot(readings, base)
		if snap == nil || snap.Value != 100 {
			t.Fatalf("exact match failed: %+v", snap)
		}
	})

	t.Run("容差内对齐", func(t *testing.T) {
		// 10:02 -> 距离 10:00 (2m) 比 10:15 (13m) 更近，应命中 10:00
		snap := aligner.FindSnapshot(readings, base.Add(2*time.Minute))
		if snap == nil || snap.Value != 100 {
			t.Fatalf("tolerance match failed: %+v", snap)
		}
	})

	t.Run("容差内取更近者", func(t *testing.T) {
		// 10:14 -> 距离 10:00 (14m) 超过容差, 但 10:15 (1m) 在容差内，应命中 10:15
		snap := aligner.FindSnapshot(readings, base.Add(14*time.Minute))
		if snap == nil || snap.Value != 105 {
			t.Fatalf("nearest match failed: %+v", snap)
		}
	})

	t.Run("超出容差返回空", func(t *testing.T) {
		// 10:08 -> 距离两边均超过 5m 容差
		snap := aligner.FindSnapshot(readings, base.Add(8*time.Minute))
		if snap != nil {
			t.Fatalf("expected nil, got %+v", snap)
		}
	})

	t.Run("空输入返回空", func(t *testing.T) {
		if snap := aligner.FindSnapshot(nil, base); snap != nil {
			t.Fatalf("expected nil for empty input, got %+v", snap)
		}
	})

	t.Run("目标在首点之前", func(t *testing.T) {
		snap := aligner.FindSnapshot(readings, base.Add(-10*time.Minute))
		if snap != nil {
			t.Fatalf("expected nil before first point, got %+v", snap)
		}
	})
}

func TestAlignerPreservesPointers(t *testing.T) {
	base, _ := time.Parse(time.RFC3339, "2023-01-01T10:00:00Z")
	readings := []domain.Reading{{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base, Value: 100}}
	aligner := domain.NewAligner(time.Minute)
	snap := aligner.FindSnapshot(readings, base)
	// 返回的是切片内元素的指针，应指向命中的元素本身
	if snap != &readings[0] {
		t.Fatalf("expected pointer into the matched element, got %p", snap)
	}
}
