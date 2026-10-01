package services_test

import (
	"context"
	"testing"
	"time"

	"github.com/RenJieWeia/prism-core/pkg/core/domain"
	"github.com/RenJieWeia/prism-core/pkg/core/services"
)

// TestBatchReferenceSourcePrevious 上一条数据参考: 同一设备上一条数据
func TestBatchReferenceSourcePrevious(t *testing.T) {
	base, _ := time.Parse(time.RFC3339, "2026-08-04T10:00:00Z")
	readings := []domain.Reading{
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base, Value: 100},
		{DeviceInfo: domain.DeviceInfo{ID: "D2"}, Timestamp: base.Add(1 * time.Minute), Value: 50},
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base.Add(2 * time.Minute), Value: 90},
	}
	src := services.NewBatchReferenceSource(readings)

	vals, err := src.Resolve(context.Background(), []domain.ReferenceRequest{{
		ID:       "prev",
		Source:   domain.ReferenceSourceCurrentBatch,
		DeviceID: "D1",
		Mode:     domain.ReferenceTimePrevious,
		Target:   base.Add(2 * time.Minute),
	}})
	if err != nil {
		t.Fatalf("resolve failed: %v", err)
	}

	ref := vals["prev"]
	if !ref.Found {
		t.Fatal("expected previous reference found")
	}
	if ref.Value != 100 {
		t.Errorf("expected previous value 100, got %v", ref.Value)
	}
	if !ref.Timestamp.Equal(base) {
		t.Errorf("expected previous timestamp %v, got %v", base, ref.Timestamp)
	}

	// 跨设备不得返回: D1 的上一条不应受 D2 数据影响 (首条 D1 无上一条)
	vals2, _ := src.Resolve(context.Background(), []domain.ReferenceRequest{{
		ID:       "prev",
		Source:   domain.ReferenceSourceCurrentBatch,
		DeviceID: "D1",
		Mode:     domain.ReferenceTimePrevious,
		Target:   base,
	}})
	if vals2["prev"].Found {
		t.Fatal("expected no previous reference for first reading")
	}
}

// TestBatchReferenceSourceRelativeThreeDaysAgo 三天前数据参考
// 目标时间必须以当前数据 Timestamp 为基准: current - 72h，不得使用 time.Now()。
// 使用远离真实 "now" 的夹具日期，确保实现若错误使用 time.Now() 会导致参考缺失。
func TestBatchReferenceSourceRelativeThreeDaysAgo(t *testing.T) {
	current, _ := time.Parse(time.RFC3339, "2024-08-04T10:00:00Z")
	target, _ := time.Parse(time.RFC3339, "2024-08-01T10:00:00Z")

	readings := []domain.Reading{
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: target, Value: 300},
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: current, Value: 330},
	}
	src := services.NewBatchReferenceSource(readings)

	vals, err := src.Resolve(context.Background(), []domain.ReferenceRequest{{
		ID:       "d3",
		Source:   domain.ReferenceSourceCurrentBatch,
		DeviceID: "D1",
		Mode:     domain.ReferenceTimeRelative,
		Target:   current.Add(-72 * time.Hour),
	}})
	if err != nil {
		t.Fatalf("resolve failed: %v", err)
	}

	ref := vals["d3"]
	if !ref.Found {
		t.Fatal("expected 3-days-ago reference found (target = current - 72h)")
	}
	if !ref.Timestamp.Equal(target) {
		t.Errorf("expected target timestamp %v, got %v", target, ref.Timestamp)
	}
	if ref.Value != 300 {
		t.Errorf("expected value 300, got %v", ref.Value)
	}
}

// TestReducePointsWindowAggregation 时间窗口聚合
// 输入 [10, 20, 30, 40] 验证 AVG/SUM/MIN/MAX/LATEST/DELTA。
func TestReducePointsWindowAggregation(t *testing.T) {
	base, _ := time.Parse(time.RFC3339, "2026-08-04T10:00:00Z")
	points := []domain.ReferencePoint{
		{Timestamp: base.Add(0 * time.Minute), Value: 10},
		{Timestamp: base.Add(1 * time.Minute), Value: 20},
		{Timestamp: base.Add(2 * time.Minute), Value: 30},
		{Timestamp: base.Add(3 * time.Minute), Value: 40},
	}

	cases := []struct {
		reducer domain.ReferenceReducer
		want    float64
	}{
		{domain.ReferenceReducerAvg, 25},
		{domain.ReferenceReducerSum, 100},
		{domain.ReferenceReducerMin, 10},
		{domain.ReferenceReducerMax, 40},
		{domain.ReferenceReducerLatest, 40},
		{domain.ReferenceReducerDelta, 30},
	}
	for _, c := range cases {
		got, ok := domain.ReducePoints(points, c.reducer)
		if !ok {
			t.Errorf("%s: expected ok=true", c.reducer)
			continue
		}
		if got != c.want {
			t.Errorf("%s: expected %v, got %v", c.reducer, c.want, got)
		}
	}
}

// TestBatchReferenceSourceWindow 时间窗口查询端到端验证
func TestBatchReferenceSourceWindow(t *testing.T) {
	base, _ := time.Parse(time.RFC3339, "2026-08-04T10:00:00Z")
	// 当前读数位于 10:00 (窗口上界，不包含), 窗口数据位于最近 24 小时内
	readings := []domain.Reading{
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base.Add(-24 * time.Hour), Value: 10},
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base.Add(-20 * time.Hour), Value: 20},
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base.Add(-10 * time.Hour), Value: 30},
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base.Add(-1 * time.Hour), Value: 40},
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base, Value: 999}, // 当前读数不应计入自身窗口
	}
	src := services.NewBatchReferenceSource(readings)

	reqs := []domain.ReferenceRequest{
		{ID: "avg", Source: domain.ReferenceSourceCurrentBatch, DeviceID: "D1",
			Mode: domain.ReferenceTimeWindow, Start: base.Add(-24 * time.Hour), End: base,
			Reducer: domain.ReferenceReducerAvg},
		{ID: "sum", Source: domain.ReferenceSourceCurrentBatch, DeviceID: "D1",
			Mode: domain.ReferenceTimeWindow, Start: base.Add(-24 * time.Hour), End: base,
			Reducer: domain.ReferenceReducerSum},
		{ID: "delta", Source: domain.ReferenceSourceCurrentBatch, DeviceID: "D1",
			Mode: domain.ReferenceTimeWindow, Start: base.Add(-24 * time.Hour), End: base,
			Reducer: domain.ReferenceReducerDelta},
	}
	vals, err := src.Resolve(context.Background(), reqs)
	if err != nil {
		t.Fatalf("resolve failed: %v", err)
	}

	if v := vals["avg"]; !v.Found || v.Value != 25 {
		t.Errorf("AVG: expected 25, got %+v", v)
	}
	if v := vals["sum"]; !v.Found || v.Value != 100 {
		t.Errorf("SUM: expected 100, got %+v", v)
	}
	if v := vals["delta"]; !v.Found || v.Value != 30 {
		t.Errorf("DELTA: expected 30, got %+v", v)
	}

	// 窗口内应只有 4 个点 (当前读数被排除)
	if len(vals["avg"].Points) != 4 {
		t.Errorf("expected 4 points in window, got %d", len(vals["avg"].Points))
	}
}
