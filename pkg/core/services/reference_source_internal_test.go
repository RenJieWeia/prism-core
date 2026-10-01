package services

import (
	"testing"
	"time"

	"github.com/RenJieWeia/prism-core/pkg/core/domain"
)

// TestBuildReferenceRequestRelativeTarget 验证相对时间点参考的请求构建
// 目标时间必须以当前读数的 Timestamp 为基准 (current.Timestamp.Add(-offset))，不得使用 time.Now()。
func TestBuildReferenceRequestRelativeTarget(t *testing.T) {
	current, _ := time.Parse(time.RFC3339, "2026-08-04T10:00:00Z")
	want, _ := time.Parse(time.RFC3339, "2026-08-01T10:00:00Z")

	spec := domain.ReferenceSpec{
		ID:      "d3",
		Source:  domain.ReferenceSourceCurrentBatch,
		Binding: domain.ReferenceBindingSameDevice,
		Time: domain.ReferenceTimeSelector{
			Mode:   domain.ReferenceTimeRelative,
			Offset: 72 * time.Hour,
		},
	}
	req, ok := buildReferenceRequest(spec, domain.Reading{
		DeviceInfo: domain.DeviceInfo{ID: "D1"},
		Timestamp:  current,
	})
	if !ok {
		t.Fatal("expected request built")
	}
	if !req.Target.Equal(want) {
		t.Errorf("expected target %v, got %v", want, req.Target)
	}
	if req.Mode != domain.ReferenceTimeRelative {
		t.Errorf("expected mode RELATIVE, got %s", req.Mode)
	}
	if req.DeviceID != "D1" {
		t.Errorf("expected device D1, got %s", req.DeviceID)
	}
}

// TestBuildReferenceRequestModes 验证上一条与时间窗口请求的构建
func TestBuildReferenceRequestModes(t *testing.T) {
	current, _ := time.Parse(time.RFC3339, "2026-08-04T10:00:00Z")

	prevSpec := domain.ReferenceSpec{
		ID: "prev", Source: domain.ReferenceSourceCurrentBatch,
		Binding: domain.ReferenceBindingSameDevice,
		Time:    domain.ReferenceTimeSelector{Mode: domain.ReferenceTimePrevious},
	}
	prevReq, ok := buildReferenceRequest(prevSpec, domain.Reading{
		DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: current,
	})
	if !ok || prevReq.Mode != domain.ReferenceTimePrevious || !prevReq.Target.Equal(current) {
		t.Errorf("unexpected PREVIOUS request: %+v (ok=%v)", prevReq, ok)
	}

	winSpec := domain.ReferenceSpec{
		ID: "win", Source: domain.ReferenceSourceStandardRepo,
		Binding: domain.ReferenceBindingExplicit, DeviceID: "D9",
		Time: domain.ReferenceTimeSelector{
			Mode:   domain.ReferenceTimeWindow,
			Offset: 24 * time.Hour,
		},
		Reducer: domain.ReferenceReducerMax,
	}
	winReq, ok := buildReferenceRequest(winSpec, domain.Reading{
		DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: current,
	})
	if !ok {
		t.Fatal("expected window request built")
	}
	if winReq.DeviceID != "D9" {
		t.Errorf("expected explicit device D9, got %s", winReq.DeviceID)
	}
	if winReq.Source != domain.ReferenceSourceStandardRepo {
		t.Errorf("expected source STANDARD_REPO, got %s", winReq.Source)
	}
	if !winReq.End.Equal(current) {
		t.Errorf("expected window end %v, got %v", current, winReq.End)
	}
	wantStart := current.Add(-24 * time.Hour)
	if !winReq.Start.Equal(wantStart) {
		t.Errorf("expected window start %v, got %v", wantStart, winReq.Start)
	}
	if winReq.Reducer != domain.ReferenceReducerMax {
		t.Errorf("expected reducer MAX, got %s", winReq.Reducer)
	}
}

// TestReferenceResolverCachesAndDedups 验证解析器在本次处理范围内去重
func TestReferenceResolverCachesAndDedups(t *testing.T) {
	base, _ := time.Parse(time.RFC3339, "2026-08-04T10:00:00Z")
	readings := []domain.Reading{
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base.Add(-24 * time.Hour), Value: 10},
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base, Value: 999},
	}
	resolver := NewReferenceResolver(readings, nil)

	// 两个不同 ID 但查询内容相同的请求
	reqs := []domain.ReferenceRequest{
		{ID: "a", Source: domain.ReferenceSourceCurrentBatch, DeviceID: "D1",
			Mode: domain.ReferenceTimeWindow, Start: base.Add(-24 * time.Hour), End: base,
			Reducer: domain.ReferenceReducerAvg},
		{ID: "b", Source: domain.ReferenceSourceCurrentBatch, DeviceID: "D1",
			Mode: domain.ReferenceTimeWindow, Start: base.Add(-24 * time.Hour), End: base,
			Reducer: domain.ReferenceReducerAvg},
	}
	vals, err := resolver.Resolve(t.Context(), reqs)
	if err != nil {
		t.Fatalf("resolve failed: %v", err)
	}
	if !vals["a"].Found || vals["a"].Value != 10 {
		t.Errorf("unexpected value for a: %+v", vals["a"])
	}
	if !vals["b"].Found || vals["b"].Value != 10 {
		t.Errorf("unexpected value for b: %+v", vals["b"])
	}
	if len(resolver.cache) != 1 {
		t.Errorf("expected 1 cached entry after dedup, got %d", len(resolver.cache))
	}
}

// TestReferenceResolverStandardRepoNotConfigured 验证未配置仓储源时的错误返回
func TestReferenceResolverStandardRepoNotConfigured(t *testing.T) {
	base, _ := time.Parse(time.RFC3339, "2026-08-04T10:00:00Z")
	resolver := NewReferenceResolver(nil, nil)
	_, err := resolver.Resolve(t.Context(), []domain.ReferenceRequest{{
		ID: "r", Source: domain.ReferenceSourceStandardRepo, DeviceID: "D1",
		Mode: domain.ReferenceTimeRelative, Target: base,
	}})
	if err == nil {
		t.Fatal("expected error when STANDARD_REPO source not configured")
	}
}
