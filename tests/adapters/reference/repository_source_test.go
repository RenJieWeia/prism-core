package reference_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/RenJieWeia/prism-core/pkg/adapters/reference"
	"github.com/RenJieWeia/prism-core/pkg/core/domain"
	"github.com/RenJieWeia/prism-core/pkg/core/ports"
)

// fakeRepo 可控测试仓储:
//   - 支持按行查询，FindRange 双端包含且不保证返回顺序 (模拟真实仓储)
//   - 统计 FindRange / FindExact 调用次数
type fakeRepo struct {
	mu             sync.Mutex
	rows           []domain.StandardReading
	findRangeCalls int
	findExactCalls int
}

func newFakeRepo(rows ...domain.StandardReading) *fakeRepo {
	return &fakeRepo{rows: rows}
}

func (f *fakeRepo) Save(_ context.Context, _ domain.StandardReading, _ ports.UpsertStrategy) error {
	return nil
}
func (f *fakeRepo) SaveBatch(_ context.Context, _ []domain.StandardReading, _ ports.UpsertStrategy) error {
	return nil
}
func (f *fakeRepo) FindExact(_ context.Context, deviceID string, ts time.Time) (*domain.StandardReading, error) {
	f.mu.Lock()
	f.findExactCalls++
	f.mu.Unlock()
	for _, r := range f.rows {
		if r.DeviceID == deviceID && r.Timestamp.Equal(ts) {
			cp := r
			return &cp, nil
		}
	}
	return nil, nil
}
func (f *fakeRepo) FindRange(_ context.Context, deviceID string, start, end time.Time) ([]domain.StandardReading, error) {
	f.mu.Lock()
	f.findRangeCalls++
	f.mu.Unlock()

	// 双端包含 [start, end]，且返回顺序与插入顺序一致 (不保证有序)
	var out []domain.StandardReading
	for _, r := range f.rows {
		if r.DeviceID == deviceID && !r.Timestamp.Before(start) && !r.Timestamp.After(end) {
			out = append(out, r)
		}
	}
	return out, nil
}

func (f *fakeRepo) counts() (rangeCalls, exactCalls int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.findRangeCalls, f.findExactCalls
}

func stdRow(deviceID string, ts time.Time, v float64) domain.StandardReading {
	return domain.StandardReading{DeviceID: deviceID, Timestamp: ts, ValueDisplay: v}
}

// TestRepositoryPreviousExcludesCurrentTimestamp PREVIOUS 不得返回 Timestamp == Target 的数据
func TestRepositoryPreviousExcludesCurrentTimestamp(t *testing.T) {
	target, _ := time.Parse(time.RFC3339, "2026-08-04T10:00:00Z")
	earlier := target.Add(-30 * time.Minute)

	repo := newFakeRepo(
		stdRow("D1", target, 999), // 恰好等于 Target，必须被排除
		stdRow("D1", earlier, 100),
	)
	src := reference.NewRepositoryReferenceSource(repo, time.Minute)

	vals, err := src.Resolve(context.Background(), []domain.ReferenceRequest{{
		ID: "prev", Source: domain.ReferenceSourceStandardRepo, DeviceID: "D1",
		Mode: domain.ReferenceTimePrevious, Target: target,
	}})
	if err != nil {
		t.Fatalf("resolve failed: %v", err)
	}
	ref := vals["prev"]
	if !ref.Found {
		t.Fatal("expected previous found")
	}
	if ref.Value != 100 {
		t.Errorf("expected previous value 100, got %v", ref.Value)
	}
	if !ref.Timestamp.Equal(earlier) {
		t.Errorf("expected timestamp %v, got %v", earlier, ref.Timestamp)
	}
}

// TestRepositoryPreviousHandlesUnsortedRows PREVIOUS 不依赖仓储返回顺序，按时间取最近历史
func TestRepositoryPreviousHandlesUnsortedRows(t *testing.T) {
	target, _ := time.Parse(time.RFC3339, "2026-08-04T11:00:00Z")
	t10_30, _ := time.Parse(time.RFC3339, "2026-08-04T10:30:00Z")
	t10_15, _ := time.Parse(time.RFC3339, "2026-08-04T10:15:00Z")
	t10_00, _ := time.Parse(time.RFC3339, "2026-08-04T10:00:00Z")

	// 仓储返回乱序
	repo := newFakeRepo(
		stdRow("D1", t10_30, 110),
		stdRow("D1", t10_00, 100),
		stdRow("D1", t10_15, 105),
	)
	src := reference.NewRepositoryReferenceSource(repo, time.Minute)

	vals, err := src.Resolve(context.Background(), []domain.ReferenceRequest{{
		ID: "prev", Source: domain.ReferenceSourceStandardRepo, DeviceID: "D1",
		Mode: domain.ReferenceTimePrevious, Target: target,
	}})
	if err != nil {
		t.Fatalf("resolve failed: %v", err)
	}
	ref := vals["prev"]
	if !ref.Found || ref.Value != 110 {
		t.Fatalf("expected latest previous value 110, got %+v", ref)
	}
	if !ref.Timestamp.Equal(t10_30) {
		t.Errorf("expected timestamp %v, got %v", t10_30, ref.Timestamp)
	}
}

// TestRepositoryWindowExcludesEndTimestamp WINDOW 必须严格使用 [Start, End)
func TestRepositoryWindowExcludesEndTimestamp(t *testing.T) {
	base, _ := time.Parse(time.RFC3339, "2026-08-04T10:00:00Z")
	start := base.Add(-24 * time.Hour)
	end := base

	// FindRange 返回了 End 时刻的数据 (仓储双端包含)，适配器必须再次过滤
	repo := newFakeRepo(
		stdRow("D1", start, 10),
		stdRow("D1", end, 999), // == End，必须排除
	)
	src := reference.NewRepositoryReferenceSource(repo, time.Minute)

	vals, err := src.Resolve(context.Background(), []domain.ReferenceRequest{{
		ID: "win", Source: domain.ReferenceSourceStandardRepo, DeviceID: "D1",
		Mode: domain.ReferenceTimeWindow, Start: start, End: end,
		Reducer: domain.ReferenceReducerSum,
	}})
	if err != nil {
		t.Fatalf("resolve failed: %v", err)
	}
	ref := vals["win"]
	if !ref.Found || ref.Value != 10 {
		t.Fatalf("expected window sum 10 (excluding End), got %+v", ref)
	}
	if len(ref.Points) != 1 {
		t.Errorf("expected 1 point in window, got %d", len(ref.Points))
	}
}

// TestRepositoryRelativeZeroToleranceRequiresExactMatch 零容差: 调用 FindExact，只允许精确匹配
func TestRepositoryRelativeZeroToleranceRequiresExactMatch(t *testing.T) {
	base, _ := time.Parse(time.RFC3339, "2026-08-04T10:00:00Z")

	// 附近有数据但目标点无精确匹配
	repo := newFakeRepo(
		stdRow("D1", base.Add(-30*time.Second), 400),
		stdRow("D1", base.Add(30*time.Second), 500),
	)
	src := reference.NewRepositoryReferenceSource(repo, time.Minute)

	vals, err := src.Resolve(context.Background(), []domain.ReferenceRequest{{
		ID: "exact", Source: domain.ReferenceSourceStandardRepo, DeviceID: "D1",
		Mode: domain.ReferenceTimeRelative, Target: base, Tolerance: 0,
	}})
	if err != nil {
		t.Fatalf("resolve failed: %v", err)
	}
	if vals["exact"].Found {
		t.Fatalf("expected no exact match at zero tolerance, got %+v", vals["exact"])
	}
	r, e := repo.counts()
	if r != 0 || e != 1 {
		t.Errorf("expected FindExact only (range=%d exact=%d), got range=%d exact=%d", 0, 1, r, e)
	}

	// 目标点存在精确匹配 -> 命中
	exactRepo := newFakeRepo(stdRow("D1", base, 300))
	src2 := reference.NewRepositoryReferenceSource(exactRepo, time.Minute)
	vals2, err := src2.Resolve(context.Background(), []domain.ReferenceRequest{{
		ID: "exact", Source: domain.ReferenceSourceStandardRepo, DeviceID: "D1",
		Mode: domain.ReferenceTimeRelative, Target: base, Tolerance: 0,
	}})
	if err != nil {
		t.Fatalf("resolve failed: %v", err)
	}
	if !vals2["exact"].Found || vals2["exact"].Value != 300 {
		t.Fatalf("expected exact match 300, got %+v", vals2["exact"])
	}
}

// TestRepositoryReferenceSourceBatchesRequestsByDevice 按设备合并范围，每设备一次 Resolve 只调一次 FindRange
func TestRepositoryReferenceSourceBatchesRequestsByDevice(t *testing.T) {
	base, _ := time.Parse(time.RFC3339, "2026-08-04T10:00:00Z")

	// D1: 多条历史点
	rows := []domain.StandardReading{
		stdRow("D1", base.Add(-96*time.Hour), 10),
		stdRow("D1", base.Add(-72*time.Hour), 20),
		stdRow("D1", base.Add(-48*time.Hour), 30),
		stdRow("D1", base.Add(-24*time.Hour), 40),
		stdRow("D1", base.Add(-12*time.Hour), 50),
		stdRow("D2", base.Add(-48*time.Hour), 100),
		stdRow("D2", base.Add(-24*time.Hour), 200),
	}
	repo := newFakeRepo(rows...)
	src := reference.NewRepositoryReferenceSource(repo, time.Minute)

	// D1: 2 条 RELATIVE(带容差) + 2 条 WINDOW; D2: 1 条 WINDOW
	requests := []domain.ReferenceRequest{
		{ID: "r1", Source: domain.ReferenceSourceStandardRepo, DeviceID: "D1",
			Mode: domain.ReferenceTimeRelative, Target: base.Add(-72 * time.Hour), Tolerance: time.Hour},
		{ID: "r2", Source: domain.ReferenceSourceStandardRepo, DeviceID: "D1",
			Mode: domain.ReferenceTimeRelative, Target: base.Add(-24 * time.Hour), Tolerance: time.Hour},
		{ID: "w1", Source: domain.ReferenceSourceStandardRepo, DeviceID: "D1",
			Mode: domain.ReferenceTimeWindow, Start: base.Add(-96 * time.Hour), End: base.Add(-24 * time.Hour),
			Reducer: domain.ReferenceReducerSum},
		{ID: "w2", Source: domain.ReferenceSourceStandardRepo, DeviceID: "D1",
			Mode: domain.ReferenceTimeWindow, Start: base.Add(-24 * time.Hour), End: base,
			Reducer: domain.ReferenceReducerMax},
		{ID: "d2w", Source: domain.ReferenceSourceStandardRepo, DeviceID: "D2",
			Mode: domain.ReferenceTimeWindow, Start: base.Add(-48 * time.Hour), End: base,
			Reducer: domain.ReferenceReducerSum},
	}

	vals, err := src.Resolve(context.Background(), requests)
	if err != nil {
		t.Fatalf("resolve failed: %v", err)
	}

	// D1 合并一次 FindRange + D2 一次 FindRange = 共 2 次，而非 5 次
	r, e := repo.counts()
	if r != 2 {
		t.Fatalf("expected 2 FindRange calls (one per device), got %d (exact=%d)", r, e)
	}
	if e != 0 {
		t.Errorf("expected no FindExact calls, got %d", e)
	}

	// 校验各请求结果
	if v := vals["r1"]; !v.Found || v.Value != 20 {
		t.Errorf("r1: expected 20, got %+v", v)
	}
	if v := vals["r2"]; !v.Found || v.Value != 40 {
		t.Errorf("r2: expected 40, got %+v", v)
	}
	if v := vals["w1"]; !v.Found || v.Value != 10+20+30 { // [start, -24h): 10+20+30
		t.Errorf("w1: expected sum 60, got %+v", v)
	}
	if v := vals["w2"]; !v.Found || v.Value != 50 { // [-24h, end): 40, 50 -> max 50
		t.Errorf("w2: expected max 50, got %+v", v)
	}
	if v := vals["d2w"]; !v.Found || v.Value != 300 { // [-48h, end): 100+200
		t.Errorf("d2w: expected sum 300, got %+v", v)
	}
}

var _ ports.StandardReadingRepository = (*fakeRepo)(nil)
