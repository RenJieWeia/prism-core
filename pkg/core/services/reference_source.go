package services

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/RenJieWeia/prism-core/pkg/core/domain"
	"github.com/RenJieWeia/prism-core/pkg/core/ports"
)

// BatchReferenceSource 当前批次参考源
// 从当前输入批次 (内存) 中查询参考数据:
//   - PREVIOUS: 同一设备上一条数据
//   - RELATIVE: 指定时间点附近的数据
//   - WINDOW:   指定时间窗口内的数据 (支持聚合)
type BatchReferenceSource struct {
	readings []domain.Reading // 已按时间升序排列 (内部拷贝)
}

// NewBatchReferenceSource 基于当前批次数据创建参考源
// 内部会拷贝并排序，不修改调用方数据。
func NewBatchReferenceSource(readings []domain.Reading) *BatchReferenceSource {
	sorted := make([]domain.Reading, len(readings))
	copy(sorted, readings)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Timestamp.Before(sorted[j].Timestamp)
	})
	return &BatchReferenceSource{readings: sorted}
}

// Resolve 实现 ports.ReferenceSource 接口
func (s *BatchReferenceSource) Resolve(ctx context.Context, requests []domain.ReferenceRequest) (map[string]domain.ReferenceValue, error) {
	out := make(map[string]domain.ReferenceValue, len(requests))
	for _, req := range requests {
		out[req.ID] = s.resolveOne(req)
	}
	return out, nil
}

func (s *BatchReferenceSource) resolveOne(req domain.ReferenceRequest) domain.ReferenceValue {
	switch req.Mode {
	case domain.ReferenceTimePrevious:
		return s.resolvePrevious(req)
	case domain.ReferenceTimeRelative:
		return s.resolveRelative(req)
	case domain.ReferenceTimeWindow:
		return s.resolveWindow(req)
	default:
		return domain.ReferenceValue{}
	}
}

// resolvePrevious 同一设备上一条数据 (时间严格早于目标时间点)
func (s *BatchReferenceSource) resolvePrevious(req domain.ReferenceRequest) domain.ReferenceValue {
	var best *domain.Reading
	for i := range s.readings {
		r := &s.readings[i]
		if r.DeviceInfo.ID != req.DeviceID {
			continue
		}
		if !r.Timestamp.Before(req.Target) {
			continue
		}
		if best == nil || r.Timestamp.After(best.Timestamp) {
			best = r
		}
	}
	if best == nil {
		return domain.ReferenceValue{}
	}
	return domain.ReferenceValue{Found: true, Timestamp: best.Timestamp, Value: best.Value}
}

// resolveRelative 同一设备指定时间点附近的数据
// 在 [Target-Tolerance, Target+Tolerance] 内取时间差最小的一条;
// Tolerance <= 0 时要求目标时间点上存在精确数据。
func (s *BatchReferenceSource) resolveRelative(req domain.ReferenceRequest) domain.ReferenceValue {
	tol := req.Tolerance
	var best *domain.Reading
	bestDiff := time.Duration(math.MaxInt64)
	for i := range s.readings {
		r := &s.readings[i]
		if r.DeviceInfo.ID != req.DeviceID {
			continue
		}
		diff := durationAbs(r.Timestamp.Sub(req.Target))
		if tol > 0 && diff > tol {
			continue
		}
		if tol <= 0 && diff != 0 {
			continue
		}
		if diff < bestDiff {
			bestDiff = diff
			best = r
		}
	}
	if best == nil {
		return domain.ReferenceValue{}
	}
	return domain.ReferenceValue{Found: true, Timestamp: best.Timestamp, Value: best.Value}
}

// resolveWindow 同一设备 [Start, End) 时间窗口内的数据，并按 Reducer 聚合
func (s *BatchReferenceSource) resolveWindow(req domain.ReferenceRequest) domain.ReferenceValue {
	var points []domain.ReferencePoint
	for i := range s.readings {
		r := &s.readings[i]
		if r.DeviceInfo.ID != req.DeviceID {
			continue
		}
		if r.Timestamp.Before(req.Start) || !r.Timestamp.Before(req.End) {
			continue
		}
		points = append(points, domain.ReferencePoint{Timestamp: r.Timestamp, Value: r.Value})
	}
	if len(points) == 0 {
		return domain.ReferenceValue{}
	}
	val, ok := domain.ReducePoints(points, req.Reducer)
	if !ok {
		return domain.ReferenceValue{}
	}
	return domain.ReferenceValue{
		Found:     true,
		Timestamp: points[len(points)-1].Timestamp,
		Value:     val,
		Points:    points,
	}
}

// ReferenceResolver 参考解析器
// 按 ReferenceRequest.Source 将请求路由到批次源或历史仓储源，
// 并在本次处理范围内缓存已解析的结果，避免相同参考请求被重复查询。
type ReferenceResolver struct {
	batch ports.ReferenceSource // 当前批次参考源
	repo  ports.ReferenceSource // 历史仓储参考源 (可为 nil)
	cache map[string]domain.ReferenceValue
}

// NewReferenceResolver 创建参考解析器
// repoRefs 为历史仓储参考源，可为 nil。
func NewReferenceResolver(readings []domain.Reading, repoRefs ports.ReferenceSource) *ReferenceResolver {
	return &ReferenceResolver{
		batch: NewBatchReferenceSource(readings),
		repo:  repoRefs,
		cache: make(map[string]domain.ReferenceValue),
	}
}

// Resolve 批量解析参考请求并缓存结果
// 相同查询内容 (缓存键一致) 的请求只执行一次，无论是否跨规则/跨读数。
func (r *ReferenceResolver) Resolve(ctx context.Context, requests []domain.ReferenceRequest) (map[string]domain.ReferenceValue, error) {
	out := make(map[string]domain.ReferenceValue, len(requests))

	// 过滤已缓存请求，并对本次调用内重复的查询去重
	var missing []domain.ReferenceRequest
	queued := make(map[string]bool)
	for _, req := range requests {
		key := cacheKey(req)
		if v, ok := r.cache[key]; ok {
			out[req.ID] = v
			continue
		}
		if !queued[key] {
			queued[key] = true
			missing = append(missing, req)
		}
	}

	if len(missing) > 0 {
		// 按来源分组，批量查询
		grouped := make(map[domain.ReferenceSourceKind][]domain.ReferenceRequest)
		for _, req := range missing {
			grouped[req.Source] = append(grouped[req.Source], req)
		}

		for kind, grp := range grouped {
			src, err := r.sourceFor(kind)
			if err != nil {
				return nil, err
			}
			vals, err := src.Resolve(ctx, grp)
			if err != nil {
				return nil, err
			}
			for _, req := range grp {
				r.cache[cacheKey(req)] = vals[req.ID]
			}
		}
	}

	// 回填本次调用内被去重的请求 (与已缓存请求具有相同查询内容)
	for _, req := range requests {
		if _, ok := out[req.ID]; ok {
			continue
		}
		if v, ok := r.cache[cacheKey(req)]; ok {
			out[req.ID] = v
		}
	}
	return out, nil
}

func (r *ReferenceResolver) sourceFor(kind domain.ReferenceSourceKind) (ports.ReferenceSource, error) {
	switch kind {
	case domain.ReferenceSourceCurrentBatch:
		return r.batch, nil
	case domain.ReferenceSourceStandardRepo:
		if r.repo == nil {
			return nil, fmt.Errorf("STANDARD_REPO reference source not configured")
		}
		return r.repo, nil
	default:
		return nil, fmt.Errorf("unknown reference source kind: %s", kind)
	}
}

// buildReferenceRequest 将 ReferenceSpec + 当前读数解析为具体的参考查询请求
// RELATIVE 目标时间以当前读数的 Timestamp 为基准 (target = current.Timestamp - Offset，
// Offset 为非负且向历史方向偏移)，不使用 time.Now()。
func buildReferenceRequest(spec domain.ReferenceSpec, current domain.Reading) (domain.ReferenceRequest, bool) {
	deviceID := current.DeviceInfo.ID
	if spec.Binding == domain.ReferenceBindingExplicit {
		deviceID = spec.DeviceID
	}
	if deviceID == "" {
		return domain.ReferenceRequest{}, false
	}

	req := domain.ReferenceRequest{
		ID:        spec.ID,
		Source:    spec.Source,
		DeviceID:  deviceID,
		Mode:      spec.Time.Mode,
		Reducer:   spec.Reducer,
		Tolerance: spec.Time.Tolerance,
	}

	switch spec.Time.Mode {
	case domain.ReferenceTimePrevious:
		req.Target = current.Timestamp
	case domain.ReferenceTimeRelative:
		req.Target = current.Timestamp.Add(-spec.Time.Offset)
	case domain.ReferenceTimeWindow:
		req.End = current.Timestamp
		req.Start = current.Timestamp.Add(-spec.Time.Offset)
	default:
		return domain.ReferenceRequest{}, false
	}
	return req, true
}

// cacheKey 生成参考请求的缓存键 (按查询内容去重，忽略请求 ID)
func cacheKey(req domain.ReferenceRequest) string {
	return fmt.Sprintf("%s|%s|%s|%d|%d|%d|%d|%s",
		req.Source, req.DeviceID, req.Mode,
		req.Target.UnixNano(), req.Start.UnixNano(), req.End.UnixNano(),
		req.Tolerance, req.Reducer)
}

// durationAbs 返回 Duration 的绝对值
func durationAbs(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}
