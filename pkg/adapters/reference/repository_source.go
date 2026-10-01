package reference

import (
	"context"
	"sort"
	"time"

	"github.com/RenJieWeia/prism-core/pkg/core/domain"
	"github.com/RenJieWeia/prism-core/pkg/core/ports"
)

// RepositoryReferenceSource 标准数据仓储参考源
// 基于现有 StandardReadingRepository 查询历史标准数据作为参考对象。
// Resolve 按设备合并时间范围批量查询，每设备在一次 Resolve 中尽量只调用一次 FindRange，
// 再在内存中按各请求自身的时间范围解析结果。
type RepositoryReferenceSource struct {
	repo ports.StandardReadingRepository
	// tolerance 仅保留以维持 NewRepositoryReferenceSource 构造签名兼容。
	// RELATIVE 容差一律取自 ReferenceRequest.Tolerance (见零容差语义)，
	// 本字段不再参与 RELATIVE 查询。
	tolerance time.Duration
}

// NewRepositoryReferenceSource 创建仓储参考源
// tolerance 参数保留仅为 API 兼容，RELATIVE 容差由请求自身决定。
func NewRepositoryReferenceSource(repo ports.StandardReadingRepository, tolerance time.Duration) *RepositoryReferenceSource {
	return &RepositoryReferenceSource{repo: repo, tolerance: tolerance}
}

// Resolve 实现 ports.ReferenceSource 接口
// 按 DeviceID 分组批量查询，避免每个请求独立访问一次仓储。
func (s *RepositoryReferenceSource) Resolve(ctx context.Context, requests []domain.ReferenceRequest) (map[string]domain.ReferenceValue, error) {
	out := make(map[string]domain.ReferenceValue, len(requests))

	byDevice := make(map[string][]domain.ReferenceRequest)
	for _, req := range requests {
		byDevice[req.DeviceID] = append(byDevice[req.DeviceID], req)
	}

	for deviceID, grp := range byDevice {
		if err := s.resolveDevice(ctx, deviceID, grp, out); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// resolveDevice 解析同一设备的一组请求:
//   - 零容差 RELATIVE 走 FindExact (精确匹配)
//   - 其余请求 (PREVIOUS / RELATIVE 带容差 / WINDOW) 合并时间范围，一次 FindRange 后内存解析
func (s *RepositoryReferenceSource) resolveDevice(ctx context.Context, deviceID string, grp []domain.ReferenceRequest, out map[string]domain.ReferenceValue) error {
	var rangeReqs []domain.ReferenceRequest

	for _, req := range grp {
		if req.Mode == domain.ReferenceTimeRelative && req.Tolerance <= 0 {
			// 零容差: 只允许精确匹配
			exact, err := s.repo.FindExact(ctx, deviceID, req.Target)
			if err != nil {
				return err
			}
			if exact == nil {
				out[req.ID] = domain.ReferenceValue{}
				continue
			}
			out[req.ID] = domain.ReferenceValue{
				Found:     true,
				Timestamp: exact.Timestamp,
				Value:     exact.ValueDisplay,
			}
			continue
		}
		rangeReqs = append(rangeReqs, req)
	}

	if len(rangeReqs) == 0 {
		return nil
	}

	start, end := mergedRange(rangeReqs)
	rows, err := s.repo.FindRange(ctx, deviceID, start, end)
	if err != nil {
		return err
	}

	for _, req := range rangeReqs {
		out[req.ID] = resolveFromRows(req, rows)
	}
	return nil
}

// mergedRange 计算一组范围请求的合并时间边界 [minStart, maxEnd]
func mergedRange(reqs []domain.ReferenceRequest) (time.Time, time.Time) {
	var start, end time.Time
	for _, req := range reqs {
		var s, e time.Time
		switch req.Mode {
		case domain.ReferenceTimePrevious:
			s = time.Unix(0, 0)
			e = req.Target
		case domain.ReferenceTimeRelative: // 仅 Tolerance > 0 的请求到达此处
			s = req.Target.Add(-req.Tolerance)
			e = req.Target.Add(req.Tolerance)
		case domain.ReferenceTimeWindow:
			s = req.Start
			e = req.End
		}
		if start.IsZero() || s.Before(start) {
			start = s
		}
		if end.IsZero() || e.After(end) {
			end = e
		}
	}
	return start, end
}

// resolveFromRows 在内存中按请求自身的时间范围解析结果
// rows 可能来自合并后的范围查询，包含该请求范围之外的数据，必须再次过滤。
func resolveFromRows(req domain.ReferenceRequest, rows []domain.StandardReading) domain.ReferenceValue {
	switch req.Mode {
	case domain.ReferenceTimePrevious:
		return previousFromRows(rows, req.Target)
	case domain.ReferenceTimeRelative:
		return relativeFromRows(rows, req.Target, req.Tolerance)
	case domain.ReferenceTimeWindow:
		return windowFromRows(rows, req.Start, req.End, req.Reducer)
	default:
		return domain.ReferenceValue{}
	}
}

// previousFromRows 严格取时间早于 target 的最近一条历史数据
//   - 只选择 Timestamp < Target 的数据 (不含 == Target)
//   - 不依赖仓储返回顺序: 先过滤再按时间排序
func previousFromRows(rows []domain.StandardReading, target time.Time) domain.ReferenceValue {
	var valid []domain.StandardReading
	for _, r := range rows {
		if r.Timestamp.Before(target) {
			valid = append(valid, r)
		}
	}
	if len(valid) == 0 {
		return domain.ReferenceValue{}
	}
	sort.Slice(valid, func(i, j int) bool {
		return valid[i].Timestamp.Before(valid[j].Timestamp)
	})
	last := valid[len(valid)-1]
	return domain.ReferenceValue{
		Found:     true,
		Timestamp: last.Timestamp,
		Value:     last.ValueDisplay,
	}
}

// relativeFromRows 取距 target 最近的记录 (容差 > 0)
func relativeFromRows(rows []domain.StandardReading, target time.Time, tolerance time.Duration) domain.ReferenceValue {
	best := -1
	bestDiff := time.Duration(1<<63 - 1)
	for i := range rows {
		diff := durationAbs(rows[i].Timestamp.Sub(target))
		if diff > tolerance {
			continue
		}
		if best < 0 || diff < bestDiff {
			bestDiff = diff
			best = i
		}
	}
	if best < 0 {
		return domain.ReferenceValue{}
	}
	return domain.ReferenceValue{
		Found:     true,
		Timestamp: rows[best].Timestamp,
		Value:     rows[best].ValueDisplay,
	}
}

// windowFromRows 严格使用窗口边界 [Start, End): 包含 Start，不包含 End
func windowFromRows(rows []domain.StandardReading, start, end time.Time, reducer domain.ReferenceReducer) domain.ReferenceValue {
	var points []domain.ReferencePoint
	for _, r := range rows {
		if r.Timestamp.Before(start) || !r.Timestamp.Before(end) {
			continue
		}
		points = append(points, domain.ReferencePoint{Timestamp: r.Timestamp, Value: r.ValueDisplay})
	}
	if len(points) == 0 {
		return domain.ReferenceValue{}
	}
	sort.Slice(points, func(i, j int) bool {
		return points[i].Timestamp.Before(points[j].Timestamp)
	})
	val, ok := domain.ReducePoints(points, reducer)
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

func durationAbs(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}
