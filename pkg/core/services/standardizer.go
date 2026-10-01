package services

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/RenJieWeia/prism-core/pkg/core/domain"
	"github.com/RenJieWeia/prism-core/pkg/core/ports"
)

// CoreStandardizer 核心数据标准化服务
// 同时实现旧接口 ports.EnergyDataStandardizer 与新接口 ports.EnergyDataProcessor。
// Core 只负责: 数据清洗 / 规则执行 / 参考数据解析 / 时间对齐 / 数据标准化 / 生成 ProcessingResult。
// 持久化、隔离区保存与 Sink 投递由 Application Pipeline (pkg/application/pipeline) 负责。
type CoreStandardizer struct {
	sanitizer        *ChainSanitizer
	aligner          ports.Aligner
	standardInterval time.Duration
	precisionFactor  int                             // 精度因子 (默认 10000，即 4 位小数)
	concurrencyLimit int                             // 并发限制
	repo             ports.StandardReadingRepository // 可选查询仓储 (仅供 GetStandardReading)
	ruleRepo         ports.CleaningRuleRepository    // 可选规则持久层
	ruleFactory      ports.CleaningRuleFactory       // 可选规则工厂 (动态清洗依赖)

	// 参考对象 (本轮新增)
	cleaningRules   []ports.CleaningRule          // 静态配置的旧规则 (WithCleaningRules)
	referenceRules  []ports.ReferenceCleaningRule // 静态配置的参考规则 (WithReferenceRules)
	referenceSource ports.ReferenceSource         // 历史仓储参考源 (可选)
}

// StandardizerOption 定义配置选项函数 (Functional Option Pattern)
type StandardizerOption func(*CoreStandardizer)

// Deprecated: 隔离数据保存属于 Application Pipeline。
// Core 不再自动保存隔离数据 (由 ProcessingPipeline + QuarantineSink 负责)。
// 本选项为无操作，仅保留以避免调用方立即编译失败。
func WithQuarantineRepository(repo ports.QuarantineRepository) StandardizerOption {
	return func(s *CoreStandardizer) {}
}

// WithRuleRepository 设置规则持久层依赖
func WithRuleRepository(repo ports.CleaningRuleRepository) StandardizerOption {
	return func(s *CoreStandardizer) {
		s.ruleRepo = repo
	}
}

// WithAlignment 设置时间对齐参数 (默认 15m, 5m)
func WithAlignment(interval, tolerance time.Duration) StandardizerOption {
	return func(s *CoreStandardizer) {
		s.standardInterval = interval
		s.aligner = domain.NewAligner(tolerance)
	}
}

// WithPrecision 设置精度因子 (即 ValueScaled 的缩放倍数，默认 10000 = 4 位小数)
// 注意: 精度转换使用 int64 截断而非四舍五入 (测试固定了该行为)
func WithPrecision(factor int) StandardizerOption {
	return func(s *CoreStandardizer) {
		if factor > 0 {
			s.precisionFactor = factor
		}
	}
}

// WithRuleFactory 设置清洗规则工厂依赖 (启用动态清洗时的必要注入)
func WithRuleFactory(f ports.CleaningRuleFactory) StandardizerOption {
	return func(s *CoreStandardizer) {
		s.ruleFactory = f
	}
}

// Deprecated: 标准数据持久化属于 Application Pipeline。
// 建议使用 ProcessingPipeline + RepositorySink 写入 Accepted。
// Core 的 Process 不再自动保存数据；配置的仓储仍用于 GetStandardReading 查询。
func WithRepository(repo ports.StandardReadingRepository) StandardizerOption {
	return func(s *CoreStandardizer) {
		s.repo = repo
	}
}

// WithCleaningRules 设置清洗规则 (旧规则接口)
func WithCleaningRules(rules ...ports.CleaningRule) StandardizerOption {
	return func(s *CoreStandardizer) {
		s.cleaningRules = append(s.cleaningRules, rules...)
	}
}

// WithReferenceRules 设置参考规则 (仅参考规则接口)
func WithReferenceRules(rules ...ports.ReferenceCleaningRule) StandardizerOption {
	return func(s *CoreStandardizer) {
		s.referenceRules = append(s.referenceRules, rules...)
	}
}

// WithReferenceSource 设置历史仓储参考源 (用于参考对象的 STANDARD_REPO 来源)
func WithReferenceSource(src ports.ReferenceSource) StandardizerOption {
	return func(s *CoreStandardizer) {
		s.referenceSource = src
	}
}

// Deprecated: Sink 投递属于 Application Pipeline。
// Core 不再自动投递结果 (由 ProcessingPipeline 负责)。
// 本选项为无操作，仅保留以避免调用方立即编译失败。
func WithResultSinks(sinks ...ports.ResultSink) StandardizerOption {
	return func(s *CoreStandardizer) {}
}

// WithConcurrencyLimit 设置最大并发数 (默认 100)
func WithConcurrencyLimit(limit int) StandardizerOption {
	return func(s *CoreStandardizer) {
		if limit > 0 {
			s.concurrencyLimit = limit
		}
	}
}

// newCoreStandardizer 内部构造函数，返回具体实现
func newCoreStandardizer(opts ...StandardizerOption) *CoreStandardizer {
	// 默认配置
	s := &CoreStandardizer{
		sanitizer:        newChainSanitizer(nil, nil),    // 默认无规则
		aligner:          domain.NewAligner(time.Minute), // 默认容差 1m
		standardInterval: 15 * time.Minute,               // 默认间隔 15m
		precisionFactor:  DefaultScaleFactor,             // 默认精度 4 位小数
		concurrencyLimit: 100,                            // 默认并发 100
		repo:             nil,
	}

	// 应用选项
	for _, opt := range opts {
		opt(s)
	}

	// 选项应用完毕后统一构建清洗器 (旧规则与参考规则由内部适配器组合)
	if len(s.cleaningRules) > 0 || len(s.referenceRules) > 0 {
		s.sanitizer = newChainSanitizer(s.cleaningRules, s.referenceRules)
	}

	return s
}

// NewCoreStandardizer 初始化标准化服务 (旧接口)
// 使用 Functional Options 模式进行配置
func NewCoreStandardizer(opts ...StandardizerOption) ports.EnergyDataStandardizer {
	return newCoreStandardizer(opts...)
}

// NewEnergyDataProcessor 初始化标准化服务 (完整处理接口)
// 与 NewCoreStandardizer 复用同一内部构造函数，返回支持 Process 的接口。
func NewEnergyDataProcessor(opts ...StandardizerOption) ports.EnergyDataProcessor {
	return newCoreStandardizer(opts...)
}

// GetStandardReading 获取特定时间点的标准读数
// 职责：查询服务 (Query Service)
// 描述: “某设备在某时间点的标准读数是多少？” -> 清洗过、精度对齐的标准答案。
func (s *CoreStandardizer) GetStandardReading(ctx context.Context, deviceID string, timestamp time.Time) (*domain.StandardReading, error) {
	if s.repo == nil {
		return nil, fmt.Errorf("repository not configured: cannot query historical standards in stateless mode")
	}
	return s.repo.FindExact(ctx, deviceID, timestamp)
}

func (s *CoreStandardizer) ProcessAndStandardize(ctx context.Context, rawReadings []domain.Reading) ([]domain.StandardReading, error) {
	result, err := s.Process(ctx, rawReadings)
	if err != nil {
		return nil, err
	}
	return result.Accepted, nil
}

// Process 完整处理流程:
//  1. 数据清洗 (旧规则 / 参考规则，记录规则评估)
//  2. 标准数据并发标准化与时间对齐
//
// Core 只负责生成 ProcessingResult，不执行任何持久化、隔离区保存或 Sink 投递，
// 这些职责由 Application Pipeline (pkg/application/pipeline) 承担。
func (s *CoreStandardizer) Process(ctx context.Context, rawReadings []domain.Reading) (domain.ProcessingResult, error) {
	// Step 1: A. 数据清洗 (替别人做“脏活累活”)
	// 剔除空值、负值、重复值和异常跳变
	// 这一步是批量操作，因为清洗依赖上下文（如前后值的跳变）
	var cleanReadings []domain.Reading
	var quarantinedReadings []domain.QuarantineReading
	var evaluations []domain.RuleEvaluation

	if s.ruleRepo != nil {
		// 动态加载规则清洗
		var err error
		cleanReadings, quarantinedReadings, err = s.cleanWithDynamicRules(ctx, rawReadings)
		if err != nil {
			// Fallback or error? For now log and return partial?
			// To be safe, return error
			return domain.ProcessingResult{}, fmt.Errorf("dynamic cleaning failed: %w", err)
		}
	} else {
		// 使用默认规则清洗 (支持参考规则)
		cleanRes, err := s.sanitizer.CleanWithReferences(ctx, rawReadings, s.referenceSource)
		if err != nil {
			return domain.ProcessingResult{}, fmt.Errorf("cleaning failed: %w", err)
		}
		cleanReadings = cleanRes.Clean
		quarantinedReadings = cleanRes.Quarantined
		evaluations = cleanRes.Evaluations
	}

	// Step 2 (Optimization): Concurrency Strategy (Sharding by DeviceID)
	deviceGroups := make(map[string][]domain.Reading)
	for _, r := range cleanReadings {
		deviceGroups[r.DeviceInfo.ID] = append(deviceGroups[r.DeviceInfo.ID], r)
	}

	var standards []domain.StandardReading
	var mu sync.Mutex
	var wg sync.WaitGroup
	errChan := make(chan error, len(deviceGroups))

	// Semaphore for bounded concurrency
	sem := make(chan struct{}, s.concurrencyLimit)

	for _, readings := range deviceGroups {
		wg.Add(1)
		sem <- struct{}{} // Acquire token

		go func(devReadings []domain.Reading) {
			defer wg.Done()
			defer func() { <-sem }() // Release token

			if len(devReadings) == 0 {
				return
			}

			// 注意: 数据已经在 Sanitizer.Clean() 中按时间排序
			// 但按设备分组后可能打乱顺序，需要重新排序
			sort.Slice(devReadings, func(i, j int) bool {
				return devReadings[i].Timestamp.Before(devReadings[j].Timestamp)
			})

			// Step C: Frequency Alignment (Time Alignment)
			// Generate time grid based on standard interval
			startTime := devReadings[0].Timestamp.Truncate(s.standardInterval)
			endTime := devReadings[len(devReadings)-1].Timestamp
			// Align endTime to grid ceiling
			if rem := endTime.Sub(endTime.Truncate(s.standardInterval)); rem > 0 {
				endTime = endTime.Truncate(s.standardInterval).Add(s.standardInterval)
			} else {
				endTime = endTime.Truncate(s.standardInterval)
			}

			var groupStandards []domain.StandardReading

			for t := startTime; !t.After(endTime); t = t.Add(s.standardInterval) {
				// Context cancellation check (Fast fail)
				select {
				case <-ctx.Done():
					errChan <- ctx.Err()
					return
				default:
				}

				// Find snapshot for this time slot
				snapshot := s.aligner.FindSnapshot(devReadings, t)
				if snapshot != nil {
					// Step 2: B. 单条转换
					sr := s.standardizeOne(ctx, *snapshot)
					sr.Timestamp = t // Force alignment to the grid time
					groupStandards = append(groupStandards, sr)
				}
			}

			mu.Lock()
			standards = append(standards, groupStandards...)
			mu.Unlock()

		}(readings)
	}

	wg.Wait()
	close(errChan)

	// 收集所有错误
	var errs []error
	for err := range errChan {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return domain.ProcessingResult{}, errors.Join(errs...)
	}

	return domain.ProcessingResult{
		Accepted:    standards,
		Rejected:    quarantinedReadings,
		Evaluations: evaluations,
	}, nil
}

// DefaultScaleFactor 默认精度因子 (支持4位小数精度)
const DefaultScaleFactor = 10000

// standardizeOne 封装单条数据的转换逻辑 (SR - Single Responsibility: Mapping)
func (s *CoreStandardizer) standardizeOne(ctx context.Context, r domain.Reading) domain.StandardReading {
	// Determine Priority from Context
	priority := domain.IngestStrategyRealtime.GetPriority() // Default
	if info, ok := domain.FromContext(ctx); ok {
		priority = info.Strategy.GetPriority()
	}

	// 精度转换: 浮点数 -> 高精度整型
	// 例如: 123.4567 * 10000 = 1234567
	scaledValue := int64(r.Value * float64(s.precisionFactor))

	// 2. 结构封装
	return domain.StandardReading{
		DeviceID:     r.DeviceInfo.ID,
		Timestamp:    r.Timestamp,
		ValueScaled:  scaledValue,
		ScaleFactor:  s.precisionFactor,
		ValueDisplay: r.Value,
		SourceType:   domain.ReadingTypeStandard,
		Quality:      domain.QualityValid, // 经过清洗剩下的都是有效值

		// Backfilling & Governance Support
		IngestedAt: time.Now(),
		Priority:   priority,
	}
}
