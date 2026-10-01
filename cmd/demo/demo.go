package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/RenJieWeia/prism-core/pkg/adapters/factory"
	"github.com/RenJieWeia/prism-core/pkg/adapters/ingest"
	"github.com/RenJieWeia/prism-core/pkg/adapters/sink"
	"github.com/RenJieWeia/prism-core/pkg/application/pipeline"
	"github.com/RenJieWeia/prism-core/pkg/core/domain"
	"github.com/RenJieWeia/prism-core/pkg/core/ports"
	"github.com/RenJieWeia/prism-core/pkg/core/services"
	"github.com/RenJieWeia/prism-core/pkg/core/services/rules"
)

const (
	demoDevice1 = "D-101"
	demoDevice2 = "D-102"
)

// ---------------------------------------------------------------------------
// 演示用内存仓储
// ---------------------------------------------------------------------------

// memoryStandardsRepo 内存版标准读数仓储 (演示持久化与查询)
type memoryStandardsRepo struct {
	mu        sync.Mutex
	standards map[string]domain.StandardReading // key: deviceID@timestamp
}

func newMemoryStandardsRepo() *memoryStandardsRepo {
	return &memoryStandardsRepo{standards: make(map[string]domain.StandardReading)}
}

func stdKey(deviceID string, ts time.Time) string {
	return deviceID + "@" + ts.UTC().Format(time.RFC3339Nano)
}

func (m *memoryStandardsRepo) Save(_ context.Context, r domain.StandardReading, _ ports.UpsertStrategy) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.standards[stdKey(r.DeviceID, r.Timestamp)] = r
	return nil
}

func (m *memoryStandardsRepo) SaveBatch(_ context.Context, rs []domain.StandardReading, strategy ports.UpsertStrategy) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, r := range rs {
		k := stdKey(r.DeviceID, r.Timestamp)
		if strategy == ports.UpsertStrategyHighPriorityWins {
			if old, ok := m.standards[k]; ok && old.Priority > r.Priority {
				continue // 低优先级不覆盖高优先级
			}
		}
		m.standards[k] = r
	}
	return nil
}

func (m *memoryStandardsRepo) FindExact(_ context.Context, deviceID string, ts time.Time) (*domain.StandardReading, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r, ok := m.standards[stdKey(deviceID, ts)]; ok {
		cp := r
		return &cp, nil
	}
	return nil, nil
}

func (m *memoryStandardsRepo) FindRange(_ context.Context, deviceID string, start, end time.Time) ([]domain.StandardReading, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []domain.StandardReading
	for _, r := range m.standards {
		if r.DeviceID == deviceID && !r.Timestamp.Before(start) && !r.Timestamp.After(end) {
			out = append(out, r)
		}
	}
	return out, nil
}

// memoryQuarantineRepo 内存版隔离区仓储 (接收异步写入的隔离数据)
type memoryQuarantineRepo struct {
	mu    sync.Mutex
	items []domain.QuarantineReading
}

func (m *memoryQuarantineRepo) Save(_ context.Context, q domain.QuarantineReading) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.items = append(m.items, q)
	return nil
}

func (m *memoryQuarantineRepo) FindPending(_ context.Context, limit int) ([]domain.QuarantineReading, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := m.items
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (m *memoryQuarantineRepo) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.items)
}

var (
	_ ports.StandardReadingRepository = (*memoryStandardsRepo)(nil)
	_ ports.QuarantineRepository      = (*memoryQuarantineRepo)(nil)
)

// demoRuleRepo 演示用规则仓储 (按设备类型返回规则配置)
type demoRuleRepo struct {
	byType map[domain.DeviceType][]domain.CleaningRule
}

func (d *demoRuleRepo) Save(_ context.Context, _ domain.CleaningRule) error { return nil }
func (d *demoRuleRepo) GetByID(_ context.Context, _ string) (*domain.CleaningRule, error) {
	return nil, nil
}
func (d *demoRuleRepo) ListByDeviceType(_ context.Context, dt domain.DeviceType) ([]domain.CleaningRule, error) {
	return d.byType[dt], nil
}
func (d *demoRuleRepo) ListEnabledByDeviceType(_ context.Context, dt domain.DeviceType) ([]domain.CleaningRule, error) {
	var out []domain.CleaningRule
	for _, r := range d.byType[dt] {
		if r.Enabled {
			out = append(out, r)
		}
	}
	return out, nil
}
func (d *demoRuleRepo) Delete(_ context.Context, _ string) error { return nil }

var _ ports.CleaningRuleRepository = (*demoRuleRepo)(nil)

// ---------------------------------------------------------------------------
// 样例数据
// ---------------------------------------------------------------------------

type annotated struct {
	reading domain.Reading
	note    string
}

func sampleReadings() []annotated {
	base, _ := time.Parse(time.RFC3339, "2026-08-04T10:00:00Z")
	add := func(dev string, mins int, v float64, note string) annotated {
		return annotated{
			reading: domain.Reading{
				DeviceInfo: domain.DeviceInfo{ID: dev, Model: "AX-1", Type: domain.DeviceTypeElec},
				Timestamp:  base.Add(time.Duration(mins) * time.Minute),
				Value:      v,
			},
			note: note,
		}
	}
	return []annotated{
		add(demoDevice1, 0, 100.0, ""),
		add(demoDevice1, 15, 105.0, ""),
		add(demoDevice1, 15, 105.0, "重复时间戳"),
		add(demoDevice1, 30, 90.0, "单调回退"),
		add(demoDevice1, 45, 110.0, ""),
		add(demoDevice1, 60, 110.0, "停滞(与上一条相同)"),
		add(demoDevice1, 75, 115.0, ""),
		add(demoDevice1, 90, 1000.0, "速率尖峰"),
		add(demoDevice1, 105, -5.0, "负值"),
		add(demoDevice1, 120, 120.0, ""),
		add(demoDevice1, 135, 20000.0, "越界"),
		add(demoDevice2, 1, 50.0, ""),
		add(demoDevice2, 30, 60.0, ""),
	}
}

// newDemoStandardizer 构建带完整规则链的标准化服务 (返回完整处理接口)
func newDemoStandardizer(opts ...services.StandardizerOption) ports.EnergyDataProcessor {
	defaults := []services.StandardizerOption{
		services.WithCleaningRules(
			&rules.RangeRule{Min: 0, Max: 10000, Action: domain.ActionReject},
			&rules.MonotonicRule{Action: domain.ActionReject},
			&rules.RateRule{MaxRatePerSecond: 0.5, Action: domain.ActionReject},
			&rules.StagnationRule{Action: domain.ActionReject},
		),
		services.WithAlignment(15*time.Minute, 5*time.Minute),
		services.WithPrecision(10000),
	}
	return services.NewEnergyDataProcessor(append(defaults, opts...)...)
}

// ---------------------------------------------------------------------------
// 脚本化演示
// ---------------------------------------------------------------------------

func runScriptedDemo() {
	ctx := context.Background()
	fmt.Println("==============================================================")
	fmt.Println("  Prism Core 能耗数据标准化引擎 · 演示")
	fmt.Println("  目标: 展示「接入 → 清洗 → 标准化 → 查询」全链路能力")
	fmt.Println("==============================================================")

	// ---------- 第 1 节: 配置规则链 ----------
	fmt.Println("\n[1/6] 配置清洗规则链 (ChainSanitizer + 4 条策略规则)")
	fmt.Println("  - RangeRule       范围 [0, 10000]，越界拒绝")
	fmt.Println("  - MonotonicRule   单调性，累计值回退拒绝")
	fmt.Println("  - RateRule        变化率 ≤ 0.5/s，尖峰拒绝")
	fmt.Println("  - StagnationRule  停滞，连续相同读数拒绝")

	// ---------- 第 2 节: 生成原始数据 ----------
	rows := sampleReadings()
	fmt.Println("\n[2/6] 生成原始读数 (D-101 / D-102，每 15 分钟一条)")
	fmt.Printf("  %-6s  %-8s  %-12s  %s\n", "设备", "时间", "值", "疑似问题")
	for _, a := range rows {
		fmt.Printf("  %-6s  %-8s  %-12.4f  %s\n",
			a.reading.DeviceInfo.ID, a.reading.Timestamp.Format("15:04:05"), a.reading.Value, a.note)
	}

	// ---------- 第 3 节: 清洗层 ----------
	fmt.Println("\n[3/6] 清洗层 (ChainSanitizer) —— 逐条判定：通过 / 拒绝")
	readings := make([]domain.Reading, 0, len(rows))
	for _, a := range rows {
		readings = append(readings, a.reading)
	}
	sanitizer := services.NewSanitizer(
		&rules.RangeRule{Min: 0, Max: 10000, Action: domain.ActionReject},
		&rules.MonotonicRule{Action: domain.ActionReject},
		&rules.RateRule{MaxRatePerSecond: 0.5, Action: domain.ActionReject},
		&rules.StagnationRule{Action: domain.ActionReject},
	)
	clean, quarantined := sanitizer.Clean(readings)

	fmt.Printf("  通过 %d 条，拒绝 %d 条:\n", len(clean), len(quarantined))
	for _, c := range clean {
		fmt.Printf("    ✔ 通过   %-6s %s  %10.4f\n", c.DeviceInfo.ID, c.Timestamp.Format("15:04:05"), c.Value)
	}
	for _, q := range quarantined {
		fmt.Printf("    ✘ 拒绝   %-6s %s  %10.4f  | %s\n",
			q.Reading.DeviceInfo.ID, q.Reading.Timestamp.Format("15:04:05"), q.Reading.Value, q.Reason)
	}

	// ---------- 第 4 节: 标准化层 ----------
	fmt.Println("\n[4/6] 标准化层 (CoreStandardizer + ProcessingPipeline) —— 时间对齐 15min 网格 + 精度 ×10000 (int64 截断)")
	standardsRepo := newMemoryStandardsRepo()
	quarantineRepo := &memoryQuarantineRepo{}
	proc := newDemoStandardizer(services.WithRepository(standardsRepo)) // 仓储仅用于 GetStandardReading 查询
	pl := pipeline.NewProcessingPipeline(
		proc,
		sink.NewRepositorySink(standardsRepo, ports.UpsertStrategyHighPriorityWins),
		sink.NewQuarantineSink(quarantineRepo),
	)

	result, err := pl.Execute(ctx, readings)
	if err != nil {
		fmt.Printf("  标准化失败: %v\n", err)
		os.Exit(1)
	}
	printStandards("  标准化结果", result.Accepted)
	fmt.Println("  说明: RepositorySink 已按 UpsertStrategyHighPriorityWins 持久化 Accepted 到内存仓储")
	fmt.Printf("  说明: QuarantineSink 已保存 %d 条隔离数据到隔离区仓储\n", quarantineRepo.count())

	// ---------- 第 5 节: 查询层 ----------
	fmt.Println("\n[5/6] 查询层 —— 从仓储读取标准读数")
	sr, err := proc.GetStandardReading(ctx, demoDevice1, mustParse("2026-08-04T10:00:00Z"))
	if err != nil {
		fmt.Printf("  GetStandardReading 失败: %v\n", err)
	} else if sr != nil {
		fmt.Printf("  单点查询  GetStandardReading(D-101, 10:00) = ValueScaled=%d (x%d)\n", sr.ValueScaled, sr.ScaleFactor)
	} else {
		fmt.Println("  单点查询  GetStandardReading(D-101, 10:00) = 未找到")
	}
	rangeList, _ := standardsRepo.FindRange(ctx, demoDevice1,
		mustParse("2026-08-04T10:00:00Z"), mustParse("2026-08-04T12:00:00Z"))
	fmt.Printf("  范围查询  FindRange(D-101, 10:00~12:00) = %d 条标准读数\n", len(rangeList))

	// ---------- 第 6 节: 动态规则 ----------
	demoDynamicRules(ctx)

	// ---------- 附加: 接入器全链路 ----------
	demoIngestor(ctx)

	// ---------- 附加: 性能演示 ----------
	demoPerformance(ctx)

	fmt.Println("\n==============================================================")
	fmt.Println("  演示结束。用你自己的数据试试: go run ./cmd/demo -ingest data.json")
	fmt.Println("==============================================================")
}

// demoDynamicRules 演示按设备类型动态加载规则
func demoDynamicRules(ctx context.Context) {
	fmt.Println("\n[6/6] 动态规则加载 —— 按设备类型从规则仓储读取规则配置并实例化")
	ruleRepo := &demoRuleRepo{byType: map[domain.DeviceType][]domain.CleaningRule{
		domain.DeviceTypeElec: {
			{ID: "r-elec-range", Type: domain.RuleTypeRange, Enabled: true,
				Parameters: map[string]interface{}{"min": float64(0), "max": float64(10000)},
				Action:     domain.ActionReject},
		},
	}}
	qRepo := &memoryQuarantineRepo{}
	proc := services.NewEnergyDataProcessor(
		services.WithRuleRepository(ruleRepo),
		services.WithRuleFactory(factory.NewRuleFactory()),
	)
	pl := pipeline.NewProcessingPipeline(proc, nil, sink.NewQuarantineSink(qRepo))
	base, _ := time.Parse(time.RFC3339, "2026-08-04T10:00:00Z")
	feed := []domain.Reading{
		{DeviceInfo: domain.DeviceInfo{ID: "D-E1", Type: domain.DeviceTypeElec}, Timestamp: base, Value: 100},
		{DeviceInfo: domain.DeviceInfo{ID: "D-E2", Type: domain.DeviceTypeElec}, Timestamp: base.Add(15 * time.Minute), Value: -50},
		{DeviceInfo: domain.DeviceInfo{ID: "D-E3", Type: domain.DeviceTypeElec}, Timestamp: base.Add(30 * time.Minute), Value: 200},
	}
	fmt.Println("  规则配置 (设备类型 ELEC): Range[0, 10000] REJECT")
	res, err := pl.Execute(ctx, feed)
	if err != nil {
		fmt.Printf("  动态清洗失败: %v\n", err)
		return
	}
	// QuarantineSink 同步保存隔离数据
	fmt.Printf("  输入 3 条 ELEC 读数 -> 通过 %d 条, 隔离区 %d 条 (负值 -50 被 Range 规则拒绝)\n", len(res.Accepted), qRepo.count())
}

// demoIngestor 演示 UniversalIngestor 接入器全链路
func demoIngestor(ctx context.Context) {
	fmt.Println("\n[附加] 接入器全链路 —— JsonUniversalIngestor → CoreStandardizer")
	jsonInput := `[
		{"device_id":"D-201","timestamp":"2026-08-04T10:00:00Z","value":100},
		{"device_id":"D-201","timestamp":"2026-08-04 10:15:00","value":105},
		{"device_id":"D-201","timestamp":"not-a-time","value":50}
	]`
	fmt.Println("  输入 JSON 数组 (第 3 条时间格式非法，应被跳过):")
	fmt.Printf("    %s\n", jsonInput)

	var (
		mu        sync.Mutex
		standards []domain.StandardReading
	)
	std := newDemoStandardizer()
	downstream := func(ctx context.Context, rs []domain.Reading) error {
		out, err := std.ProcessAndStandardize(ctx, rs)
		if err != nil {
			return err
		}
		mu.Lock()
		standards = append(standards, out...)
		mu.Unlock()
		return nil
	}
	result, err := ingest.NewJsonUniversalIngestor(downstream).IngestStream(ctx, strings.NewReader(jsonInput))
	if err != nil {
		fmt.Printf("  接入失败: %v\n", err)
		return
	}
	fmt.Printf("  接入统计: 总数=%d 成功=%d 失败=%d\n", result.Total, result.Success, result.Failed)
	for _, e := range result.Errors {
		fmt.Printf("    - %s\n", e)
	}
	printStandards("  标准化输出", standards)
}

// demoPerformance 演示并发分片处理大批量数据
func demoPerformance(ctx context.Context) {
	fmt.Println("\n[附加] 性能演示 —— 100 设备 × 100 条读数 = 10000 条并发分片处理")
	base, _ := time.Parse(time.RFC3339, "2026-08-04T00:00:00Z")
	var feed []domain.Reading
	for d := 0; d < 100; d++ {
		dev := fmt.Sprintf("PERF-%03d", d)
		seed := float64(1000 + d)
		for i := 0; i < 100; i++ {
			feed = append(feed, domain.Reading{
				DeviceInfo: domain.DeviceInfo{ID: dev, Type: domain.DeviceTypeElec},
				Timestamp:  base.Add(time.Duration(i) * time.Minute),
				Value:      seed + float64(i),
			})
		}
	}
	std := services.NewCoreStandardizer(
		services.WithAlignment(15*time.Minute, 5*time.Minute),
		services.WithConcurrencyLimit(16),
	)
	start := time.Now()
	out, err := std.ProcessAndStandardize(ctx, feed)
	elapsed := time.Since(start)
	if err != nil {
		fmt.Printf("  处理失败: %v\n", err)
		return
	}
	fmt.Printf("  输入 %d 条 -> 输出 %d 条标准读数, 耗时 %v (按设备分片 + 信号量并发)\n", len(feed), len(out), elapsed)
}

// ---------------------------------------------------------------------------
// 工具函数
// ---------------------------------------------------------------------------

func printStandards(title string, rs []domain.StandardReading) {
	fmt.Printf("%s:\n", title)
	if len(rs) == 0 {
		fmt.Println("  (无结果)")
		return
	}
	fmt.Printf("    %-6s  %-8s  %-12s  %-10s  %-8s\n", "设备", "网格时间", "ValueScaled", "ScaleFactor", "质量")
	for _, r := range rs {
		fmt.Printf("    %-6s  %-8s  %-12d  %-10d  %-8s\n",
			r.DeviceID, r.Timestamp.Format("15:04:05"), r.ValueScaled, r.ScaleFactor, r.Quality)
	}
}

func mustParse(s string) time.Time {
	t, _ := time.Parse(time.RFC3339, s)
	return t
}

// ---------------------------------------------------------------------------
// -ingest 自喂数据模式
// ---------------------------------------------------------------------------

func runIngestDemo(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("打开文件失败: %w", err)
	}
	defer f.Close()
	ctx := context.Background()

	var (
		mu        sync.Mutex
		standards []domain.StandardReading
	)
	std := newDemoStandardizer()
	downstream := func(ctx context.Context, rs []domain.Reading) error {
		out, err := std.ProcessAndStandardize(ctx, rs)
		if err != nil {
			return err
		}
		mu.Lock()
		standards = append(standards, out...)
		mu.Unlock()
		return nil
	}

	var result *domain.IngestionResult
	switch {
	case strings.HasSuffix(path, ".json"):
		result, err = ingest.NewJsonUniversalIngestor(downstream).IngestStream(ctx, f)
	case strings.HasSuffix(path, ".csv"):
		result, err = ingest.NewCsvUniversalIngestor(downstream).IngestStream(ctx, f)
	default:
		return fmt.Errorf("不支持的文件格式 (仅支持 .json / .csv): %s", path)
	}
	if err != nil {
		return err
	}

	fmt.Println("=== 接入统计 ===")
	fmt.Printf("总数: %d  成功: %d  失败: %d\n", result.Total, result.Success, result.Failed)
	for _, e := range result.Errors {
		fmt.Printf("  - %s\n", e)
	}
	printStandards("\n=== 标准化输出 ===", standards)
	return nil
}
