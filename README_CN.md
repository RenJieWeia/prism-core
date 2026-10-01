# Prism Core SDK

**Prism Core** 是 Prism 能源数据生态系统的基础 SDK。它提供了一个基于**六边形架构**的高性能、模块化数据处理引擎，专为水、电、气等异构能源数据的标准化而设计。

本库被设计为核心依赖 (Core Dependency)，供上层服务（如 HTTP API、CLI 工具、ETL 管道）引用，以提供一致的数据清洗和标准化能力。

## 🌟 核心特性

- **通用摄入 (Universal Ingestion)**: 
  - 支持基于流 (Stream) 的 **JSON** 解析，以及 **CSV** 格式支持。能够高效处理大规模数据集，内存占用极低。
- **稳健的数据清洗流水线 (Robust Pipeline)**:
  - **策略模式 (Strategy Pattern)**: 清洗规则完全解耦，通过 `RuleFactory` 支持热插拔。
  - **内置规则库**:
    - `RangeRule`: 范围检查，支持 Min/Max 阈值校验与自动修正 (Clamping)。
    - `MonotonicRule`: 单调性检查，防止累计读数回退。
    - `RateRule`: 变化率检查，检测不可能出现的尖峰跳变。
    - `StagnationRule`: 停滞检查，识别死传感器 (连续读数不变)。
    - 规则均通过 `RuleFactory` 按类型实例化 (RANGE / MONOTONIC / RATE / STAGNATION)。
  - **责任链 (Chain of Responsibility)**: 通过 `Sanitizer` 服务串行执行配置的过滤器。
  - **结构化结果**: 规则返回 `CheckResult` 结构体，包含修正状态和原因说明。
- **数据标准化 (Standardization)**:
  - **精度统一**: 将浮点数转换为高精度的整型定点数 (Scaled Integer)，默认 10000 倍精度 (4 位小数)，可通过 `WithPrecision(factor)` 配置；转换采用 **int64 截断**。
  - **时间对齐 (Aligner)**: 使用**二分查找算法** O(log n) 将散乱的时间点对齐到标准的整点快照。
- **参考对象 (Reference Objects)** (v1.1+): 规则可比较"当前数据 vs 参考数据"——同一设备的**上一条有效值**、**相对时间点**数据（如三天前，以当前读数自身 `Timestamp` 为基准，绝不用 `time.Now()`）、或**时间窗口**聚合数据（`AVG`/`SUM`/`MIN`/`MAX`/`LATEST`/`DELTA`）。参考缺失按策略处理：`SKIP_RULE` / `REJECT` / `QUARANTINE`。
- **完整处理结果 (Full Processing Result)** (v1.1+): `NewEnergyDataProcessor` 提供的 `Process` 返回 `ProcessingResult{Accepted, Rejected, Evaluations}`，含每条规则的评估记录（`PASS`/`REJECT`/`SKIP`/`CORRECT`/`QUARANTINE`）、使用的参考对象 ID 与原因。
- **下游输出 (Downstream Output)** (v1.1+): 应用层管线（`ProcessingPipeline` + `ResultSink`，如 `MemorySink` / `CallbackSink` / `RepositorySink` / `QuarantineSink`）持久化 Accepted/Rejected 并在每次处理后接收完整结果；多个 Sink 失败时错误聚合返回，绝不静默丢弃。
- **多设备状态隔离**: 有状态规则按**设备**分别维护"上一条有效数据"，交错的多设备数据互不污染，且不原地修改调用方传入的数据切片。
- **并发安全**:
  - 按设备分片并使用有界信号量限制并发，处理错误通过 `errors.Join()` 聚合返回。
- **架构设计**:
  - **Domain (领域层)**: 核心业务实体与接口定义 (`pkg/core/domain`)，含 `ReferenceSpec`、`ProcessingResult` 等。
  - **Services (服务层)**: 业务流程编排 (`pkg/core/services`)，包含 Sanitizer 与 Standardizer 实现、`BatchReferenceSource`/`ReferenceResolver`。
  - **Adapters (适配层)**: 外部交互实现 (`pkg/adapters`)，包含 Ingestors、Factory、`RepositoryReferenceSource` 与 ResultSinks。

## 🎬 快速体验 (Demo)

无需编写代码即可感受完整流水线 (接入 → 清洗 → 标准化 → 查询)：

```bash
go run ./cmd/demo              # 脚本化演示 (内置脏数据样例)
go run ./cmd/demo -ingest data.json   # 自喂数据 (.json / .csv)
```

## 📂 项目结构

```
prism-core/
├── cmd/
│   └── demo/          # 演示程序 (go run ./cmd/demo)
├── docs/
│   └── dev-guide/     # 开发手册 (六边形架构说明)
├── pkg/
│   ├── adapters/      # 适配器层 (外部交互)
│   │   ├── factory/    #   规则工厂 (RuleFactory 规则实例化)
│   │   ├── ingest/     #   数据摄入实现 (CSV, JSON)
│   │   ├── reference/  #   仓储参考源 (RepositoryReferenceSource)
│   │   └── sink/       #   结果输出端口 (Memory / Callback / Repository)
│   └── core/
│       ├── domain/    # 核心业务逻辑 (实体 & 算法)
│       │   ├── aligner.go    # 时间对齐逻辑
│       │   ├── unifier.go    # 精度转换器 (math.Round)
│       │   ├── rule.go       # 规则定义 (RuleType / RuleAction)
│       │   ├── reference.go  # 参考对象 (ReferenceSpec / Request / Value)
│       │   ├── result.go     # 处理结果 (ProcessingResult / RuleEvaluation)
│       │   └── ...
│       ├── ports/     # 接口定义 (驱动/被驱动端口)
│       └── services/  # 应用服务 (流程编排)
│           ├── sanitizer.go  # 清洗器 (责任链)
│           ├── reference_source.go # 批次参考源与解析器 (Batch / Resolver)
│           ├── rules/        # 内置规则: Range / Monotonic / Rate / Stagnation
│           └── ...
├── tests/             # 外部集成测试 (包均为 xxx_test)
│   ├── adapters/      # 工厂、接入器、参考源与 Sink 测试
│   └── core/          # 领域与服务层测试
└── testdata/          # 测试用例样本数据
```

## 🚀 快速开始

### 安装

```bash
go get github.com/RenJieWeia/prism-core
```

使用 `JsonUniversalIngestor` 从文件或网络流中读取原始数据。

```go
import (
    "context"
    "fmt"
    "os"
    "github.com/RenJieWeia/prism-core/pkg/adapters/ingest"
    "github.com/RenJieWeia/prism-core/pkg/core/services"
    "github.com/RenJieWeia/prism-core/pkg/core/domain"
)

// 定义数据接收回调（模拟“下游”处理）
downstreamHandler := func(ctx context.Context, readings []domain.Reading) error {
    for _, r := range readings {
        fmt.Printf("Received: %s - %.2f\n", r.DeviceInfo.ID, r.Value)
    }
    return nil
}

// 初始化摄入器
ingestor := ingest.NewJsonUniversalIngestor(downstreamHandler)

// 打开数据源 (io.Reader)
file, _ := os.Open("data.json")
defer file.Close()

// 开始流式处理
ingestor.IngestStream(context.Background(), file)
```

### 2. 配置清洗规则与标准化服务

核心服务 `CoreStandardizer` 负责编排清洗和标准化逻辑。你可以根据业务需求注入不同的规则。

```go
import (
    "time"
    "github.com/RenJieWeia/prism-core/pkg/core/services"
    "github.com/RenJieWeia/prism-core/pkg/core/services/rules"
    "github.com/RenJieWeia/prism-core/pkg/core/domain"
)

// 使用内置的 RangeRule (范围检查)
rangeRule := &rules.RangeRule{
    Min:    0.0,
    Max:    1000.0,
    Action: domain.ActionReject, // 超出范围直接丢弃
}

// 场景：我们需要严格的数据质量控制
// 使用 Functional Options 配置服务
// 精度转换已内置，默认 ScaleFactor = 10000
standardizer := services.NewCoreStandardizer(
    services.WithCleaningRules(rangeRule),
    services.WithAlignment(15*time.Minute, 1*time.Minute),
)
```

### 3. 执行并获取结果

```go
rawReadings := []domain.Reading{
    {Timestamp: t1, Value: 100.0},
    {Timestamp: t2, Value: -5.0}, // 将被 RangeRule 过滤
    {Timestamp: t3, Value: 105.0},
}

// 执行标准化
results, err := standardizer.ProcessAndStandardize(ctx, rawReadings)

// 结果中只包含有效且转换后的数据
for _, res := range results {
    fmt.Printf("Standardized: %d (Raw: %.2f)\n", res.ValueScaled, res.ValueDisplay)
}
// Output:
// Standardized: 1000000 (Raw: 100.00)
// Standardized: 1050000 (Raw: 105.00)
```

### 4. 数据持久化与下游输出 (Persistence & Downstream)

持久化与 Sink 投递属于**应用层**：通过 `ProcessingPipeline` 组合 `RepositorySink`（保存 Accepted）与 `QuarantineSink`（保存 Rejected），并可追加其他 Sink。

```go
import (
    "github.com/RenJieWeia/prism-core/pkg/adapters/sink"
    "github.com/RenJieWeia/prism-core/pkg/application/pipeline"
    "github.com/RenJieWeia/prism-core/pkg/core/services"
)

proc := services.NewEnergyDataProcessor() // Core: 清洗 + 标准化
pl := pipeline.NewProcessingPipeline(
    proc,
    sink.NewRepositorySink(repo, ports.UpsertStrategyHighPriorityWins), // 保存 Accepted
    sink.NewQuarantineSink(quarantineRepo),                             // 保存 Rejected
)

result, err := pl.Execute(ctx, rawReadings)
// result.Accepted / result.Rejected / result.Evaluations
```

> `WithRepository` 已弃用其持久化语义，仅用于 `GetStandardReading` 查询；Core 的 `Process` 不再自动落库、不再保存隔离数据、不再投递 Sink。

### 5. 动态规则加载 (Dynamic Rules)

按设备类型从仓储加载清洗规则时，需要同时注入规则仓储与规则工厂：

```go
import "github.com/RenJieWeia/prism-core/pkg/adapters/factory" // 具体工厂实现 (基础设施层)

standardizer := services.NewCoreStandardizer(
    services.WithRuleRepository(ruleRepo),   // 实现 ports.CleaningRuleRepository
    services.WithRuleFactory(factory.GetRuleFactory()), // ports.CleaningRuleFactory
)
```

> 依赖倒置：核心层只依赖 `ports.CleaningRuleFactory` 接口，不直接依赖 `pkg/adapters`。

### 6. 参考对象与完整处理结果 (Reference Objects & Full Processing)

规则可比较"当前数据 vs 参考数据"。实现 `ports.ReferenceCleaningRule` 并通过 `domain.ReferenceSpec` 声明所需参考对象：

```go
import "github.com/RenJieWeia/prism-core/pkg/core/domain"

// 示例: 同一设备三天前的值 (以当前读数 Timestamp 为基准, 不使用 time.Now())
spec := domain.ReferenceSpec{
    ID:      "d3",
    Source:  domain.ReferenceSourceStandardRepo, // 或 ReferenceSourceCurrentBatch (当前批次)
    Binding: domain.ReferenceBindingSameDevice,  // 或 EXPLICIT + DeviceID (指定设备)
    Time:    domain.ReferenceTimeSelector{Mode: domain.ReferenceTimeRelative, Offset: 72 * time.Hour, Tolerance: time.Hour},
    MissingPolicy: domain.MissingReferenceSkip,  // SKIP_RULE / REJECT / QUARANTINE
}

// 参考规则: 当前值必须 >= 参考值
type RefGeRule struct{ Spec domain.ReferenceSpec }
func (r *RefGeRule) RuleID() string { return "ref-ge" }
func (r *RefGeRule) ReferenceSpecs() []domain.ReferenceSpec { return []domain.ReferenceSpec{r.Spec} }
func (r *RefGeRule) CheckWithReferences(in domain.RuleInput) ports.CheckResult {
    ref := in.References[r.Spec.ID]
    if !ref.Found || in.Current.Value >= ref.Value {
        return ports.CheckResult{Reading: in.Current, Passed: true}
    }
    return ports.CheckResult{Reading: in.Current, Passed: false, Reason: "below reference"}
}
```

时间选择器支持三种模式：

- `PREVIOUS`：同一设备**上一条有效值**（通过规则链的有效数据，跨设备隔离）。
- `RELATIVE`：目标时间 = `当前时间 - Offset`（Offset 非负，向历史方向偏移）。
- `WINDOW`：`[当前时间-Offset, 当前时间)` 内聚合，支持 `AVG`/`SUM`/`MIN`/`MAX`/`LATEST`/`DELTA`。

注册参考规则、注入历史参考源，并通过处理管线获取完整结果：

```go
import (
    "github.com/RenJieWeia/prism-core/pkg/adapters/reference"
    "github.com/RenJieWeia/prism-core/pkg/application/pipeline"
    "github.com/RenJieWeia/prism-core/pkg/core/services"
)

// Core 处理器: 清洗 + 参考解析 + 标准化
proc := services.NewEnergyDataProcessor(
    services.WithReferenceRules(&RefGeRule{Spec: spec}),                     // ports.ReferenceCleaningRule
    services.WithReferenceSource(reference.NewRepositoryReferenceSource(repo, time.Minute)), // STANDARD_REPO 历史参考源
)

// 应用层管线: 持久化 (Accepted/Rejected) + 其他 Sink
pl := pipeline.NewProcessingPipeline(
    proc,
    sink.NewRepositorySink(repo, ports.UpsertStrategyHighPriorityWins),
    sink.NewQuarantineSink(quarantineRepo),
)

result, err := pl.Execute(ctx, rawReadings)
// result.Accepted    []domain.StandardReading   通过并标准化的数据
// result.Rejected    []domain.QuarantineReading 被拒绝/隔离的数据
// result.Evaluations []domain.RuleEvaluation    规则评估 (RuleID / Outcome / Reason / ReferenceIDs)
```

要点：

- `ReferenceSource` 是参考查询端口（`Resolve(ctx, requests)`），规则不得直接查库。内置实现：`BatchReferenceSource`（当前批次）与 `RepositoryReferenceSource`（历史 `StandardReadingRepository`），单次处理内有内存缓存去重。
- 声明 `STANDARD_REPO` 参考对象但未配置参考源、或 `RELATIVE`/`WINDOW` 的 `Offset` 为负，都会在处理前返回明确配置错误——参考规则**绝不静默退化**。
- `SKIP_RULE`/`REJECT`/`QUARANTINE` 仅在"确实查无参考数据"时触发。
- 持久化与投递属于应用层：`ProcessingPipeline` + `RepositorySink`（Accepted）/ `QuarantineSink`（Rejected）+ 其他 Sink；`WithRepository` 仅用于 `GetStandardReading` 查询，不再写入。
- 实现位于 `pkg/adapters/reference`、`pkg/adapters/sink` 与 `pkg/application/pipeline`，`pkg/core` 不依赖任何适配器。

## 🛠 开发与测试

本项目采用严格的测试分离策略，单元测试位于 `tests/` 目录下。

### 运行测试
确保你已安装 Go 1.25+。

```bash
# 运行所有测试
go test ./...
go vet ./...
# 竞态检测 (需要 gcc/clang, 即 CGO_ENABLED=1)
CGO_ENABLED=1 go test -race ./...
```

## 📄 许可证
MIT License
