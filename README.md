# Prism Core SDK

[中文文档 (Chinese Documentation)](README_CN.md)

**Prism Core** is the foundational SDK for the Prism energy data ecosystem. It provides a highly modular, hexagonally architected engine for standardizing energy and utilities data (Water, Electricity, Gas) from heterogeneous sources.

This library is designed to be imported by other services (HTTP APIs, CLI tools, ETL pipelines) to provide consistent data processing capabilities.

## 🌟 Core Features

- **Universal Ingestion**: Stream-based JSON ingestor capable of handling large datasets efficiently with minimal memory footprint.
- **Robust Cleaning Pipeline**:
  - **Strategy Pattern** based cleaning rules.
  - **Pluggable Rules**:
    - `MonotonicRule`: Prevents negative accumulation/regressions.
    - `RateRule`: Detects and filters impossible spikes (jumps).
    - `StagnationRule`: Identifies dead sensors.
    - `RangeRule`: Min/Max threshold checks with optional clamping.
    - Rules are instantiated from config via `RuleFactory` (RANGE / MONOTONIC / RATE / STAGNATION).
  - **Chain of Responsibility**: `Sanitizer` runs a configurable chain of filters.
- **Data Standardization**:
  - **Precision Control**: floating-point readings → high-precision `int64` scaled values (default ×10000), configurable via `WithPrecision(factor)`. Conversion uses **int64 truncation**.
  - **Time Alignment**: `Aligner` (O(log n) binary search) snaps readings to standard intervals (Snapshots) within a configurable tolerance.
- **Reference Objects** (v1.1+): rules can compare the current reading against reference data — the same device's **previous valid value**, a value at a **relative point in time** (e.g. 3 days ago, anchored to the reading's own `Timestamp`, never `time.Now()`), or a **rolling time window** aggregated via `AVG`/`SUM`/`MIN`/`MAX`/`LATEST`/`DELTA`. Missing references are handled per-spec via `SKIP_RULE` / `REJECT` / `QUARANTINE`.
- **Full Processing Result** (v1.1+): `Process` (via `NewEnergyDataProcessor`) returns `ProcessingResult{Accepted, Rejected, Evaluations}` including per-rule evaluations (`PASS`/`REJECT`/`SKIP`/`CORRECT`/`QUARANTINE`) with the referenced object IDs and reasons.
- **Downstream Output** (v1.1+): the application layer (`ProcessingPipeline` + `ResultSink`s — `MemorySink`, `CallbackSink`, `RepositorySink`, `QuarantineSink`) persists `Accepted`/`Rejected` and receives the full result after every run; delivery failures are aggregated by sink ID and never silently dropped.
- **Multi-Device Isolation**: stateful rules keep a **per-device** "last valid reading", so interleaved devices never pollute each other's context, and the caller's input slice is never mutated in place.
- **Hexagonal Architecture**:
  - **Domain**: Pure business logic & entities (`pkg/core/domain`) — `Reading`, `StandardReading`, `ReferenceSpec`, `ProcessingResult`, `Aligner`, `IngestContext`.
  - **Ports**: Interface definitions (`pkg/core/ports`) — `CleaningRule`, `ReferenceCleaningRule`, `Sanitizer`/`ReferenceSanitizer`, `EnergyDataStandardizer`/`EnergyDataProcessor`, `ReferenceSource`, `ResultSink`, repositories, `CleaningRuleFactory`.
  - **Services**: Orchestration layer (`pkg/core/services`) — `CoreStandardizer`, `ChainSanitizer`, built-in rules, `BatchReferenceSource`/`ReferenceResolver`.
  - **Adapters**: Infrastructure (`pkg/adapters`) — JSON/CSV ingestors, `RuleFactory`, `RepositoryReferenceSource`, result sinks.

## 🎬 快速体验 (Demo)

无需编写代码即可感受完整流水线 (接入 → 清洗 → 标准化 → 查询)：

```bash
go run ./cmd/demo              # 脚本化演示 (内置脏数据样例)
go run ./cmd/demo -ingest data.json   # 自喂数据 (.json / .csv)
```

## 📂 Project Structure

```
prism-core/
├── cmd/
│   └── demo/           # Demo program (go run ./cmd/demo)
├── docs/
│   └── dev-guide/      # Developer handbook (hexagonal architecture)
├── pkg/
│   ├── adapters/       # Infrastructure layer
│   │   ├── factory/    #   RuleFactory (rule instantiation)
│   │   ├── ingest/     #   JSON / CSV ingestors
│   │   ├── reference/  #   RepositoryReferenceSource (historical reference queries)
│   │   └── sink/       #   Result sinks (Memory / Callback / Repository)
│   └── core/
│       ├── domain/     # Pure business logic (entities & algorithms)
│       │   ├── aligner.go    # Time alignment
│       │   ├── reference.go  # ReferenceSpec / ReferenceRequest / ReferenceValue
│       │   ├── result.go     # ProcessingResult / RuleEvaluation
│       │   └── ...
│       ├── ports/      # Interface definitions (driver / driven ports)
│       └── services/   # Application services (orchestration + built-in rules)
│           ├── reference_source.go  # BatchReferenceSource + ReferenceResolver
│           └── rules/  #   Range / Monotonic / Rate / Stagnation
├── tests/              # External tests (packages are xxx_test)
└── testdata/           # Test sample data
```

## 🚀 Getting Started

### Installation

```bash
go get github.com/RenJieWeia/prism-core
```

### Usage Example

```go
package main

import (
    "context"
    "fmt"
    "strings"
    "time"

    // Import from the public package path
    "github.com/RenJieWeia/prism-core/pkg/adapters/ingest"
    "github.com/RenJieWeia/prism-core/pkg/core/domain"
    "github.com/RenJieWeia/prism-core/pkg/core/services"
)

func main() {
    // 1. Setup Standardization Service
    // 15-minute alignment, 4-decimal precision (x10000)
    // NewCoreStandardizer -> ports.EnergyDataStandardizer (ProcessAndStandardize)
    // NewEnergyDataProcessor -> ports.EnergyDataProcessor (adds Process, full result)
    standardizer := services.NewCoreStandardizer(
        services.WithAlignment(15*time.Minute, 5*time.Minute),
        services.WithPrecision(10000),
    )

    // 2. Process raw readings -> standardized grid output
    ctx := context.Background()
    raw := []domain.Reading{
        {DeviceInfo: domain.DeviceInfo{ID: "D1", Type: domain.DeviceTypeElec},
         Timestamp: time.Now().Truncate(15 * time.Minute), Value: 100.0},
        {DeviceInfo: domain.DeviceInfo{ID: "D1", Type: domain.DeviceTypeElec},
         Timestamp: time.Now().Truncate(15 * time.Minute).Add(15 * time.Minute), Value: 105.0},
    }
    results, err := standardizer.ProcessAndStandardize(ctx, raw)
    if err != nil {
        panic(err)
    }
    for _, r := range results {
        fmt.Printf("%s ValueScaled=%d x%d\n", r.DeviceID, r.ValueScaled, r.ScaleFactor)
    }

    // 3. Or ingest from a stream (JSON array/object) via the universal ingestor
    ing := ingest.NewJsonUniversalIngestor(func(ctx context.Context, rs []domain.Reading) error {
        _, err := standardizer.ProcessAndStandardize(ctx, rs)
        return err
    })
    ing.IngestStream(ctx, strings.NewReader(`[{"device_id":"D1","timestamp":"2026-01-01T10:00:00Z","value":100}]`))
}
```

### Running Tests

```bash
gofmt -w .
go test ./...
go vet ./...
# Race detector (requires gcc/clang, i.e. CGO_ENABLED=1)
CGO_ENABLED=1 go test -race ./...
```

## 🛠 Architecture

### domain.Sanitizer
The `Standardizer` service cleans incoming raw data using a chain of injected rules.

```go
// Define custom rules implementing ports.CleaningRule
type MaxLimitRule struct{ limit float64 }

func (r *MaxLimitRule) Check(ctx ports.CleaningContext, curr domain.Reading) ports.CheckResult {
    if curr.Value > r.limit {
        return ports.CheckResult{
            Reading: curr,
            Passed:  false,
            Reason:  fmt.Sprintf("value %.2f exceeded limit %.2f", curr.Value, r.limit),
        }
    }
    return ports.CheckResult{Reading: curr, Passed: true}
}

// Injected via functional options
svc := services.NewCoreStandardizer(services.WithCleaningRules(&MaxLimitRule{100}))
```

For **dynamic rule loading** (rules configured per device type in a repository), you must inject both a rule repository and a rule factory:

```go
import "github.com/RenJieWeia/prism-core/pkg/adapters/factory" // concrete factory (infrastructure layer)

svc := services.NewCoreStandardizer(
    services.WithRuleRepository(ruleRepo),   // ports.CleaningRuleRepository
    services.WithRuleFactory(factory.GetRuleFactory()), // ports.CleaningRuleFactory
)
```
The `CoreStandardizer` core layer depends only on the `ports.CleaningRuleFactory` interface, never on `pkg/adapters` directly.

### Precision Conversion
The `CoreStandardizer` handles the conversion between "Human Readable" floats and "Machine Precise" integers automatically.

```go
// Default: 4 decimal places precision (x10000)
// 100.00019 * 10000 = 1000001 (int64 truncation)
standardizer := services.NewCoreStandardizer()
results, _ := standardizer.ProcessAndStandardize(ctx, readings)
// results[0].ValueScaled = 1000001
// results[0].ScaleFactor = 10000
// results[0].ValueDisplay = 100.00019 (original preserved)
```

### Aligner (Time Alignment)
Uses **binary search** (O(log n)) to find the nearest reading within tolerance:

```go
aligner := domain.NewAligner(5 * time.Minute) // 5-minute tolerance
snapshot := aligner.FindSnapshot(sortedReadings, targetTime)
```

## 🎯 Reference Objects & Full Processing Result

Rules can compare the current reading against reference data instead of only the previous reading. Implement `ports.ReferenceCleaningRule` and declare what data the rule needs via `domain.ReferenceSpec`:

```go
import "github.com/RenJieWeia/prism-core/pkg/core/domain"

// spec: 同一设备三天前的值 (以当前读数 Timestamp 为基准, 不使用 time.Now())
spec := domain.ReferenceSpec{
    ID:      "d3",
    Source:  domain.ReferenceSourceStandardRepo, // 或 ReferenceSourceCurrentBatch
    Binding: domain.ReferenceBindingSameDevice,  // 或 EXPLICIT + DeviceID
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

Time selector supports `PREVIOUS` (same device's last **valid** reading), `RELATIVE` (current `Timestamp - Offset`) and `WINDOW` (aggregated over `[Timestamp-Offset, Timestamp)` with `AVG`/`SUM`/`MIN`/`MAX`/`LATEST`/`DELTA`).

Wire the rule, an optional historical reference source, and get the full result:

```go
import (
    "github.com/RenJieWeia/prism-core/pkg/adapters/reference"
    "github.com/RenJieWeia/prism-core/pkg/adapters/sink"
    "github.com/RenJieWeia/prism-core/pkg/application/pipeline"
    "github.com/RenJieWeia/prism-core/pkg/core/services"
)

// Core processor: cleaning + reference resolution + standardization only.
proc := services.NewEnergyDataProcessor(
    services.WithReferenceRules(&RefGeRule{Spec: spec}),           // ports.ReferenceCleaningRule
    services.WithReferenceSource(reference.NewRepositoryReferenceSource(repo, time.Minute)), // STANDARD_REPO source
)

// Application pipeline: persistence (Accepted/Rejected) + downstream sinks.
pl := pipeline.NewProcessingPipeline(
    proc,
    sink.NewRepositorySink(repo, ports.UpsertStrategyHighPriorityWins), // Accepted
    sink.NewQuarantineSink(quarantineRepo),                             // Rejected
    sink.NewMemorySink(),                                               // optional other sinks
)

result, err := pl.Execute(ctx, rawReadings)
// result.Accepted    []domain.StandardReading
// result.Rejected    []domain.QuarantineReading
// result.Evaluations []domain.RuleEvaluation  // RuleID / Outcome / Reason / ReferenceIDs
```

Notes:

- `ReferenceSource` is the query port (`Resolve(ctx, requests)`); rules never touch the database directly. Built-ins: `BatchReferenceSource` (current batch) and `RepositoryReferenceSource` (historical `StandardReadingRepository`), with per-run in-memory dedup of identical requests.
- A `STANDARD_REPO` spec without a configured source, or a negative `RELATIVE`/`WINDOW` `Offset`, fails fast with a clear config error — reference rules never silently degrade.
- `SKIP_RULE`/`REJECT`/`QUARANTINE` are applied **only** when a reference is genuinely not found.
- Persistence and sink delivery live in the application layer: `ProcessingPipeline` + `RepositorySink` (Accepted) / `QuarantineSink` (Rejected) + extra sinks. `WithRepository` only feeds `GetStandardReading` queries and no longer writes during `Process`.
- The `pkg/adapters/reference`, `pkg/adapters/sink` and `pkg/application/pipeline` packages implement the infrastructure, keeping `pkg/core` free of adapter imports.

## 📄 License
MIT
