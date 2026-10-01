package ports

import (
	"context"

	"github.com/RenJieWeia/prism-core/pkg/core/domain"
)

// CleaningContext 清洗规则执行时的上下文信息
type CleaningContext struct {
	Previous *domain.Reading // 前一条读数 (可能为nil，表示第一条数据)
	// 可扩展其他上下文字段，如批次信息、设备元数据等
}

// CheckResult 清洗规则检查的结果
type CheckResult struct {
	Reading   domain.Reading // 结果读数 (可能是原值或修正后的值)
	Passed    bool           // 是否通过检查
	Corrected bool           // 是否进行了修正
	Reason    string         // 失败或修正的原因描述
}

// CleaningRule 清洗规则接口
// 这是一个策略接口，具体的业务规则（如单调性、跳变检测）由外部实现注入
type CleaningRule interface {
	// Check 检查当前读数是否满足规则
	// 返回 CheckResult 包含完整的检查结果信息
	Check(ctx CleaningContext, curr domain.Reading) CheckResult
}

// CleanResult 清洗结果 (含规则评估记录)
type CleanResult struct {
	Clean       []domain.Reading
	Quarantined []domain.QuarantineReading
	Evaluations []domain.RuleEvaluation
}

// Sanitizer 数据清洗器接口
// 负责协调多个清洗规则的执行
type Sanitizer interface {
	// Clean 执行清洗逻辑，返回:
	// 1. clean: 通过规则的良品数据 (可能经过修正)
	// 2. quarantined: 违反规则被拒绝的次品数据 (包含拒绝原因)
	// 注意: 返回的 clean 数据已按时间戳升序排列
	Clean(readings []domain.Reading) (clean []domain.Reading, quarantined []domain.QuarantineReading)
}

// ReferenceSanitizer 支持参考对象的清洗器接口
// 不嵌入 Sanitizer，因此不会暴露会吞掉错误的 Clean()。
// ChainSanitizer 可同时实现 Sanitizer 与 ReferenceSanitizer；
// 含参考规则的清洗器必须通过本接口调用 CleanWithReferences。
type ReferenceSanitizer interface {
	// CleanWithReferences 带参考数据源的清洗逻辑
	// repoRefs 为历史仓储参考源 (可为 nil，此时 STANDARD_REPO 类型的参考请求会报错)。
	// 返回包含规则评估记录的完整清洗结果。
	CleanWithReferences(ctx context.Context, readings []domain.Reading, repoRefs ReferenceSource) (CleanResult, error)
}
