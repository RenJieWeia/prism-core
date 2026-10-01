package ports

import (
	"context"

	"github.com/RenJieWeia/prism-core/pkg/core/domain"
)

// ReferenceSource 参考数据查询端口
// 规则本身不得直接查询数据库，必须通过本端口获取参考数据。
// Resolve 一次可提交多条请求并批量返回，实现方负责去重与批量查询。
type ReferenceSource interface {
	Resolve(
		ctx context.Context,
		requests []domain.ReferenceRequest,
	) (map[string]domain.ReferenceValue, error)
}

// ReferenceCleaningRule 参考规则接口
// 规则声明自己需要哪些参考对象，并在 CheckWithReferences 中消费解析后的参考数据。
// 该接口是 CleaningRule 的扩展能力，旧的 CleaningRule 接口保持不变。
type ReferenceCleaningRule interface {
	// RuleID 规则标识，用于评估记录
	RuleID() string

	// ReferenceSpecs 声明规则需要的参考对象定义
	ReferenceSpecs() []domain.ReferenceSpec

	// CheckWithReferences 携带参考数据执行检查
	CheckWithReferences(input domain.RuleInput) CheckResult
}
