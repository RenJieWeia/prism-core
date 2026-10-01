package ports

import "github.com/RenJieWeia/prism-core/pkg/core/domain"

// CleaningRuleFactory 清洗规则工厂接口
// 职责：根据规则配置（domain.CleaningRule）实例化可执行的清洗规则。
// 由基础设施层（如 pkg/adapters/factory.RuleFactory）实现并注入，
// 从而保证核心服务层（pkg/core/services）不反向依赖适配器层。
type CleaningRuleFactory interface {
	// CreateRule 根据规则配置创建具体的清洗规则实例
	CreateRule(rule domain.CleaningRule) (CleaningRule, error)
}
