package rules

import (
	"fmt"

	"github.com/RenJieWeia/prism-core/pkg/core/domain"
	"github.com/RenJieWeia/prism-core/pkg/core/ports"
)

// RangeRule 实现数值范围检查
type RangeRule struct {
	Min    float64
	Max    float64
	Action domain.RuleAction
}

// Check 检查读数是否在范围内
// 返回 CheckResult 包含完整的检查结果信息
func (r *RangeRule) Check(ctx ports.CleaningContext, curr domain.Reading) ports.CheckResult {
	if curr.Value >= r.Min && curr.Value <= r.Max {
		return ports.CheckResult{
			Reading:   curr,
			Passed:    true,
			Corrected: false,
			Reason:    "",
		}
	}

	// 触发规则: 超出范围
	switch r.Action {
	case domain.ActionCorrect:
		// 修正策略: 截断 (Clamp)
		correctedReading := curr
		var reason string
		if curr.Value < r.Min {
			correctedReading.Value = r.Min
			reason = fmt.Sprintf("value %.2f corrected to min %.2f", curr.Value, r.Min)
		} else {
			correctedReading.Value = r.Max
			reason = fmt.Sprintf("value %.2f corrected to max %.2f", curr.Value, r.Max)
		}
		return ports.CheckResult{
			Reading:   correctedReading,
			Passed:    true, // 修正后仍然通过
			Corrected: true,
			Reason:    reason,
		}

	case domain.ActionReject:
		fallthrough
	default:
		return ports.CheckResult{
			Reading:   curr,
			Passed:    false,
			Corrected: false,
			Reason:    fmt.Sprintf("value %.2f out of range [%.2f, %.2f]", curr.Value, r.Min, r.Max),
		}
	}
}
