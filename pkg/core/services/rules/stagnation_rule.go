package rules

import (
	"fmt"

	"github.com/RenJieWeia/prism-core/pkg/core/domain"
	"github.com/RenJieWeia/prism-core/pkg/core/ports"
)

// StagnationRule 停滞检查规则 (死传感器检测)
// 业务背景: 传感器长时间读数不变 (完全相同) 通常意味着设备故障、断线或冻结。
// 注意: 该规则基于相邻两条读数的比较，连续出现相同值即判定为停滞。
type StagnationRule struct {
	Action domain.RuleAction // REJECT (默认) 或 CORRECT/FLAG_ONLY
}

// Check 检查读数是否与上一条完全相同 (停滞)
func (r *StagnationRule) Check(ctx ports.CleaningContext, curr domain.Reading) ports.CheckResult {
	// 第一条数据无法判断，默认通过
	if ctx.Previous == nil {
		return ports.CheckResult{Reading: curr, Passed: true, Corrected: false}
	}

	if curr.Value != ctx.Previous.Value {
		return ports.CheckResult{Reading: curr, Passed: true, Corrected: false}
	}

	// 值与上一条完全相同 -> 停滞
	switch r.Action {
	case domain.ActionCorrect, domain.ActionFlagOnly:
		// 保留原值，但标记为修正/带警告，交由后续质量评估
		return ports.CheckResult{
			Reading:   curr,
			Passed:    true,
			Corrected: true,
			Reason:    fmt.Sprintf("stagnation detected: value %.4f unchanged since previous reading", curr.Value),
		}
	default:
		// 默认 REJECT
		return ports.CheckResult{
			Reading:   curr,
			Passed:    false,
			Corrected: false,
			Reason:    fmt.Sprintf("stagnation detected: dead sensor, value %.4f unchanged", curr.Value),
		}
	}
}
