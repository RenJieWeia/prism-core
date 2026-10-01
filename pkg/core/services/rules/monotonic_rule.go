package rules

import (
	"fmt"

	"github.com/RenJieWeia/prism-core/pkg/core/domain"
	"github.com/RenJieWeia/prism-core/pkg/core/ports"
)

// MonotonicRule 单调性检查规则
// 业务背景: 电表/水表的累计读数只增不减，出现回退即异常。
// (设备换表归零属于特例，通常由 RATE 或专门的 Rollover 逻辑处理)
type MonotonicRule struct {
	Action domain.RuleAction // REJECT (默认) 或 CORRECT
}

// Check 检查读数是否发生回退 (curr.Value < prev.Value)
func (r *MonotonicRule) Check(ctx ports.CleaningContext, curr domain.Reading) ports.CheckResult {
	// 第一条数据无法判断趋势，默认通过
	if ctx.Previous == nil {
		return ports.CheckResult{Reading: curr, Passed: true, Corrected: false}
	}

	if curr.Value >= ctx.Previous.Value {
		return ports.CheckResult{Reading: curr, Passed: true, Corrected: false}
	}

	// 发生回退
	if r.Action == domain.ActionCorrect {
		// 修正策略: 钳制到上一次的值 (保持单调)
		fixed := curr
		fixed.Value = ctx.Previous.Value
		return ports.CheckResult{
			Reading:   fixed,
			Passed:    true,
			Corrected: true,
			Reason:    fmt.Sprintf("value regression corrected from %.4f to %.4f", curr.Value, ctx.Previous.Value),
		}
	}

	// 默认 REJECT
	return ports.CheckResult{
		Reading:   curr,
		Passed:    false,
		Corrected: false,
		Reason:    fmt.Sprintf("value regression detected: %.4f < %.4f", curr.Value, ctx.Previous.Value),
	}
}
