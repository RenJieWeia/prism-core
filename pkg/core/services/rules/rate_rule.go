package rules

import (
	"fmt"
	"math"

	"github.com/RenJieWeia/prism-core/pkg/core/domain"
	"github.com/RenJieWeia/prism-core/pkg/core/ports"
)

// RateRule 变化率检查规则 (对应 README 中的 JumpRule)
// 业务背景: 检测不可能出现的跳变 (Spike)。当单位时间内的变化量超过阈值时判定为异常。
// 参数 MaxRatePerSecond: 每秒允许的最大变化量 (例如累计表每秒最大涨 10)。
type RateRule struct {
	MaxRatePerSecond float64           // 每秒最大变化量 (必须 > 0)
	Action           domain.RuleAction // REJECT (默认) 或 CORRECT
}

// Check 检查当前读数相对上一条的变化率是否超限
func (r *RateRule) Check(ctx ports.CleaningContext, curr domain.Reading) ports.CheckResult {
	// 第一条数据无法计算变化率，默认通过
	if ctx.Previous == nil || r.MaxRatePerSecond <= 0 {
		return ports.CheckResult{Reading: curr, Passed: true, Corrected: false}
	}

	dt := curr.Timestamp.Sub(ctx.Previous.Timestamp).Seconds()
	// 时间相同或回拨时无法计算变化率，交给其他规则处理
	if dt <= 0 {
		return ports.CheckResult{Reading: curr, Passed: true, Corrected: false}
	}

	delta := math.Abs(curr.Value - ctx.Previous.Value)
	maxDelta := r.MaxRatePerSecond * dt
	if delta <= maxDelta {
		return ports.CheckResult{Reading: curr, Passed: true, Corrected: false}
	}

	// 变化率超限 (跳变)
	if r.Action == domain.ActionCorrect {
		// 修正策略: 钳制到允许的最大变化量 (保持趋势方向)
		fixed := curr
		if curr.Value > ctx.Previous.Value {
			fixed.Value = ctx.Previous.Value + maxDelta
		} else {
			fixed.Value = ctx.Previous.Value - maxDelta
		}
		return ports.CheckResult{
			Reading:   fixed,
			Passed:    true,
			Corrected: true,
			Reason:    fmt.Sprintf("rate spike corrected from %.4f to %.4f", curr.Value, fixed.Value),
		}
	}

	// 默认 REJECT
	return ports.CheckResult{
		Reading:   curr,
		Passed:    false,
		Corrected: false,
		Reason:    fmt.Sprintf("rate spike detected: delta %.4f exceeds max %.4f in %.2fs", delta, maxDelta, dt),
	}
}
