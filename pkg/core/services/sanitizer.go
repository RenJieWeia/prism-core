package services

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/RenJieWeia/prism-core/pkg/core/domain"
	"github.com/RenJieWeia/prism-core/pkg/core/ports"
)

// ChainSanitizer 基于责任链模式的清洗器实现
// 内部规则列表允许混合旧规则 (ports.CleaningRule) 与参考规则 (ports.ReferenceCleaningRule)。
type ChainSanitizer struct {
	rules []any
}

// NewSanitizer 创建默认的基于规则链的清洗器 (仅支持旧规则)
func NewSanitizer(rules ...ports.CleaningRule) ports.Sanitizer {
	return newChainSanitizer(rules, nil)
}

// NewSanitizerWithReferences 创建支持参考对象的清洗器 (仅支持参考规则)
// 旧规则与参考规则的组合由 CoreStandardizer 内部通过 newChainSanitizer 完成。
func NewSanitizerWithReferences(rules ...ports.ReferenceCleaningRule) ports.ReferenceSanitizer {
	return newChainSanitizer(nil, rules)
}

// newChainSanitizer 内部构造器: 组合旧规则与参考规则 (调用方不传任意类型)
func newChainSanitizer(legacy []ports.CleaningRule, refs []ports.ReferenceCleaningRule) *ChainSanitizer {
	rules := make([]any, 0, len(legacy)+len(refs))
	for _, r := range legacy {
		rules = append(rules, r)
	}
	for _, r := range refs {
		rules = append(rules, r)
	}
	return &ChainSanitizer{rules: rules}
}

// Clean 实现 ports.Sanitizer 接口
// 返回的 clean 数据已按时间戳升序排列。
// 该方法仅供旧规则使用; 含参考规则的清洗器必须使用 CleanWithReferences，
// 否则参考源配置错误会被吞掉 (Clean 无错误返回)。
// context.TODO() 仅为旧的无上下文 API 提供的占位，不参与任何持久化/投递。
func (s *ChainSanitizer) Clean(readings []domain.Reading) ([]domain.Reading, []domain.QuarantineReading) {
	res, _ := s.CleanWithReferences(context.TODO(), readings, nil)
	return res.Clean, res.Quarantined
}

// CleanWithReferences 实现 ports.Sanitizer 接口
// repoRefs 为历史仓储参考源 (可为 nil)。
func (s *ChainSanitizer) CleanWithReferences(ctx context.Context, readings []domain.Reading, repoRefs ports.ReferenceSource) (ports.CleanResult, error) {
	return s.cleanInternal(ctx, readings, repoRefs)
}

// cleanInternal 核心清洗逻辑:
//  1. 拷贝 + 时间排序，绝不原地修改调用方传入的数据切片
//  2. 按设备分别维护上一条有效数据，避免跨设备状态污染
//  3. 规则链执行 (旧规则走 Previous 上下文，参考规则走参考对象解析)
//  4. 全程记录规则评估
func (s *ChainSanitizer) cleanInternal(ctx context.Context, readings []domain.Reading, repoRefs ports.ReferenceSource) (ports.CleanResult, error) {
	res := ports.CleanResult{}
	if len(readings) == 0 {
		return res, nil
	}

	// 0. 配置校验: 参考源缺失 / Offset 非法必须在处理前报错，不得静默退化
	if err := validateRulesForReferences(s.rules, repoRefs); err != nil {
		return res, err
	}

	// 1. 拷贝 + 时间排序 (不修改调用方切片)
	sorted := make([]domain.Reading, len(readings))
	copy(sorted, readings)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Timestamp.Before(sorted[j].Timestamp)
	})

	// 2. 参考解析器: 批次源始终可用, 仓储源可选
	resolver := NewReferenceResolver(sorted, repoRefs)

	// 3. 按设备维护上一条有效数据
	prevByDevice := make(map[string]*domain.Reading)

	for _, curr := range sorted {
		deviceID := curr.DeviceInfo.ID
		prev := prevByDevice[deviceID]

		// 0. 内置规则: 同设备下的时间戳去重
		if prev != nil && prev.Timestamp.Equal(curr.Timestamp) {
			res.Quarantined = append(res.Quarantined, domain.QuarantineReading{
				Reading:   curr,
				Status:    domain.QuarantineStatusPending,
				Reason:    "Duplicate timestamp",
				CreatedAt: time.Now(),
			})
			continue
		}

		tempReading := curr
		passed := true
		failReason := ""

	ruleLoop:
		for _, rule := range s.rules {
			eval, next, verdict, err := s.executeRule(ctx, rule, tempReading, prev, resolver)
			if err != nil {
				return res, err
			}
			res.Evaluations = append(res.Evaluations, eval)

			switch verdict {
			case verdictPass:
				tempReading = next
			case verdictSkip:
				// 跳过该规则，继续下一个规则
				continue
			default:
				// 拒绝 / 隔离
				passed = false
				failReason = eval.Reason
				break ruleLoop
			}
		}

		if passed {
			res.Clean = append(res.Clean, tempReading)
			// 保存本设备上一条有效数据的本地副本 (避免指向动态扩容的切片元素)
			last := res.Clean[len(res.Clean)-1]
			prevByDevice[deviceID] = &last
		} else {
			res.Quarantined = append(res.Quarantined, domain.QuarantineReading{
				Reading:   curr,
				Status:    domain.QuarantineStatusPending,
				Reason:    failReason,
				CreatedAt: time.Now(),
			})
		}
	}

	return res, nil
}

// validateRulesForReferences 参考规则配置校验 (处理前 fail-fast)
//  1. 规则声明 STANDARD_REPO 参考对象但没有可用仓储参考源 -> 配置错误
//  2. RELATIVE/WINDOW 模式的 Offset 为负 -> 配置错误
//
// 仅当校验通过后，参考缺失策略 (SKIP_RULE/REJECT/QUARANTINE) 才会因"查无数据"而触发。
func validateRulesForReferences(rules []any, repoRefs ports.ReferenceSource) error {
	for _, rule := range rules {
		rr, ok := rule.(ports.ReferenceCleaningRule)
		if !ok {
			continue
		}
		for _, spec := range rr.ReferenceSpecs() {
			if spec.Source == domain.ReferenceSourceStandardRepo && repoRefs == nil {
				return fmt.Errorf(
					"reference rule %q declares spec %q with STANDARD_REPO source, but no repository reference source is configured",
					rr.RuleID(), spec.ID)
			}
			if (spec.Time.Mode == domain.ReferenceTimeRelative ||
				spec.Time.Mode == domain.ReferenceTimeWindow) && spec.Time.Offset < 0 {
				return fmt.Errorf(
					"reference rule %q spec %q: negative Offset %v is not allowed (Offset must be non-negative, target = current.Timestamp - Offset)",
					rr.RuleID(), spec.ID, spec.Time.Offset)
			}
		}
	}
	return nil
}

// ruleVerdict 单条规则的执行判定结果
type ruleVerdict int

const (
	verdictPass       ruleVerdict = iota // 通过
	verdictSkip                          // 跳过 (缺少参考对象且策略为 SKIP_RULE)
	verdictFail                          // 拒绝
	verdictQuarantine                    // 隔离 (缺少参考对象且策略为 QUARANTINE)
)

// executeRule 执行单条规则，返回评估记录与判定结果
func (s *ChainSanitizer) executeRule(ctx context.Context, rule any, curr domain.Reading, prev *domain.Reading, resolver *ReferenceResolver) (domain.RuleEvaluation, domain.Reading, ruleVerdict, error) {
	if rr, ok := rule.(ports.ReferenceCleaningRule); ok {
		return s.executeReferenceRule(ctx, rr, curr, prev, resolver)
	}

	legacy, ok := rule.(ports.CleaningRule)
	if !ok {
		return domain.RuleEvaluation{}, curr, verdictFail, fmt.Errorf("unsupported rule type: %T", rule)
	}

	result := legacy.Check(ports.CleaningContext{Previous: prev}, curr)
	eval := domain.RuleEvaluation{
		RuleID:  fmt.Sprintf("%T", legacy),
		Outcome: outcomeFromCheckResult(result),
		Reason:  result.Reason,
	}
	if !result.Passed {
		return eval, curr, verdictFail, nil
	}
	return eval, result.Reading, verdictPass, nil
}

// executeReferenceRule 执行参考规则:
// 解析规则声明的 ReferenceSpec -> 提交参考查询 -> 处理缺失策略 -> 调用规则检查
func (s *ChainSanitizer) executeReferenceRule(ctx context.Context, rr ports.ReferenceCleaningRule, curr domain.Reading, prev *domain.Reading, resolver *ReferenceResolver) (domain.RuleEvaluation, domain.Reading, ruleVerdict, error) {
	specs := rr.ReferenceSpecs()

	refs := make(map[string]domain.ReferenceValue, len(specs))
	refIDs := make([]string, 0, len(specs))
	requests := make([]domain.ReferenceRequest, 0, len(specs))
	for _, spec := range specs {
		refIDs = append(refIDs, spec.ID)

		// 当前批次 + 同设备 + 上一条: 直接使用清洗器维护的本设备上一条有效数据，
		// 不得从包含被拒绝数据的原始批次中选择 PREVIOUS。
		if spec.Source == domain.ReferenceSourceCurrentBatch &&
			spec.Binding == domain.ReferenceBindingSameDevice &&
			spec.Time.Mode == domain.ReferenceTimePrevious {
			refs[spec.ID] = prevReferenceValue(prev)
			continue
		}

		req, ok := buildReferenceRequest(spec, curr)
		if !ok {
			continue
		}
		requests = append(requests, req)
	}

	if len(requests) > 0 {
		resolved, err := resolver.Resolve(ctx, requests)
		if err != nil {
			return domain.RuleEvaluation{}, curr, verdictFail, err
		}
		for id, v := range resolved {
			refs[id] = v
		}
	}

	// 缺失参考对象处理
	for _, spec := range specs {
		ref, ok := refs[spec.ID]
		if ok && ref.Found {
			continue
		}
		reason := fmt.Sprintf("reference %q missing", spec.ID)
		switch spec.MissingPolicy {
		case domain.MissingReferenceSkip:
			return domain.RuleEvaluation{
				RuleID:       rr.RuleID(),
				Outcome:      string(domain.RuleOutcomeSkip),
				Reason:       reason,
				ReferenceIDs: refIDs,
			}, curr, verdictSkip, nil

		case domain.MissingReferenceQuarantine:
			return domain.RuleEvaluation{
				RuleID:       rr.RuleID(),
				Outcome:      string(domain.RuleOutcomeQuarantine),
				Reason:       reason,
				ReferenceIDs: refIDs,
			}, curr, verdictQuarantine, nil

		default: // REJECT (默认安全策略)
			return domain.RuleEvaluation{
				RuleID:       rr.RuleID(),
				Outcome:      string(domain.RuleOutcomeReject),
				Reason:       reason,
				ReferenceIDs: refIDs,
			}, curr, verdictFail, nil
		}
	}

	// 全部参考数据就绪，执行规则
	result := rr.CheckWithReferences(domain.RuleInput{
		Current:    curr,
		Previous:   prev,
		References: refs,
	})
	eval := domain.RuleEvaluation{
		RuleID:       rr.RuleID(),
		Outcome:      outcomeFromCheckResult(result),
		Reason:       result.Reason,
		ReferenceIDs: refIDs,
	}
	if !result.Passed {
		return eval, curr, verdictFail, nil
	}
	return eval, result.Reading, verdictPass, nil
}

// prevReferenceValue 将清洗器维护的"本设备上一条有效数据"转换为参考值
func prevReferenceValue(prev *domain.Reading) domain.ReferenceValue {
	if prev == nil {
		return domain.ReferenceValue{}
	}
	return domain.ReferenceValue{
		Found:     true,
		Timestamp: prev.Timestamp,
		Value:     prev.Value,
	}
}

// outcomeFromCheckResult 将 CheckResult 映射为评估结果类型
func outcomeFromCheckResult(result ports.CheckResult) string {
	if !result.Passed {
		return string(domain.RuleOutcomeReject)
	}
	if result.Corrected {
		return string(domain.RuleOutcomeCorrect)
	}
	return string(domain.RuleOutcomePass)
}
