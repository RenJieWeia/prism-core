package factory_test

import (
	"testing"

	"github.com/RenJieWeia/prism-core/pkg/adapters/factory"
	"github.com/RenJieWeia/prism-core/pkg/core/domain"
	"github.com/RenJieWeia/prism-core/pkg/core/ports"
)

func TestRuleFactoryCreate(t *testing.T) {
	f := factory.NewRuleFactory()

	t.Run("RANGE", func(t *testing.T) {
		rule, err := f.CreateRule(domain.CleaningRule{
			Type:       domain.RuleTypeRange,
			Parameters: map[string]interface{}{"min": float64(0), "max": float64(100)},
		})
		if err != nil {
			t.Fatalf("create RANGE failed: %v", err)
		}
		if rule == nil {
			t.Fatal("nil rule")
		}
	})

	t.Run("MONOTONIC", func(t *testing.T) {
		rule, err := f.CreateRule(domain.CleaningRule{Type: domain.RuleTypeMonotonic})
		if err != nil {
			t.Fatalf("create MONOTONIC failed: %v", err)
		}
		_ = rule
	})

	t.Run("RATE", func(t *testing.T) {
		rule, err := f.CreateRule(domain.CleaningRule{
			Type:       domain.RuleTypeRate,
			Parameters: map[string]interface{}{"max_rate_per_second": float64(1)},
		})
		if err != nil {
			t.Fatalf("create RATE failed: %v", err)
		}
		_ = rule
	})

	t.Run("RATE 缺参数报错", func(t *testing.T) {
		if _, err := f.CreateRule(domain.CleaningRule{Type: domain.RuleTypeRate}); err == nil {
			t.Fatalf("expected error for RATE without params")
		}
	})

	t.Run("STAGNATION", func(t *testing.T) {
		rule, err := f.CreateRule(domain.CleaningRule{Type: domain.RuleTypeStagnation})
		if err != nil {
			t.Fatalf("create STAGNATION failed: %v", err)
		}
		_ = rule
	})

	t.Run("未知类型报错", func(t *testing.T) {
		if _, err := f.CreateRule(domain.CleaningRule{Type: "UNKNOWN_TYPE"}); err == nil {
			t.Fatalf("expected error for unknown rule type")
		}
	})
}

func TestRuleFactoryIsolation(t *testing.T) {
	// 独立实例互不影响
	f1 := factory.NewRuleFactory()
	f2 := factory.NewRuleFactory()
	f1.Register("CUSTOM", func(params map[string]interface{}, action domain.RuleAction) (ports.CleaningRule, error) {
		return nil, nil
	})
	if _, err := f2.CreateRule(domain.CleaningRule{Type: "CUSTOM"}); err == nil {
		t.Fatalf("expected f2 to not see custom rule registered on f1")
	}
}

func TestGetRuleFactorySingleton(t *testing.T) {
	// 单例: 两次获取应为同一实例
	if factory.GetRuleFactory() != factory.GetRuleFactory() {
		t.Fatalf("GetRuleFactory should return the same singleton instance")
	}
	// 单例包含内置规则
	if _, err := factory.GetRuleFactory().CreateRule(domain.CleaningRule{
		Type:       domain.RuleTypeRange,
		Parameters: map[string]interface{}{"min": float64(0), "max": float64(100)},
	}); err != nil {
		t.Fatalf("singleton should have built-in rules: %v", err)
	}
}
