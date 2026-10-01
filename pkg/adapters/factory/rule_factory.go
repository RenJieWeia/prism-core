package factory

import (
	"fmt"
	"sync"

	"github.com/RenJieWeia/prism-core/pkg/core/domain"
	"github.com/RenJieWeia/prism-core/pkg/core/ports"
	"github.com/RenJieWeia/prism-core/pkg/core/services/rules"
)

// RuleBuilder defines the contract for creating a specific rule logic
type RuleBuilder func(params map[string]interface{}, action domain.RuleAction) (ports.CleaningRule, error)

// RuleFactory is the registry for all available rule types
type RuleFactory struct {
	builders map[domain.RuleType]RuleBuilder
	mu       sync.RWMutex
}

var (
	instance *RuleFactory
	once     sync.Once
)

// GetRuleFactory returns the singleton instance
func GetRuleFactory() *RuleFactory {
	once.Do(func() {
		instance = NewRuleFactory()
	})
	return instance
}

// NewRuleFactory creates a new RuleFactory instance with built-in rules registered
// This constructor is useful for testing where you need isolated factory instances
func NewRuleFactory() *RuleFactory {
	f := &RuleFactory{
		builders: make(map[domain.RuleType]RuleBuilder),
	}
	// Register built-in rules
	f.Register(domain.RuleTypeRange, buildRangeRule)
	f.Register(domain.RuleTypeMonotonic, buildMonotonicRule)
	f.Register(domain.RuleTypeRate, buildRateRule)
	f.Register(domain.RuleTypeStagnation, buildStagnationRule)
	return f
}

// Register adds or overrides a rule builder
func (f *RuleFactory) Register(ruleType domain.RuleType, builder RuleBuilder) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.builders[ruleType] = builder
}

// CreateRule instantiates a rule strategy based on configuration
func (f *RuleFactory) CreateRule(rule domain.CleaningRule) (ports.CleaningRule, error) {
	f.mu.RLock()
	builder, ok := f.builders[rule.Type]
	f.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("no builder registered for rule type: %s", rule.Type)
	}
	return builder(rule.Parameters, rule.Action)
}

// buildRangeRule (Built-in implementation)
func buildRangeRule(params map[string]interface{}, action domain.RuleAction) (ports.CleaningRule, error) {
	min, ok1 := params["min"].(float64)
	max, ok2 := params["max"].(float64)
	if !ok1 || !ok2 {
		return nil, fmt.Errorf("invalid parameters for RANGE rule: need min(float) and max(float)")
	}

	if action == "" {
		action = domain.ActionReject
	}
	return &rules.RangeRule{Min: min, Max: max, Action: action}, nil
}

// buildMonotonicRule (Built-in implementation)
func buildMonotonicRule(params map[string]interface{}, action domain.RuleAction) (ports.CleaningRule, error) {
	if action == "" {
		action = domain.ActionReject
	}
	return &rules.MonotonicRule{Action: action}, nil
}

// buildRateRule (Built-in implementation)
func buildRateRule(params map[string]interface{}, action domain.RuleAction) (ports.CleaningRule, error) {
	maxRate, ok := params["max_rate_per_second"].(float64)
	if !ok || maxRate <= 0 {
		return nil, fmt.Errorf("invalid parameters for RATE rule: need max_rate_per_second(float) > 0")
	}
	if action == "" {
		action = domain.ActionReject
	}
	return &rules.RateRule{MaxRatePerSecond: maxRate, Action: action}, nil
}

// buildStagnationRule (Built-in implementation)
func buildStagnationRule(params map[string]interface{}, action domain.RuleAction) (ports.CleaningRule, error) {
	if action == "" {
		action = domain.ActionReject
	}
	return &rules.StagnationRule{Action: action}, nil
}
