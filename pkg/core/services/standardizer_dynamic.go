package services

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/RenJieWeia/prism-core/pkg/core/domain"
	"github.com/RenJieWeia/prism-core/pkg/core/ports"
)

// cleanWithDynamicRules 根据设备类型动态加载规则进行清洗
func (s *CoreStandardizer) cleanWithDynamicRules(ctx context.Context, readings []domain.Reading) ([]domain.Reading, []domain.QuarantineReading, error) {
	// 0. 前置条件: 必须注入规则工厂 (依赖倒置，避免核心层依赖适配器层)
	if s.ruleFactory == nil {
		return nil, nil, fmt.Errorf("rule factory not configured: dynamic cleaning requires WithRuleFactory")
	}

	// 1. Group by DeviceType
	typeGroups := make(map[domain.DeviceType][]domain.Reading)
	for _, r := range readings {
		typeGroups[r.DeviceInfo.Type] = append(typeGroups[r.DeviceInfo.Type], r)
	}

	var result []domain.Reading
	var quarantined []domain.QuarantineReading
	var mu sync.Mutex
	var wg sync.WaitGroup
	errChan := make(chan error, len(typeGroups))
	// 2. Process each type group concurrently (or sequentially, concurrency here is minor optimization)
	// Given we hit DB, concurrency is good.
	for dType, grp := range typeGroups {
		wg.Add(1)
		go func(dt domain.DeviceType, curReadings []domain.Reading) {
			defer wg.Done()

			// a. Load Rules
			domainRules, err := s.ruleRepo.ListEnabledByDeviceType(ctx, dt)
			if err != nil {
				errChan <- fmt.Errorf("load rules for %s failed: %w", dt, err)
				return
			}

			// b. Convert Rules
			var execRules []ports.CleaningRule
			ruleFactory := s.ruleFactory

			for _, dr := range domainRules {
				idx, err := ruleFactory.CreateRule(dr)
				if err != nil {
					// Log warning but continue? Or fail?
					// Strict mode: fail
					errChan <- fmt.Errorf("convert rule %s failed: %w", dr.ID, err)
					return
				}
				execRules = append(execRules, idx)
			}

			// c. Sanitize
			localSanitizer := NewSanitizer(execRules...)
			cleanedRows, rejectedRows := localSanitizer.Clean(curReadings)

			mu.Lock()
			result = append(result, cleanedRows...)
			quarantined = append(quarantined, rejectedRows...)
			mu.Unlock()
		}(dType, grp)
	}

	wg.Wait()
	close(errChan)

	// 收集所有错误
	var errs []error
	for err := range errChan {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return nil, nil, errors.Join(errs...)
	}

	return result, quarantined, nil
}
