package services_test

import (
	"context"
	"testing"
	"time"

	"github.com/RenJieWeia/prism-core/pkg/core/domain"
	"github.com/RenJieWeia/prism-core/pkg/core/services"
)

func TestWithPrecision(t *testing.T) {
	base, _ := time.Parse(time.RFC3339, "2023-01-01T10:00:00Z")
	standardizer := services.NewCoreStandardizer(services.WithPrecision(1000))

	raw := []domain.Reading{
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base, Value: 1.2345},
	}
	results, err := standardizer.ProcessAndStandardize(context.Background(), raw)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	// 1.2345 * 1000 = 1234.5 -> int64 截断 -> 1234
	if results[0].ValueScaled != 1234 {
		t.Errorf("expected ValueScaled 1234, got %d", results[0].ValueScaled)
	}
	if results[0].ScaleFactor != 1000 {
		t.Errorf("expected ScaleFactor 1000, got %d", results[0].ScaleFactor)
	}
}

func TestWithPrecisionRejectsInvalid(t *testing.T) {
	// 无效因子 (<=0) 应保持默认值
	base, _ := time.Parse(time.RFC3339, "2023-01-01T10:00:00Z")
	standardizer := services.NewCoreStandardizer(services.WithPrecision(-5))
	results, err := standardizer.ProcessAndStandardize(context.Background(), []domain.Reading{
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base, Value: 100},
	})
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}
	if results[0].ScaleFactor != services.DefaultScaleFactor {
		t.Errorf("expected default factor %d, got %d", services.DefaultScaleFactor, results[0].ScaleFactor)
	}
}

func TestGetStandardReadingStatelessError(t *testing.T) {
	// 未注入仓储时，查询类方法应报错
	s := services.NewCoreStandardizer()
	if _, err := s.GetStandardReading(context.Background(), "D1", time.Now()); err == nil {
		t.Fatalf("expected error in stateless mode")
	}
}
