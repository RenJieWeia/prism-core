package services_test

import (
	"context"
	"testing"

	"github.com/RenJieWeia/prism-core/pkg/core/ports"
	"github.com/RenJieWeia/prism-core/pkg/core/services"
)

// 编译期断言: 原有公开接口保持不变，实现同时满足旧接口与新扩展接口。
var (
	// ChainSanitizer 同时实现旧接口 ports.Sanitizer 与新接口 ports.ReferenceSanitizer
	_ ports.Sanitizer          = (*services.ChainSanitizer)(nil)
	_ ports.ReferenceSanitizer = (*services.ChainSanitizer)(nil)

	// CoreStandardizer 同时实现旧接口 ports.EnergyDataStandardizer 与新接口 ports.EnergyDataProcessor
	_ ports.EnergyDataStandardizer = (*services.CoreStandardizer)(nil)
	_ ports.EnergyDataProcessor    = (*services.CoreStandardizer)(nil)
)

// TestLegacyInterfacesStillWork 验证旧接口路径不受参考能力影响
func TestLegacyInterfacesStillWork(t *testing.T) {
	// 仅使用旧接口的调用方式 (NewSanitizer 返回 ports.Sanitizer)
	var legacy ports.Sanitizer = services.NewSanitizer()
	clean, quarantined := legacy.Clean(nil)
	if clean != nil || quarantined != nil {
		t.Fatalf("expected empty results, got %v / %v", clean, quarantined)
	}

	// 仅使用旧接口的标准化调用方式 (NewCoreStandardizer 返回 ports.EnergyDataStandardizer)
	var std ports.EnergyDataStandardizer = services.NewCoreStandardizer()
	if _, err := std.ProcessAndStandardize(context.Background(), nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// NewEnergyDataProcessor 返回支持 Process 的完整接口
	var proc ports.EnergyDataProcessor = services.NewEnergyDataProcessor()
	if _, err := proc.Process(context.Background(), nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
