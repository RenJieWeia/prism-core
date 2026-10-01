package services_test

import (
	"testing"
	"time"

	"github.com/RenJieWeia/prism-core/pkg/core/domain"
	"github.com/RenJieWeia/prism-core/pkg/core/services"
	"github.com/RenJieWeia/prism-core/pkg/core/services/rules"
)

// TestChainSanitizerInterleavedDeviceStateIsolation 多设备状态隔离回归测试
// 输入 D1/D2/D1 交错数据时，D1 第二条必须与 D1 第一条比较，并被单调性规则拒绝。
func TestChainSanitizerInterleavedDeviceStateIsolation(t *testing.T) {
	base, _ := time.Parse(time.RFC3339, "2026-08-04T10:00:00Z")
	sanitizer := services.NewSanitizer(&rules.MonotonicRule{Action: domain.ActionReject})

	readings := []domain.Reading{
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base, Value: 100},
		{DeviceInfo: domain.DeviceInfo{ID: "D2"}, Timestamp: base.Add(1 * time.Minute), Value: 50},
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base.Add(2 * time.Minute), Value: 90},
	}

	clean, quarantined := sanitizer.Clean(readings)

	if len(clean) != 2 {
		t.Fatalf("expected 2 clean readings (D1@100, D2@50), got %d: %+v", len(clean), clean)
	}
	if len(quarantined) != 1 {
		t.Fatalf("expected 1 quarantined reading, got %d: %+v", len(quarantined), quarantined)
	}
	// D1 第二条 (90 < 100) 必须与 D1 第一条比较并被拒绝
	if quarantined[0].Reading.DeviceInfo.ID != "D1" {
		t.Fatalf("expected D1 regression to be quarantined, got device %s",
			quarantined[0].Reading.DeviceInfo.ID)
	}
	if clean[0].Value != 100 || clean[1].Value != 50 {
		t.Errorf("unexpected clean values: %v, %v", clean[0].Value, clean[1].Value)
	}
}

// TestChainSanitizerDoesNotMutateInputSlice 验证清洗器不原地修改调用方数据切片
func TestChainSanitizerDoesNotMutateInputSlice(t *testing.T) {
	base, _ := time.Parse(time.RFC3339, "2026-08-04T10:00:00Z")
	sanitizer := services.NewSanitizer(&rules.RangeRule{Min: 0, Max: 1000})

	readings := []domain.Reading{
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base.Add(30 * time.Minute), Value: 110},
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base, Value: 100},
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: base.Add(15 * time.Minute), Value: 105},
	}
	orig := make([]domain.Reading, len(readings))
	copy(orig, readings)

	_, _ = sanitizer.Clean(readings)

	for i := range orig {
		if !readings[i].Timestamp.Equal(orig[i].Timestamp) || readings[i].Value != orig[i].Value {
			t.Fatalf("input slice was mutated at index %d: got %+v, want %+v", i, readings[i], orig[i])
		}
	}
}
