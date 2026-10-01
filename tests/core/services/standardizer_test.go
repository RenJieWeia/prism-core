package services_test

import (
	"context"
	"testing"
	"time"

	"github.com/RenJieWeia/prism-core/pkg/core/domain"
	"github.com/RenJieWeia/prism-core/pkg/core/ports"
	"github.com/RenJieWeia/prism-core/pkg/core/services"
)

// Define a test-specific rule using the new interface
type monotonicTestRule struct{}

func (r *monotonicTestRule) Check(ctx ports.CleaningContext, curr domain.Reading) ports.CheckResult {
	if curr.Value < 0 {
		return ports.CheckResult{
			Reading:   curr,
			Passed:    false,
			Corrected: false,
			Reason:    "negative",
		}
	}
	// Strict monotonic for test: no regression allowed
	if ctx.Previous != nil && curr.Value < ctx.Previous.Value {
		return ports.CheckResult{
			Reading:   curr,
			Passed:    false,
			Corrected: false,
			Reason:    "calc regression",
		}
	}
	return ports.CheckResult{
		Reading:   curr,
		Passed:    true,
		Corrected: false,
		Reason:    "",
	}
}

func TestCoreStandardizer(t *testing.T) {
	// Setup
	// Factor 10000 (4 decimal places)
	// Repo nil (Stateless mode test)
	// Rules: Monotonic (Prevent Decreases)
	// Interval: 15m, Tolerance: 5m
	standardizer := services.NewCoreStandardizer(
		services.WithAlignment(15*time.Minute, 5*time.Minute),
		services.WithCleaningRules(&monotonicTestRule{}),
	)

	// Prepare Data with Issues
	// 1. Normal (10:00, 100.0)
	// 2. Duplicate Time (10:00, 100.0) -> Should be removed by sanitizer internal deduplication
	// 3. Jump Error (10:30, 20.0) -> Drop (100 -> 20 is decrease) by MonotonicRule
	// 4. Floating Point Precision (11:00, 100.00019) -> Should align to 1000002
	tBase, _ := time.Parse(time.RFC3339, "2023-01-01T10:00:00Z")

	raw := []domain.Reading{
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: tBase, Value: 100.0},
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: tBase, Value: 100.0},
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: tBase.Add(30 * time.Minute), Value: 20.0},
		{DeviceInfo: domain.DeviceInfo{ID: "D1"}, Timestamp: tBase.Add(60 * time.Minute), Value: 100.00019},
	}

	// Calculate Expectation
	// sanitizer.Clean sorts and cleans.
	// Item 1: Keep. LastVal = 100.0.
	// Item 2: Drop (Duplicate Timestamp).
	// Item 3: Drop (20.0 < 100.0). Sanity check logic in domain/strategies.go check: if lastVal >= 0 && r.Value < lastVal -> continue.
	// Item 4: Keep (100.00019 > 100.0).
	// Result: Item 1 and Item 4.

	ctx := context.Background()
	results, err := standardizer.ProcessAndStandardize(ctx, raw)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("Expected 2 standard readings, got %d", len(results))
	}

	// Verify Item 1
	r1 := results[0]
	if r1.ValueScaled != 1000000 {
		t.Errorf("Item 1 Scaled Value wrong: expected 1000000 (100.0), got %d", r1.ValueScaled)
	}
	if r1.ScaleFactor != 10000 {
		t.Errorf("Item 1 Factor wrong")
	}

	// Verify Item 2 (Precision Alignment)
	r2 := results[1]
	// 100.00019 * 10000 = 1000001.9 -> int64 truncation -> 1000001
	expectedScaled := int64(1000001)
	if r2.ValueScaled != expectedScaled {
		t.Errorf("Item 2 Scaled Value wrong: expected %d, got %d", expectedScaled, r2.ValueScaled)
	}
	if r2.ValueDisplay != 100.00019 {
		t.Errorf("Item 2 Display Value preserved wrong")
	}

	t.Logf("Transformation Success:")
	for _, r := range results {
		t.Logf("[%s] %v -> Scaled: %d (x%d)", r.Timestamp.Format("15:04:05"), r.ValueDisplay, r.ValueScaled, r.ScaleFactor)
	}
}
