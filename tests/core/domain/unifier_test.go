package domain_test

import (
	"testing"

	"github.com/RenJieWeia/prism-core/pkg/core/domain"
)

// 文档化 MetricUnifier 的 math.Round 行为
// 注意: 这与 services.standardizeOne 的 int64 截断行为不同 (100.00019 -> 1000001)，
// 两套精度转换有意保持独立，勿擅自统一。
func TestMetricUnifierRounding(t *testing.T) {
	u := domain.NewUnifier(10000)

	if u.GetScaleFactor() != 10000 {
		t.Fatalf("unexpected scale factor: %d", u.GetScaleFactor())
	}

	// math.Round(100.00019 * 10000) = Round(1000001.9) = 1000002
	if got := u.ToScaled(100.00019); got != 1000002 {
		t.Errorf("ToScaled(100.00019) = %d, want 1000002", got)
	}

	// 整数精确往返
	scaled := u.ToScaled(123.4567)
	back := u.FromScaled(scaled)
	if got := back; got != 123.4567 {
		t.Errorf("FromScaled(ToScaled(123.4567)) = %v, want 123.4567", got)
	}
}

func TestMetricUnifierRoundTrip(t *testing.T) {
	u := domain.NewUnifier(100)
	cases := []float64{0, 1.5, -3.25, 999.99}
	for _, c := range cases {
		if back := u.FromScaled(u.ToScaled(c)); back != c {
			t.Errorf("round trip failed for %v: got %v", c, back)
		}
	}
}
