package rabbitmq

import "testing"

func TestRankingRefreshDoesNotBoostOrReapplyShareDelta(t *testing.T) {
	for _, decrease := range []bool{false, true} {
		if got := rankingRecentBoost(2.5, "refresh", decrease); got != 2.5 {
			t.Fatalf("refresh boosted score: %v", got)
		}
	}
	if got := rankingRecentBoost(2., "share", false); got != 2.5 {
		t.Fatalf("ordinary share boost changed: %v", got)
	}
	if got := rankingRecentBoost(2., "comment", true); got != 1.7 {
		t.Fatalf("ordinary comment decrease changed: %v", got)
	}
}
