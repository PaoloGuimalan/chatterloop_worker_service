package models

import (
	"time"
)

// EntityInterestAffinity maps your personalization scoring table
type EntityInterestAffinity struct {
	ID           int64     `db:"id"`
	EntityID     string    `db:"entity_id"`
	InterestID   string    `db:"interest_id"`
	Score        float64   `db:"score"`
	LastBumpedAt time.Time `db:"last_bumped_at"`
}

// InterestTrendingScore maps your global platform-wide trending table
type InterestTrendingScore struct {
	ID                  int64     `db:"id"`
	InterestID          string    `db:"interest_id"`
	Score               float64   `db:"score"`
	RecentActivityBoost float64   `db:"recent_activity_boost"`
	UpdatedAt           time.Time `db:"updated_at"`
}
