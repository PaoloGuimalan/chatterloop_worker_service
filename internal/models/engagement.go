package models

import (
	"time"

	"github.com/gocql/gocql"
)

type ViewCacheItem struct {
	PostID      string `json:"post_id"`
	PostOwnerID string `json:"post_owner_id"`
	Duration    float64 `json:"duration"`
	CreatedAt   string  `json:"created_at"`
}

type UserEngagementLog struct {
	LogID        gocql.UUID `cql:"log_id"`
	UserID       gocql.UUID `cql:"user_id"`
	ActivityTime time.Time `cql:"activity_time"`
	TimeSpent    float64   `cql:"time_spent"`
	ActivityType string    `cql:"activity_type"`
	TargetType   string    `cql:"target_type"`
	TargetID     string    `cql:"target_id"`
	Metadata     string    `cql:"metadata"`
	CreatedAt    time.Time `cql:"created_at"`
	UpdatedAt    time.Time `cql:"updated_at"`
}
