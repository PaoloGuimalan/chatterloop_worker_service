package models

import (
	"database/sql"
	"time"
)

// PrivacyStatus represents the custom type for post privacy levels.
type PrivacyStatus string

const (
	PrivacyPublic      PrivacyStatus = "public"
	PrivacyConnections PrivacyStatus = "connections"
	PrivacyPrivate     PrivacyStatus = "private"
	PrivacyCustom      PrivacyStatus = "custom"
)

// Post represents the Go equivalent of your Django models.Model.
type Post struct {
	PostID        string        `db:"post_id"` // Primary Key
	EntityID      string         `db:"entity_id"` // Foreign Key (Assumed int64/BIGINT ID)
	IsShared      bool          `db:"is_shared"`
	FileType      string        `db:"file_type"`
	Caption       sql.NullString `db:"caption"` // Text field that allows NULL
	ContentType   string        `db:"content_type"`
	IsTagged      bool          `db:"is_tagged"`
	PrivacyStatus PrivacyStatus `db:"privacy_status"`
	IsSponsored   bool          `db:"is_sponsored"`
	IsLive        bool          `db:"is_live"`
	IsArchived    bool          `db:"is_archived"`
	OnFeed        string        `db:"on_feed"`
	DatePosted    time.Time     `db:"date_posted"`
	FromSystem    bool          `db:"from_system"`
	DeletedAt     sql.NullTime  `db:"deleted_at"` // DateTime field that allows NULL
	// NullString, not NullInt64. The column is varchar - it held Account ids
	// (varchar) before newsfeed/0009 repointed it and Entity ids (varchar)
	// after - so the int type was always wrong. It went unnoticed because
	// UpdateRankingScore does SELECT *, and a NULL scans cleanly into any
	// Null* type: every post the worker had touched happened to have no
	// deleter. A post that HAS one - the 29 already in this database, and
	// every one the moderation service removes - failed to parse.
	DeletedByID sql.NullString `db:"deleted_by_id"` // Foreign Key that allows NULL
}

type PostScore struct {
	ID                int64   `db:"id"`   
	PostID            string  `db:"post_id"` // OneToOneField uses the referenced model's primary key as the DB column
	AffinityScore     float64 `db:"affinity_score"`
	ContentTypeWeight float64 `db:"content_type_weight"`
	RecentUpdateBoost float64 `db:"recent_update_boost"`
	LikesCount        uint64  `db:"likes_count"`
	CommentsCount     uint64  `db:"comments_count"`
	SharesCount       uint64  `db:"shares_count"`
	RankingScore      float64 `db:"ranking_score"`
}

type PostReference struct {
	ID                 int64     `db:"id"`
	PostID             string    `db:"post_id"`
	ReferenceMediaType string    `db:"reference_media_type"` // "image", "video", etc.
	CreatedAt          time.Time `db:"created_at"`
}
