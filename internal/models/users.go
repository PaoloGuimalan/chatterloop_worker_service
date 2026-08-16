package models

import (
	"time"
)

type Connection struct {
	ID                string    `db:"id"`
	ConnectionID      string    `db:"connection_id"`
	ActionByID        string    `db:"action_by_id"`
	Nickname          string    `db:"nickname"`
	Status            bool      `db:"status"`
	InvolvedEntityID  string    `db:"involved_entity_id"`
	ActionDate        time.Time `db:"action_date"`
	Type              string    `db:"type"`
	InteractionScore  float64   `db:"interaction_score"`
	LastInteractionAt time.Time `db:"last_interaction_at"`
}

type Follow struct {
	FollowID          string    `db:"follow_id"`
	FollowerID        string    `db:"follower_id"`
	FolloweeID        string    `db:"followee_id"`
	CreatedAt         time.Time `db:"created_at"`
	Status            bool      `db:"status"`
	InteractionScore  float64   `db:"interaction_score"`
	LastInteractionAt time.Time `db:"last_interaction_at"`
}
