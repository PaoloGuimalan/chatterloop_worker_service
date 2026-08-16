package rabbitmq

import (
	"context"
	"errors"
	"log"
	"math"
	"time"
	"worker_service/internal/connections"
	"worker_service/internal/models"

	"github.com/jackc/pgx/v5"
)

type UpdateRankingPayload struct {
    PostID     string `json:"post_id"`
    UpdateType string `json:"update_type"`
    IsDecrease bool	`json:"is_decrease"`
}

func UpdateRankingScore(post_id string, update_type string, is_decrease bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	postRows, err := connections.Pool().Query(ctx, "SELECT * FROM newsfeed_post WHERE post_id = $1", post_id)
	if err != nil {
		log.Fatalf("Failed to query post: %v", err)
	}
	postData, err := pgx.CollectOneRow(postRows, pgx.RowToStructByName[models.Post])
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			log.Printf("No post found with ID: %s\n", post_id)
			return
		}
		log.Fatalf("Failed to parse post data: %v", err)
	}

	scoreRows, err := connections.Pool().Query(ctx, "SELECT * FROM newsfeed_postscore WHERE post_id = $1", post_id)
	if err != nil {
		log.Fatalf("Failed to query post score: %v", err)
	}
	postScore, err := pgx.CollectOneRow(scoreRows, pgx.RowToStructByName[models.PostScore])
	
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			postScore = models.PostScore{
				PostID:            post_id,
				AffinityScore:     1.0,
				ContentTypeWeight: 1.0,
				RecentUpdateBoost: 1.0,
				LikesCount:        0,
				CommentsCount:     0,
				SharesCount:       0,
				RankingScore:      0.0,
			}
		} else {
			log.Fatalf("Failed to parse post score data: %v", err)
		}
	}

	reactions := postScore.LikesCount
	reactionsTotal := reactions
	commentsCount := postScore.CommentsCount
	sharesCount := postScore.SharesCount

	var change int64 = 1
	if is_decrease {
		change = -1
	}

	switch update_type {
	case "react":
		if change == 1 {
			reactionsTotal++
		} else if change == -1 && reactionsTotal > 0 {
			reactionsTotal--
		}
	case "comment":
		if change == 1 {
			commentsCount++
		} else if change == -1 && commentsCount > 0 {
			commentsCount--
		}
	case "share":
		if change == 1 {
			sharesCount++
		} else if change == -1 && sharesCount > 0 {
			sharesCount--
		}
	}

	newRecentUpdateBoost := postScore.RecentUpdateBoost
	switch update_type {
		case "react":
			if is_decrease {
				newRecentUpdateBoost -= 0.1
			} else {
				newRecentUpdateBoost += 0.1
			}
		case "comment":
			if is_decrease {
				newRecentUpdateBoost -= 0.3
			} else {
				newRecentUpdateBoost += 0.3
			}
		case "share":
			if is_decrease {
				newRecentUpdateBoost -= 0.5
			} else {
				newRecentUpdateBoost += 0.5
			}
		default:
			
		if is_decrease {
			newRecentUpdateBoost -= 0.1
		} else {
			newRecentUpdateBoost += 0.1
		}
	}

	ageHours := time.Since(postData.DatePosted).Hours()
	affinityScore := 1.0
	contentTypeWeight := postScore.ContentTypeWeight 
	recentUpdateBoost := newRecentUpdateBoost
	likesCount := reactionsTotal

	baseEngagement := 1.0

	weightedEngagement := float64(commentsCount)*3.0 + float64(likesCount)*1.0 + float64(sharesCount)*5.0 + baseEngagement
	
	decayFactor := math.Pow(ageHours+1.0, 0.5) 
	
	rankingScore := (weightedEngagement / decayFactor) * affinityScore * contentTypeWeight * recentUpdateBoost

	upsertQuery := `
		INSERT INTO newsfeed_postscore (
			post_id, affinity_score, content_type_weight, recent_update_boost, 
			likes_count, comments_count, shares_count, ranking_score
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (post_id) DO UPDATE SET
			affinity_score = EXCLUDED.affinity_score,
			content_type_weight = EXCLUDED.content_type_weight,
			recent_update_boost = EXCLUDED.recent_update_boost,
			likes_count = EXCLUDED.likes_count,
			comments_count = EXCLUDED.comments_count,
			shares_count = EXCLUDED.shares_count,
			ranking_score = EXCLUDED.ranking_score;`

	_, err = connections.Pool().Exec(ctx, upsertQuery,
		post_id,
		affinityScore,
		contentTypeWeight,
		recentUpdateBoost,
		likesCount,
		commentsCount,
		sharesCount,
		rankingScore,
	)
	if err != nil {
		log.Fatalf("Failed to execute upsert on post score: %v", err)
	}

	log.Printf("Successfully updated tracking score profile for Post ID: %s. Score: %f\n", post_id, rankingScore)
}
