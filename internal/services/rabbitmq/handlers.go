package rabbitmq

import (
	"context"
	"errors"
	"log"
	"math"
	"time"
	"worker_service/internal/connections"
	"worker_service/internal/models"

	"github.com/gocql/gocql"
	"github.com/jackc/pgx/v5"
)

type UpdateRankingPayload struct {
    PostID     string `json:"post_id"`
    UpdateType string `json:"update_type"`
    IsDecrease bool	`json:"is_decrease"`
}

type BumpInterestAffinityPayload struct {
	EntityID   string   `json:"entity_id"`
	InterestIDs []string `json:"interest_ids"`
	Action     string   `json:"action"`
	IsDecrease bool     `json:"is_decrease"`
}

type InteractionBumpPayload struct {
	ActorID    string `json:"actor_id"`
	ReceiverID string `json:"receiver_id"`
	Action     string `json:"action"`
	IsDecrease bool   `json:"is_decrease"`
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

	log.Printf("update_ranking_score: Successfully updated tracking score profile for Post ID: %s. Score: %f\n", post_id, rankingScore)
}

type ViewCachePayload struct {
	EntityID  string                 `json:"entity_id"`
	ViewCache []models.ViewCacheItem `json:"view_cache"`
}

func SaveViewCacheEngagements(entityID string, viewCache []models.ViewCacheItem) []models.UserEngagementLog {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if len(viewCache) == 0 {
		return []models.UserEngagementLog{}
	}

	userID, err := gocql.ParseUUID(entityID)
	if err != nil {
		log.Printf("Invalid entity UUID string for Cassandra: %v\n", err)
		return []models.UserEngagementLog{}
	}

	viewedPostIDs := make([]string, 0, len(viewCache))
	for _, view := range viewCache {
		viewedPostIDs = append(viewedPostIDs, view.PostID)
	}

	interestsByPostID := make(map[string][]string)
	
	pgQuery := `
		SELECT pil.post_id, pil.interest_id 
		FROM interests_postinterestlink pil 
		WHERE pil.post_id = ANY($1)`

	pgRows, err := connections.Pool().Query(ctx, pgQuery, viewedPostIDs)
	if err != nil {
		log.Printf("Postgres join pre-fetch query error: %v\n", err)
		return []models.UserEngagementLog{}
	}
	defer pgRows.Close()

	for pgRows.Next() {
		var pid, interestID string
		if err := pgRows.Scan(&pid, &interestID); err == nil {
			interestsByPostID[pid] = append(interestsByPostID[pid], interestID)
			log.Println(interestsByPostID[pid], interestID)
		}
	}

	var logsToCreate []models.UserEngagementLog
	var postIDsToClean []string

	var allInterestIDs []string

	for _, view := range viewCache {
		pid := view.PostID
		poid := view.PostOwnerID
		currentDuration := view.Duration

		postIDsToClean = append(postIDsToClean, pid)

		if interests, exists := interestsByPostID[pid]; exists {
			allInterestIDs = append(allInterestIDs, interests...)
		}

		if poid != entityID {
			createdAtTime, err := time.Parse(time.RFC3339, view.CreatedAt)
			if err != nil {
				createdAtTime = time.Now()
			}

			logUUID, err := gocql.RandomUUID()
			if err != nil {
				log.Printf("Failed to generate a cryptographic random UUID: %v\n", err)
				continue // Skip this log entry if UUID generation fails
			}

			logInstance := models.UserEngagementLog{
				LogID:        logUUID,
				UserID:       userID,
				ActivityTime: createdAtTime,
				TimeSpent:    currentDuration,
				ActivityType: "view",
				TargetType:   "post",
				TargetID:     pid,
				CreatedAt:    time.Now(),
				UpdatedAt:    time.Now(),
			}
			logsToCreate = append(logsToCreate, logInstance)
		}
	}

	if len(allInterestIDs) > 0 {
		go func(id string, interests []string) {
			bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			
			BumpInterestAffinity(bgCtx, id, interests, "VIEW", false)
		}(entityID, allInterestIDs)
	}

	session := connections.CassandraSession()
	if session == nil {
		log.Println("Skipping metrics storage: Astra session disconnected")
		return []models.UserEngagementLog{}
	}

	batch := session.NewBatch(gocql.UnloggedBatch).WithContext(ctx)

	insertCQL := `
		INSERT INTO user_engagement_log (
			log_id, user_id, activity_time, time_spent, activity_type, 
			target_type, target_id, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`

	for _, logLog := range logsToCreate {
		batch.Query(insertCQL, 
			logLog.LogID.String(), logLog.UserID.String(), logLog.ActivityTime, float32(logLog.TimeSpent), 
			logLog.ActivityType, logLog.TargetType, logLog.TargetID, logLog.CreatedAt, logLog.UpdatedAt,
		)
	}

	deleteCQL := "DELETE FROM newsfeed_index WHERE bucket = ? AND post_id = ?"
	for _, pidToClean := range postIDsToClean {
		batch.Query(deleteCQL, entityID, pidToClean)
	}

	if err := session.ExecuteBatch(batch); err != nil {
		log.Printf("Error saving viewcache metrics to Astra DB Batch: %v\n", err)
		return []models.UserEngagementLog{}
	}

	log.Printf("save_view_cache_engagements: Engagement Recorded")

	return logsToCreate
}

func BumpInterestAffinity(ctx context.Context, entityID string, interestIDs []string, action string, isDecrease bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	var interactionWeights = map[string]float64{
		"NEW_CONNECTION": 10.0,
		"SHARE": 7.0,
		"REPOST": 7.0,
		"DIARY_TAG": 5.0,
		"COMMENT": 4.0,
		"LIKE": 1.0,
		"VIEW": 0.1,
		"PROFILE_VISIT": 0.5,
	}

	weight, exists := interactionWeights[action]
	if !exists || weight == 0.0 {
		return
	}

	if len(interestIDs) == 0 {
		return
	}

	uniqueInterests := make(map[string]struct{})
	for _, id := range interestIDs {
		if id != "" {
			uniqueInterests[id] = struct{}{}
		}
	}

	delta := weight
	if isDecrease {
		delta = -weight
	}

	tx, err := connections.Pool().Begin(ctx)
	if err != nil {
		log.Printf("Failed to open affinity tracking transaction block: %v\n", err)
		return
	}
	defer tx.Rollback(ctx)

	affinityUpsert := `
		INSERT INTO interests_entityinterestaffinity (entity_id, interest_id, score, last_bumped_at)
		VALUES ($1, $2, $3, NOW())
		ON CONFLICT (entity_id, interest_id) DO UPDATE SET
			score = interests_entityinterestaffinity.score + EXCLUDED.score,
			last_bumped_at = NOW();`

	trendingUpsert := `
		INSERT INTO interests_interesttrendingscore (interest_id, score, recent_activity_boost, updated_at)
		VALUES ($1, $2, 1.0, NOW())
		ON CONFLICT (interest_id) DO UPDATE SET
			score = interests_interesttrendingscore.score + EXCLUDED.score,
			updated_at = NOW();`

	for interestID := range uniqueInterests {
		_, err = tx.Exec(ctx, affinityUpsert, entityID, interestID, delta)
		if err != nil {
			log.Printf("Failed atomic tracking execution on entity %s affinity: %v\n", entityID, err)
			return
		}

		_, err = tx.Exec(ctx, trendingUpsert, interestID, delta)
		if err != nil {
			log.Printf("Failed atomic trending execution on interest %s metrics: %v\n", interestID, err)
			return
		}
	}

	if err := tx.Commit(ctx); err != nil {
		log.Printf("Failed to commit affinity transactional modifications: %v\n", err)
		return
	}

	log.Printf("bump_interest_affinity: Bump Affinity Recorded")
}

var interactionWeights = map[string]float64{
	"NEW_CONNECTION": 10.0,
	"SHARE":          7.0,
	"REPOST":         7.0,
	"COMMENT":        4.0,
	"LIKE":           1.0,
	"VIEW":           0.1,
	"PROFILE_VISIT":  0.5,
}

func InteractionScoreBump(ctx context.Context, actorID string, receiverID string, action string, isDecrease bool) {
	if actorID == receiverID {
		return
	}

	weight, exists := interactionWeights[action]
	if !exists || weight == 0.0 {
		return
	}

	delta := weight
	if isDecrease {
		delta = -weight
	}

	tx, err := connections.Pool().Begin(ctx)
	if err != nil {
		log.Printf("Failed to begin transaction for interaction score bump: %v\n", err)
		return
	}
	defer tx.Rollback(ctx)

	optimizedQuery := `
		UPDATE entity_connection
		SET 
			interaction_score = interaction_score + $1,
			last_interaction_at = NOW()
		WHERE connection_id IN (
			SELECT connection_id 
			FROM entity_connection
			WHERE (action_by_id = $2 AND involved_entity_id = $3)
			   OR (action_by_id = $3 AND involved_entity_id = $2)
		)`

	_, err = tx.Exec(ctx, optimizedQuery, delta, actorID, receiverID)
	if err != nil {
		log.Printf("Failed to execute optimized entity interaction bump: %v\n", err)
		return
	}

	if err := tx.Commit(ctx); err != nil {
		log.Printf("Failed to commit interaction score changes: %v\n", err)
	}

	log.Printf("interaction_score_bump: Interaction Bump Recorded")
}

func FollowerInteractionScoreBump(ctx context.Context, actorID string, receiverID string, action string, isDecrease bool) {
	if receiverID == "" {
		return
	}

	weight, exists := interactionWeights[action]
	if !exists || weight == 0.0 {
		return
	}

	delta := weight
	if isDecrease {
		delta = -weight
	}

	tx, err := connections.Pool().Begin(ctx)
	if err != nil {
		log.Printf("Failed to begin transaction for follower score bump: %v\n", err)
		return
	}
	defer tx.Rollback(ctx)

	followQuery := `
		UPDATE entity_follow
		SET 
			interaction_score = interaction_score + $1,
			last_interaction_at = NOW()
		WHERE follower_id = $2 AND followee_id = $3`

	_, err = tx.Exec(ctx, followQuery, delta, actorID, receiverID)
	if err != nil {
		log.Printf("Failed to execute follow score update parameters: %v\n", err)
		return
	}

	if err := tx.Commit(ctx); err != nil {
		log.Printf("Failed to commit follower interaction score changes: %v\n", err)
	}

	log.Printf("follower_interaction_score_bump: Follower Interaction Bump Recorded")
}
