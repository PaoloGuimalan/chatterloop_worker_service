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

type NewPostCreatedPayload struct {
	PostID     string    `json:"post_id"`
	DatePosted string `json:"date_posted"`
}

type PostData struct {
	ID       string `json:"id"`
	AuthorID string `json:"author_id"`
}

type BulkFanoutPayload struct {
	PostData        PostData `json:"post_data"`
	CurrentEntityID string   `json:"current_entity_id"`
	Type string `json:"type"`
}
const DefaultFanoutType = "fanout"

type BackfillFriendFeedPayload struct {
	ViewerID    string `json:"viewer_id"`
	NewFriendID string `json:"new_friend_id"`
	Type        string `json:"type"`
}

// How many of the new friend's posts are considered for the backfill.
const BackfillCandidateLimit = 50

type RemoveFeedPayload struct {
	ActorID  string `json:"actor_id"`
	AuthorID string `json:"author_id"`
	Type     string `json:"type"`
}

// One conversation's worth of bumps in a single message. InteractionBumpPayload
// carries one receiver, which meant a group chat published one message per
// member; this carries them all and resolves them in one statement.
type ChatScoreBumpPayload struct {
	ActorID    string   `json:"actor_id"`
	MemberIDs  []string `json:"member_ids"`
	Action     string   `json:"action"`
	IsDecrease bool     `json:"is_decrease"`
}

const DefaultChatAction = "CHAT"

// Deletes are chunked to stay under Cassandra's batch size threshold. Every row
// shares one partition (bucket), so the batches themselves are cheap.
const FeedDeleteChunkSize = 100


func UpdateRankingScore(post_id string, update_type string, is_decrease bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Every failure below abandons this one message and returns, matching how
	// the rest of this file handles errors. A bad post id or a blipped
	// connection is a property of the message, not of the worker, and must not
	// take the process down — the other listeners are still serving their own
	// queues from the same binary.
	postRows, err := connections.Pool().Query(ctx, "SELECT * FROM newsfeed_post WHERE post_id = $1", post_id)
	if err != nil {
		log.Printf("update_ranking_score: failed to query post %s: %v\n", post_id, err)
		return
	}
	postData, err := pgx.CollectOneRow(postRows, pgx.RowToStructByName[models.Post])
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			log.Printf("No post found with ID: %s\n", post_id)
			return
		}
		log.Printf("update_ranking_score: failed to parse post %s: %v\n", post_id, err)
		return
	}

	scoreRows, err := connections.Pool().Query(ctx, "SELECT * FROM newsfeed_postscore WHERE post_id = $1", post_id)
	if err != nil {
		log.Printf("update_ranking_score: failed to query score for post %s: %v\n", post_id, err)
		return
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
			log.Printf("update_ranking_score: failed to parse score for post %s: %v\n", post_id, err)
			return
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
		log.Printf("update_ranking_score: failed to upsert score for post %s: %v\n", post_id, err)
		return
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
	"CHAT":          3.0,
	"COMMENT":       4.0,
	"LIKE":          1.0,
	"VIEW":          0.1,
	"PROFILE_VISIT": 0.5,
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

// BumpChatScore is InteractionScoreBump for an entire conversation.
//
// The chat path bumps the actor against every other member at once, which as
// one-message-per-member meant N publishes, N handler invocations and N
// transactions for a single sent message. Here the member list arrives whole
// and every matching connection moves in one UPDATE.
//
// The 30-minute Redis lock that decides WHETHER to bump stays on the publisher:
// it is per-conversation, and this handler has no idea a conversation exists.
func BumpChatScore(ctx context.Context, actorID string, memberIDs []string, action string, isDecrease bool) {
	if action == "" {
		action = DefaultChatAction
	}

	if actorID == "" || len(memberIDs) == 0 {
		return
	}

	weight, exists := interactionWeights[action]
	if !exists || weight == 0.0 {
		log.Printf("bump_chat_score: unknown action %q, nothing to do\n", action)
		return
	}

	delta := weight
	if isDecrease {
		delta = -weight
	}

	// Deduplicated, and the actor dropped: nobody bumps a connection with
	// themselves, and a member listed twice must not count twice - the
	// per-member version got both of those from its early return.
	seen := make(map[string]struct{}, len(memberIDs))
	receivers := make([]string, 0, len(memberIDs))
	for _, id := range memberIDs {
		if id == "" || id == actorID {
			continue
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		receivers = append(receivers, id)
	}

	if len(receivers) == 0 {
		return
	}

	tx, err := connections.Pool().Begin(ctx)
	if err != nil {
		log.Printf("bump_chat_score: failed to begin transaction: %v\n", err)
		return
	}
	defer tx.Rollback(ctx)

	// Same shape as InteractionScoreBump's statement, with ANY($3) standing in
	// for its single receiver so one round trip covers the whole conversation.
	const query = `
		UPDATE entity_connection
		SET
			interaction_score = interaction_score + $1,
			last_interaction_at = NOW()
		WHERE connection_id IN (
			SELECT connection_id
			FROM entity_connection
			WHERE (action_by_id = $2 AND involved_entity_id = ANY($3))
			   OR (involved_entity_id = $2 AND action_by_id = ANY($3))
		)`

	tag, err := tx.Exec(ctx, query, delta, actorID, receivers)
	if err != nil {
		log.Printf("bump_chat_score: failed to execute bump for actor %s: %v\n", actorID, err)
		return
	}

	if err := tx.Commit(ctx); err != nil {
		log.Printf("bump_chat_score: failed to commit: %v\n", err)
		return
	}

	log.Printf("bump_chat_score: %d connection rows bumped for actor %s across %d members (action=%s)\n",
		tag.RowsAffected(), actorID, len(receivers), action)
}

func CreatePostScoreForNewPost(ctx context.Context, postID string, datePosted time.Time) {
	query := "SELECT reference_id, post_id, reference_media_type FROM newsfeed_postreference WHERE post_id = $1"
	
	rows, err := connections.Pool().Query(ctx, query, postID)
	if err != nil {
		log.Printf("Failed to query post references for ID %s: %v\n", postID, err)
		return
	}
	defer rows.Close()

	var mediaTypes []string
	for rows.Next() {
		var reference_id int64
		var pid string
		var mediaType string
		if err := rows.Scan(&reference_id, &pid, &mediaType); err == nil {
			mediaTypes = append(mediaTypes, mediaType)
		}
	}

	// These constants reproduce the Node createpost block this handler replaced,
	// deliberately and to the digit. Posts are only ever created by the chat
	// server (routes/posts/index.js), so every existing newsfeed_postscore row
	// was scored this way - and content_type_weight is PERSISTED and reused by
	// UpdateRankingScore forever after, so a different scale here would leave
	// new posts permanently incomparable with the corpus.
	//
	// The Django signal that also fed this queue used +1.2/+1.5 and decay ^1.2,
	// but nothing in user_service ever creates a Post, so those numbers never
	// reached a real row.
	contentTM := 1.0
	referenceCount := float64(len(mediaTypes))

	if referenceCount > 0 {
		for _, mType := range mediaTypes {
			switch mType {
			case "image":
				contentTM += 6.5
			case "video":
				contentTM += 8.5
			default:
				contentTM += 2.0
			}
		}
	} else {
		// A text-only post is credited rather than left at the 1.0 floor.
		contentTM += 4.0
	}

	finalContentScore := contentTM / (referenceCount + 1.0)

	// Faithful to the original, which read
	//   age_hours = currentTimestampInSeconds / (1000 * 60 * 60)
	// i.e. epoch SECONDS over milliseconds-per-hour. That is not the post's age
	// - it is ~496 for any post created today, and creeps up by ~8.8 per year -
	// so every new post is divided by a decay of about 22.3 regardless of when
	// it was written.
	//
	// Kept because the value only survives until the post's first interaction,
	// at which point UpdateRankingScore recomputes it from the real elapsed
	// time; changing it here would put new posts on a different scale from every
	// untouched row already in the table. Fix it in a backfill that rewrites all
	// of them together, not one post at a time.
	ageHours := float64(datePosted.Unix()) / (1000.0 * 60.0 * 60.0)
	affinityScore := 1.0
	contentTypeWeight := finalContentScore
	recentUpdateBoost := 1.0
	commentsCount := 0
	likesCount := 0
	sharesCount := 0

	// A new post has no engagement, so without this term the numerator is zero
	// and every post is created with ranking_score 0.0 - bottom of every ranked
	// feed, seen by nobody, therefore never interacted with, therefore never
	// lifted out of it. The base term is what gives a post its opening position,
	// earned from its content weight alone.
	//
	// UpdateRankingScore already carries the same 1.0, so this also stops a
	// post's score jumping the first time anyone reacts to it.
	baseEngagement := 1.0

	weightedEngagement := float64(commentsCount)*3.0 + float64(likesCount)*1.0 + float64(sharesCount)*5.0 + baseEngagement

	// ^0.5, matching UpdateRankingScore. The ^1.2 this used to carry came from
	// the Django signal and made a post's score jump the first time anyone
	// touched it, since the two handlers then decayed it differently.
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
		postID,
		affinityScore,
		contentTypeWeight,
		recentUpdateBoost,
		likesCount,
		commentsCount,
		sharesCount,
		rankingScore,
	)
	if err != nil {
		log.Printf("Failed to execute initial post score upsert for ID %s: %v\n", postID, err)
		return
	}

	log.Printf("create_post_score_for_new_post: Successfully generated initial score profile for new Post ID: %s. Score: %f\n", postID, rankingScore)
}

func BulkFanoutToCache(ctx context.Context, currentEntityID string, postData PostData, rowType string) {
	if rowType == "" {
		rowType = DefaultFanoutType
	}

	pgQuery := `
		SELECT follower_id 
		FROM entity_follow 
		WHERE followee_id = $1 AND status = true 
		ORDER BY interaction_score DESC, last_interaction_at DESC 
		LIMIT 500`

	pgRows, err := connections.Pool().Query(ctx, pgQuery, currentEntityID)
	if err != nil {
		log.Printf("Failed to resolve follower graph for Entity %s: %v\n", currentEntityID, err)
		return
	}
	defer pgRows.Close()

	var followerIDs []string
	for pgRows.Next() {
		var fidStr string
		if err := pgRows.Scan(&fidStr); err == nil {
			if fidStr != "" {
				followerIDs = append(followerIDs, fidStr)
			}
		}
	}

	if len(followerIDs) == 0 {
		return 
	}

	session := connections.CassandraSession()
	if session == nil {
		log.Println("Astra DB disconnected. Skipping timeline cache fanout.")
		return
	}

	batch := session.NewBatch(gocql.UnloggedBatch).WithContext(ctx)

	insertCQL := `
		INSERT INTO newsfeed_index (bucket, post_id, created_at, author_id, type)
		VALUES (?, ?, ?, ?, ?)`

	nowTimestamp := time.Now()

	for _, followerID := range followerIDs {
		batch.Query(insertCQL,
			followerID,
			postData.ID,
			nowTimestamp,
			postData.AuthorID,
			rowType,
		)
	}

	if err := session.ExecuteBatch(batch); err != nil {
		log.Printf("Error processing serverless Astra timeline fanout batch query: %v\n", err)
		return
	}

	log.Printf("bulk_fanout_to_cache: Successfully fanned out Post ID %s for entity %s to %d follower feeds in Astra DB (type=%s).\n",
		postData.ID, currentEntityID, len(followerIDs), rowType)
}

// mutualConnectionIDs mirrors ConnectionHelpers.get_mutual_connections: the
// accepted connections both entities share. Each side must be an active +
// verified account or an active realm (entity_side_is_visible).
func mutualConnectionIDs(ctx context.Context, viewerID string, friendID string) []string {
	const query = `
		WITH visible AS (
			SELECT c.action_by_id AS a, c.involved_entity_id AS b
			FROM entity_connection c
			LEFT JOIN user_account au ON au.entity_id = c.action_by_id
			LEFT JOIN community_realm ar ON ar.entity_id = c.action_by_id
			LEFT JOIN user_account bu ON bu.entity_id = c.involved_entity_id
			LEFT JOIN community_realm br ON br.entity_id = c.involved_entity_id
			WHERE c.status = TRUE
			  AND c.action_by_id <> c.involved_entity_id
			  AND (COALESCE(au.is_active AND au.is_verified, FALSE) OR COALESCE(ar.is_active, FALSE))
			  AND (COALESCE(bu.is_active AND bu.is_verified, FALSE) OR COALESCE(br.is_active, FALSE))
		),
		viewer_peers AS (
			SELECT DISTINCT CASE WHEN a = $1 THEN b ELSE a END AS peer
			FROM visible WHERE a = $1 OR b = $1
		),
		friend_peers AS (
			SELECT DISTINCT CASE WHEN a = $2 THEN b ELSE a END AS peer
			FROM visible WHERE a = $2 OR b = $2
		)
		SELECT vp.peer
		FROM viewer_peers vp
		JOIN friend_peers fp ON fp.peer = vp.peer
		WHERE vp.peer <> $1 AND vp.peer <> $2`

	rows, err := connections.Pool().Query(ctx, query, viewerID, friendID)
	if err != nil {
		log.Printf("backfill_new_friend_feed: failed to resolve mutual connections for %s/%s: %v\n", viewerID, friendID, err)
		return nil
	}
	defer rows.Close()

	var peers []string
	for rows.Next() {
		var peer string
		if err := rows.Scan(&peer); err == nil && peer != "" {
			peers = append(peers, peer)
		}
	}
	return peers
}

func engagedPostTimes(ctx context.Context, entityID string, activityTypes []string, candidates map[string]struct{}) map[string]time.Time {
	out := make(map[string]time.Time)

	userID, err := gocql.ParseUUID(entityID)
	if err != nil {
		return out
	}

	session := connections.CassandraSession()
	if session == nil {
		return out
	}

	const cql = `
		SELECT target_id, activity_time
		FROM user_engagement_log
		WHERE user_id = ? AND activity_type IN ? AND target_type = 'post'
		ALLOW FILTERING`

	iter := session.Query(cql, userID, activityTypes).WithContext(ctx).Iter()

	var targetID string
	var activityTime time.Time
	for iter.Scan(&targetID, &activityTime) {
		if _, wanted := candidates[targetID]; !wanted {
			continue
		}
		if prev, seen := out[targetID]; !seen || activityTime.After(prev) {
			out[targetID] = activityTime
		}
	}

	if err := iter.Close(); err != nil {
		log.Printf("backfill_new_friend_feed: engagement scan failed for %s: %v\n", entityID, err)
	}

	return out
}

func BackfillNewFriendFeed(ctx context.Context, viewerID string, newFriendID string, rowType string) {
	if rowType == "" {
		rowType = DefaultFanoutType
	}
	if viewerID == "" || newFriendID == "" || viewerID == newFriendID {
		return
	}

	const postQuery = `
		SELECT post_id
		FROM newsfeed_post
		WHERE entity_id = $1
		ORDER BY date_posted DESC
		LIMIT $2`

	postRows, err := connections.Pool().Query(ctx, postQuery, newFriendID, BackfillCandidateLimit)
	if err != nil {
		log.Printf("backfill_new_friend_feed: failed to load posts for %s: %v\n", newFriendID, err)
		return
	}
	defer postRows.Close()

	var candidateIDs []string
	candidates := make(map[string]struct{})
	for postRows.Next() {
		var pid string
		if err := postRows.Scan(&pid); err == nil && pid != "" {
			candidateIDs = append(candidateIDs, pid)
			candidates[pid] = struct{}{}
		}
	}

	if len(candidateIDs) == 0 {
		return
	}

	skip := make(map[string]time.Time)
	merge := func(seen map[string]time.Time) {
		for pid, ts := range seen {
			if prev, ok := skip[pid]; !ok || ts.After(prev) {
				skip[pid] = ts
			}
		}
	}

	for _, mutualID := range mutualConnectionIDs(ctx, viewerID, newFriendID) {
		merge(engagedPostTimes(ctx, mutualID, []string{"comment", "share"}, candidates))
	}
	merge(engagedPostTimes(ctx, viewerID, []string{"view"}, candidates))

	session := connections.CassandraSession()
	if session == nil {
		log.Println("Astra DB disconnected. Skipping friend feed backfill.")
		return
	}

	batch := session.NewBatch(gocql.UnloggedBatch).WithContext(ctx)

	const insertCQL = `
		INSERT INTO newsfeed_index (bucket, post_id, created_at, author_id, type)
		VALUES (?, ?, ?, ?, ?)`

	nowTimestamp := time.Now()
	inserted := 0

	for _, pid := range candidateIDs {
		if _, alreadyEngaged := skip[pid]; alreadyEngaged {
			continue
		}
		batch.Query(insertCQL, viewerID, pid, nowTimestamp, newFriendID, rowType)
		inserted++
	}

	if inserted == 0 {
		log.Printf("backfill_new_friend_feed: nothing new to seed for viewer %s from %s.\n", viewerID, newFriendID)
		return
	}

	if err := session.ExecuteBatch(batch); err != nil {
		log.Printf("backfill_new_friend_feed: batch insert failed for viewer %s: %v\n", viewerID, err)
		return
	}

	log.Printf("backfill_new_friend_feed: Seeded %d of %d posts by %s into %s's timeline (type=%s).\n",
		inserted, len(candidateIDs), newFriendID, viewerID, rowType)
}

// RemoveFeedOnUnfriend pulls one author's fanned-out posts back out of a
// viewer's timeline — the inverse of BulkFanoutToCache, run on unfollow, block
// and connection removal.
func RemoveFeedOnUnfriend(ctx context.Context, actorID string, authorID string, rowType string) {
	if rowType == "" {
		rowType = DefaultFanoutType
	}
	if actorID == "" || authorID == "" {
		return
	}

	session := connections.CassandraSession()
	if session == nil {
		log.Println("Astra DB disconnected. Skipping timeline cleanup.")
		return
	}

	// bucket is the partition key, so this reads a single partition. author_id
	// and type are regular columns, hence ALLOW FILTERING.
	const selectCQL = `
		SELECT post_id, created_at
		FROM newsfeed_index
		WHERE bucket = ? AND author_id = ? AND type = ?
		ALLOW FILTERING`

	type indexRow struct {
		postID    string
		createdAt time.Time
	}

	var doomed []indexRow

	iter := session.Query(selectCQL, actorID, authorID, rowType).WithContext(ctx).Iter()

	var postID string
	var createdAt time.Time
	for iter.Scan(&postID, &createdAt) {
		doomed = append(doomed, indexRow{postID: postID, createdAt: createdAt})
	}

	if err := iter.Close(); err != nil {
		log.Printf("remove_feed_on_unfriend: failed to scan bucket %s for author %s: %v\n", actorID, authorID, err)
		return
	}

	if len(doomed) == 0 {
		return
	}

	// Deleting needs the whole primary key: (bucket, post_id, created_at).
	const deleteCQL = `DELETE FROM newsfeed_index WHERE bucket = ? AND post_id = ? AND created_at = ?`

	removed := 0
	for start := 0; start < len(doomed); start += FeedDeleteChunkSize {
		end := start + FeedDeleteChunkSize
		if end > len(doomed) {
			end = len(doomed)
		}

		batch := session.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
		for _, row := range doomed[start:end] {
			batch.Query(deleteCQL, actorID, row.postID, row.createdAt)
		}

		if err := session.ExecuteBatch(batch); err != nil {
			log.Printf("remove_feed_on_unfriend: batch delete failed for bucket %s after %d rows: %v\n", actorID, removed, err)
			return
		}

		removed += end - start
	}

	log.Printf("remove_feed_on_unfriend: Removed %d rows authored by %s from %s's timeline (type=%s).\n",
		removed, authorID, actorID, rowType)
}
