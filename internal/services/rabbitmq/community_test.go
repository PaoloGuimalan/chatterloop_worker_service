package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// Only PostgreSQL and Cassandra are replaced: audience selection, pagination,
// stored identity resolution and the emitted cache rows are production code.
type memoryFanoutStore struct {
	scope              fanoutScope
	err                error
	members, followers []string
	commentAuthor      string
	commentAnonymous   bool
	commentErr         error
	followerArgs       []string
}

func (s *memoryFanoutStore) currentMembers(_ context.Context, _ string, ids []string) ([]string, error) {
	if s.err != nil {
		return nil, s.err
	}
	var result []string
	for _, id := range ids {
		for _, member := range s.members {
			if id == member {
				result = append(result, id)
				break
			}
		}
	}
	return result, nil
}

func TestCommunityPushRechecksCurrentMembersAndRemovesIdentity(t *testing.T) {
	store := &memoryFanoutStore{scope: fanoutScope{CommunityID: "community", RealmID: "realm", Live: true, Private: true, Anonymous: true}, members: []string{"still-member"}}
	payload := SendPushPayload{CommunityPostID: "post", EntityIDs: []string{"former-member", "still-member"}, Tokens: []string{"bypass-token"}, ImageURL: "author.jpg", Tag: "author-id", Body: "author name", Data: map[string]string{"actor_id": "secret", "route": "/post/post?anchor=comment"}}
	safe, err := prepareCommunityPush(context.Background(), store, payload)
	if err != nil || !reflect.DeepEqual(safe.EntityIDs, []string{"still-member"}) || len(safe.Tokens) != 0 {
		t.Fatalf("push audience: %+v err=%v", safe, err)
	}
	if safe.ImageURL != "" || safe.Tag != "community-post" || safe.Body != "New activity in your community." || safe.Data["actor_id"] != "" || safe.Data["route"] != "/post/post?anchor=comment" {
		t.Fatalf("unsafe projection: %+v", safe)
	}
	store.err = errors.New("offline")
	if _, err := prepareCommunityPush(context.Background(), store, payload); err == nil {
		t.Fatal("community lookup errors must block push")
	}
	store.err = nil
	store.scope.Live = false
	if _, err := prepareCommunityPush(context.Background(), store, payload); err == nil {
		t.Fatal("deleted/expired community post must block push")
	}
	store.scope.Live = true
	store.scope.CommunityID = ""
	if _, err := prepareCommunityPush(context.Background(), store, payload); err == nil {
		t.Fatal("noncommunity post cannot bypass membership via community push")
	}
}

func TestOrdinaryPushIsUnchanged(t *testing.T) {
	payload := SendPushPayload{EntityIDs: []string{"user"}, Tokens: []string{"token"}, Body: "hello", Data: map[string]string{"sender": "user"}}
	safe, err := prepareCommunityPush(context.Background(), nil, payload)
	if err != nil || !reflect.DeepEqual(safe, payload) {
		t.Fatalf("ordinary push changed: %+v err=%v", safe, err)
	}
}

func (s *memoryFanoutStore) postScope(context.Context, string) (fanoutScope, error) {
	scope := s.scope
	if scope.PostedAt.IsZero() {
		scope.PostedAt = fanoutTestTime
	}
	return scope, s.err
}
func (s *memoryFanoutStore) commentActor(context.Context, string, string) (string, bool, time.Time, error) {
	return s.commentAuthor, s.commentAnonymous, fanoutTestTime.Add(time.Hour), s.commentErr
}
func (s *memoryFanoutStore) recipientPage(_ context.Context, scope fanoutScope, followerID, after string, limit int) ([]string, error) {
	s.followerArgs = append(s.followerArgs, followerID)
	ids := append([]string{}, s.members...)
	if scope.CommunityID == "" {
		ids = nil
	}
	if followerID != "" {
		ids = append(ids, s.followers...)
	}
	sort.Strings(ids)
	page := []string{}
	for _, id := range ids {
		if id <= after || (len(page) > 0 && page[len(page)-1] == id) {
			continue
		}
		page = append(page, id)
		if len(page) == limit {
			break
		}
	}
	return page, nil
}

func TestFollowerBackfillCannotWidenCommunityAudience(t *testing.T) {
	for _, scope := range []fanoutScope{
		{CommunityID: "community", Private: true},
		{CommunityID: "community", Anonymous: true},
	} {
		if scope.allowsFollowerFanout() {
			t.Fatalf("follower backfill widens audience: %+v", scope)
		}
	}
	for _, scope := range []fanoutScope{{}, {CommunityID: "community"}} {
		if !scope.allowsFollowerFanout() {
			t.Fatalf("public followers lost: %+v", scope)
		}
	}
}

func TestNamedCommunityCommentRetainsSocialReason(t *testing.T) {
	for _, private := range []bool{false, true} {
		store := &memoryFanoutStore{scope: fanoutScope{AuthorID: "author", Kind: "feed", Live: true, RealmID: "realm", CommunityID: "community", Private: private}, members: []string{"member"}, followers: []string{"follower"}, commentAuthor: "named-commenter"}
		var got []feedIndexRow
		err := runAllFanout(context.Background(), store, "forged", PostData{ID: "post"}, "comment", "comment", func(rows []feedIndexRow) error { got = append(got, rows...); return nil })
		if err != nil || len(got) == 0 {
			t.Fatalf("missing fanout: %v %v", got, err)
		}
		for _, row := range got {
			if row.Type != "comment" || row.TriggeredBy != "named-commenter" {
				t.Fatalf("named reason lost: %+v", row)
			}
			if private && row.RecipientID == "follower" {
				t.Fatal("private community membership leaked")
			}
		}
	}
}

func TestCommunityMediaUpdatesDoNotReachFollowers(t *testing.T) {
	scope := fanoutScope{AuthorID: "community", CommunityID: "community", Live: true, Kind: "feed"}
	if scope.allowsFollowerFanout() {
		t.Fatal("community's own system media must be member-only")
	}
}

type chatGuardDB struct {
	community bool
	err       error
	args      []any
}
type chatGuardRow struct {
	community bool
	err       error
}

func (d *chatGuardDB) QueryRow(_ context.Context, _ string, args ...any) pgx.Row {
	d.args = args
	return chatGuardRow{d.community, d.err}
}
func (r chatGuardRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	*dest[0].(*bool) = r.community
	return nil
}

func TestCommunityChatGuardRejectsTargetsActorsAndLookupFailures(t *testing.T) {
	for _, tc := range []struct {
		name      string
		community bool
		err       error
		allowed   bool
	}{
		{"community", true, nil, false}, {"ordinary chat", false, nil, true}, {"database failure", false, errors.New("offline"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := &chatGuardDB{community: tc.community, err: tc.err}
			allowed, err := communityChatAllowed(context.Background(), db, "conversation", "actor")
			if allowed != tc.allowed || !errors.Is(err, tc.err) {
				t.Fatalf("allowed=%v err=%v", allowed, err)
			}
			if !reflect.DeepEqual(db.args, []any{"conversation", "actor"}) {
				t.Fatalf("target and actor must both be checked: %v", db.args)
			}
		})
	}
}

func TestCommunityFanoutAudienceAndIdentity(t *testing.T) {
	for _, tc := range []struct {
		name               string
		private, anonymous bool
		want               []string
		follower           string
	}{
		{"public", false, false, []string{"a", "b", "f"}, "stored-author"},
		{"private", true, false, []string{"a", "b"}, ""},
		{"anonymous", false, true, []string{"a", "b"}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &memoryFanoutStore{scope: fanoutScope{AuthorID: "stored-author", Kind: "feed", Live: true, RealmID: "realm", CommunityID: "community-entity", Private: tc.private, Anonymous: tc.anonymous}, members: []string{"a", "b", "a"}, followers: []string{"b", "f"}}
			var got []string
			err := runAllFanout(context.Background(), store, "untrusted-actor", PostData{ID: "post", AuthorID: "untrusted-author"}, "", "", func(rows []feedIndexRow) error {
				for _, row := range rows {
					got = append(got, row.RecipientID)
					if row.AuthorID != "stored-author" || row.Type != "community" || row.TriggeredBy != "community-entity" {
						t.Fatalf("identity leaked via cache reason: %+v", row)
					}
				}
				return nil
			})
			if err != nil || !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("recipients=%v, err=%v; want %v", got, err, tc.want)
			}
			if store.followerArgs[0] != tc.follower {
				t.Fatalf("follower graph=%q; want %q", store.followerArgs[0], tc.follower)
			}
		})
	}
}

func TestCommunityFanoutStreamsMoreThan500Members(t *testing.T) {
	store := &memoryFanoutStore{scope: fanoutScope{AuthorID: "author", Kind: "feed", Live: true, RealmID: "realm", CommunityID: "community", Private: true}}
	for i := 0; i < 1203; i++ {
		store.members = append(store.members, fmt.Sprintf("member-%04d", i))
	}
	store.members = append(store.members, store.members...)
	seen := map[string]bool{}
	batches := 0
	err := runAllFanout(context.Background(), store, "author", PostData{ID: "post"}, "", "", func(rows []feedIndexRow) error {
		batches++
		if len(rows) > 100 {
			t.Fatalf("unbounded batch: %d", len(rows))
		}
		for _, row := range rows {
			if seen[row.RecipientID] {
				t.Fatalf("duplicate %s", row.RecipientID)
			}
			seen[row.RecipientID] = true
		}
		return nil
	})
	if err != nil || len(seen) != 1203 || batches != 13 {
		t.Fatalf("members=%d batches=%d err=%v", len(seen), batches, err)
	}
}

func TestFanoutDeliveryHasBoundedWork(t *testing.T) {
	store := &memoryFanoutStore{scope: fanoutScope{AuthorID: "author", Kind: "feed", Live: true, RealmID: "realm", CommunityID: "community", Private: true}}
	for i := 0; i < 1203; i++ {
		store.members = append(store.members, fmt.Sprintf("member-%04d", i))
	}
	written := 0
	continuations := 0
	err := runPostFanout(context.Background(), store, BulkFanoutPayload{CurrentEntityID: "author", PostData: PostData{ID: "post"}}, func(_ context.Context, _ time.Time, rows []feedIndexRow) error {
		written += len(rows)
		return nil
	}, func(context.Context, BulkFanoutPayload) error {
		continuations++
		return nil
	})
	if err != nil || written != 100 || continuations != 1 {
		t.Fatalf("one delivery wrote %d recipients (want at most 100), err=%v", written, err)
	}
}

// Drain real continuation jobs for the existing audience/privacy regressions.
// Only the broker transport is replaced; each page still resolves its scope.
func runAllFanout(ctx context.Context, store fanoutStore, actor string, post PostData, rowType, commentID string, emit func([]feedIndexRow) error) error {
	jobs := []BulkFanoutPayload{{CurrentEntityID: actor, PostData: post, Type: rowType, CommentID: commentID}}
	for len(jobs) > 0 {
		job := jobs[0]
		jobs = jobs[1:]
		if err := runPostFanout(ctx, store, job, func(_ context.Context, _ time.Time, rows []feedIndexRow) error { return emit(rows) }, func(_ context.Context, next BulkFanoutPayload) error {
			jobs = append(jobs, next)
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func TestAnonymousCommentHasNoFollowerFanout(t *testing.T) {
	store := &memoryFanoutStore{scope: fanoutScope{AuthorID: "author", Kind: "feed", Live: true}, followers: []string{"f"}, commentAuthor: "commenter", commentAnonymous: true}
	writes := 0
	err := runAllFanout(context.Background(), store, "forged", PostData{ID: "post"}, "comment", "comment-id", func(rows []feedIndexRow) error { writes += len(rows); return nil })
	if err != nil || writes != 0 || len(store.followerArgs) != 0 {
		t.Fatalf("anonymous comment fanned out: writes=%d pages=%v err=%v", writes, store.followerArgs, err)
	}
	store.commentAnonymous = false
	err = runAllFanout(context.Background(), store, "forged", PostData{ID: "post"}, "comment", "comment-id", func(rows []feedIndexRow) error { writes += len(rows); return nil })
	if err != nil || writes != 1 || store.followerArgs[0] != "commenter" {
		t.Fatalf("stored comment actor not used: writes=%d pages=%v err=%v", writes, store.followerArgs, err)
	}
}

func TestFanoutLookupAndWriteFailuresStopDelivery(t *testing.T) {
	boom := errors.New("database unavailable")
	store := &memoryFanoutStore{err: boom}
	write := func([]feedIndexRow) error { t.Fatal("scope failure must not emit cache rows"); return nil }
	if err := runAllFanout(context.Background(), store, "author", PostData{ID: "post"}, "", "", write); !errors.Is(err, boom) {
		t.Fatalf("want lookup error, got %v", err)
	}
	store.err = nil
	store.scope = fanoutScope{AuthorID: "author", Kind: "feed", Live: true}
	store.followers = []string{"f"}
	store.commentErr = boom
	if err := runAllFanout(context.Background(), store, "actor", PostData{ID: "post"}, "comment", "comment", write); !errors.Is(err, boom) {
		t.Fatalf("want comment lookup error, got %v", err)
	}
	if err := runAllFanout(context.Background(), store, "author", PostData{ID: "post"}, "", "", func([]feedIndexRow) error { return boom }); !errors.Is(err, boom) {
		t.Fatalf("want write error, got %v", err)
	}
}

func TestAuthorReceivesOnlyNamedExternalCommentResurfaces(t *testing.T) {
	for _, activity := range []string{"fanout", "comment"} {
		store := &memoryFanoutStore{scope: fanoutScope{AuthorID: "author", Kind: "feed", Live: true,
			RealmID: "realm", CommunityID: "community", Private: true}, members: []string{"author", "member"},
			commentAuthor: "friend"}
		var rows []feedIndexRow
		err := runAllFanout(context.Background(), store, "friend", PostData{ID: "post"}, activity, "comment",
			func(batch []feedIndexRow) error { rows = append(rows, batch...); return nil })
		if err != nil {
			t.Fatal(err)
		}
		sawAuthor := false
		for _, row := range rows {
			if row.RecipientID == "author" {
				sawAuthor = true
			}
		}
		if sawAuthor != (activity == "comment") {
			t.Fatalf("activity=%s rows=%+v", activity, rows)
		}
	}
}
