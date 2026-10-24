package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	amqp "github.com/rabbitmq/amqp091-go"
)

var fanoutTestTime = time.Date(2026, 10, 25, 1, 2, 3, 0, time.UTC)

func fanoutJobStore(n int) *memoryFanoutStore {
	s := &memoryFanoutStore{scope: fanoutScope{AuthorID: "stored-author", Kind: "feed", Live: true, RealmID: "realm", CommunityID: "community", Private: true, PostedAt: fanoutTestTime}}
	for i := 0; i < n; i++ {
		s.members = append(s.members, fmt.Sprintf("member-%04d", i))
	}
	return s
}

func TestFanoutContinuationDeliversEveryMemberWithStableKeys(t *testing.T) {
	s := fanoutJobStore(1203)
	jobs := []BulkFanoutPayload{{PostData: PostData{ID: "post", AuthorID: "forged"}, CurrentEntityID: "forged"}}
	seen := map[string]bool{}
	for len(jobs) > 0 {
		job := jobs[0]
		jobs = jobs[1:]
		writes := 0
		err := runPostFanout(context.Background(), s, job, func(_ context.Context, at time.Time, rows []feedIndexRow) error {
			if !at.Equal(fanoutTestTime) {
				t.Fatalf("unstable key time: %v", at)
			}
			writes += len(rows)
			for _, row := range rows {
				if seen[row.RecipientID] || row.AuthorID != "stored-author" || row.Type != "community" || row.TriggeredBy != "community" {
					t.Fatalf("duplicate or unsafe row: %+v", row)
				}
				seen[row.RecipientID] = true
			}
			return nil
		}, func(_ context.Context, next BulkFanoutPayload) error {
			// Exercise the wire shape, including the timestamp's round trip.
			body, err := json.Marshal(next)
			if err != nil {
				return err
			}
			var decoded BulkFanoutPayload
			if err := json.Unmarshal(body, &decoded); err != nil {
				return err
			}
			if decoded.AfterRecipient <= job.AfterRecipient {
				t.Fatal("cursor did not advance")
			}
			jobs = append(jobs, decoded)
			return nil
		})
		if err != nil || writes > 100 {
			t.Fatalf("writes=%d err=%v", writes, err)
		}
	}
	if len(seen) != 1203 {
		t.Fatalf("delivered %d of 1203 members", len(seen))
	}
}

func TestFanoutFailuresRetrySamePageAndTimestamp(t *testing.T) {
	for _, failure := range []string{"write", "publish", "deadline after write"} {
		t.Run(failure, func(t *testing.T) {
			s := fanoutJobStore(201)
			job := BulkFanoutPayload{PostData: PostData{ID: "post"}, AfterRecipient: "member-0099", IndexedAt: fanoutTestTime}
			boom := errors.New("offline")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			published := 0
			keys := map[string]time.Time{}
			write := func(_ context.Context, at time.Time, rows []feedIndexRow) error {
				if rows[0].RecipientID != "member-0100" {
					t.Fatalf("restarted prefix: %+v", rows[0])
				}
				for _, r := range rows {
					if prior, ok := keys[r.RecipientID]; ok && !prior.Equal(at) {
						t.Fatalf("retry changed Cassandra primary key")
					}
					keys[r.RecipientID] = at
				}
				if failure == "write" {
					return boom
				}
				if failure == "deadline after write" {
					cancel()
				}
				return nil
			}
			err := runPostFanout(ctx, s, job, write, func(context.Context, BulkFanoutPayload) error { published++; return boom })
			if err == nil {
				t.Fatal("failed delivery/enqueue was reported as success")
			}
			if failure != "publish" && published != 0 {
				t.Fatal("advanced cursor past failed/cancelled batch")
			}
			err = runPostFanout(context.Background(), s, job, func(_ context.Context, at time.Time, rows []feedIndexRow) error {
				for _, r := range rows {
					if !keys[r.RecipientID].Equal(at) {
						t.Fatal("retry time changed")
					}
				}
				return nil
			}, func(_ context.Context, next BulkFanoutPayload) error {
				if next.AfterRecipient != "member-0199" || !next.IndexedAt.Equal(fanoutTestTime) {
					t.Fatalf("wrong checkpoint: %+v", next)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestFanoutContinuationRechecksPrivacyMembershipAndStoredAuthor(t *testing.T) {
	s := fanoutJobStore(101)
	var next BulkFanoutPayload
	err := runPostFanout(context.Background(), s, BulkFanoutPayload{PostData: PostData{ID: "post"}}, func(context.Context, time.Time, []feedIndexRow) error { return nil }, func(_ context.Context, p BulkFanoutPayload) error { next = p; return nil })
	if err != nil {
		t.Fatal(err)
	}
	s.scope.AuthorID, s.scope.Anonymous = "new-stored-author", true
	s.members = []string{"member-0101"} // member-0100 left before this continuation.
	s.followers = []string{"outsider"}
	var got []feedIndexRow
	err = runPostFanout(context.Background(), s, next, func(_ context.Context, _ time.Time, rows []feedIndexRow) error {
		got = append(got, rows...)
		return nil
	}, func(context.Context, BulkFanoutPayload) error { t.Fatal("unexpected continuation"); return nil })
	if err != nil || len(got) != 1 || got[0].RecipientID != "member-0101" || got[0].AuthorID != "new-stored-author" || got[0].TriggeredBy != "community" {
		t.Fatalf("stale scope or membership: %+v err=%v", got, err)
	}
}

func TestFanoutQueueAdapterPreservesFailuresOnRedelivery(t *testing.T) {
	for _, redelivered := range []bool{false, true} {
		s := fanoutJobStore(101)
		cfg := bulkFanoutConsumer(func(ctx context.Context, p BulkFanoutPayload) error {
			return runPostFanout(ctx, s, p, func(context.Context, time.Time, []feedIndexRow) error { return nil }, func(context.Context, BulkFanoutPayload) error { return ErrNotConnected })
		})
		ack := &fakeAcknowledger{}
		(&RabbitMQ{}).process(cfg, amqp.Delivery{Acknowledger: ack, Redelivered: redelivered, Body: []byte(`{"post_data":{"id":"post"}}`)})
		if ack.acked || !ack.nacked || !ack.nackRequeued {
			t.Fatalf("failed continuation ACKed on redelivery=%v: %+v", redelivered, ack)
		}
		ack = &fakeAcknowledger{}
		(&RabbitMQ{}).process(cfg, amqp.Delivery{Acknowledger: ack, Body: []byte(`invalid json`)})
		if !ack.acked || ack.nacked {
			t.Fatal("malformed fanout job must be dropped")
		}
	}
}

func TestFanoutMissingActivityFinishesWithoutRetry(t *testing.T) {
	for _, comment := range []bool{false, true} {
		s := fanoutJobStore(101)
		job := BulkFanoutPayload{PostData: PostData{ID: "post"}}
		if comment {
			job.Type, job.CommentID, s.commentErr = "comment", "comment", pgx.ErrNoRows
		} else {
			s.err = pgx.ErrNoRows
		}
		err := runPostFanout(context.Background(), s, job, func(context.Context, time.Time, []feedIndexRow) error {
			t.Fatal("missing activity wrote rows")
			return nil
		}, func(context.Context, BulkFanoutPayload) error { t.Fatal("missing activity continued"); return nil })
		if err != nil {
			t.Fatalf("removed activity must finish, got %v", err)
		}
	}
}

type invalidScopeDB struct {
	destinations int
	kind         string
}
type invalidScopeRow struct{ invalidScopeDB }

func (d invalidScopeDB) QueryRow(context.Context, string, ...any) pgx.Row { return invalidScopeRow{d} }
func (d invalidScopeDB) Query(context.Context, string, ...any) (pgx.Rows, error) {
	panic("invalid scope must not query recipients")
}
func (r invalidScopeRow) Scan(dest ...any) error {
	*dest[0].(*string), *dest[1].(*string), *dest[2].(*bool) = "author", r.kind, true
	*dest[3].(*bool), *dest[4].(*string), *dest[5].(*string) = false, "realm", "community"
	*dest[6].(*bool), *dest[7].(*int), *dest[8].(*time.Time) = true, r.destinations, fanoutTestTime
	return nil
}

func TestFanoutInvalidStoredScopeIsPermanent(t *testing.T) {
	for _, db := range []invalidScopeDB{{2, "feed"}, {1, "moment"}} {
		_, err := (postgresFanoutStore{db: db}).postScope(context.Background(), "post")
		if !errors.Is(err, ErrDrop) {
			t.Fatalf("invalid stored scope will requeue forever: %v", err)
		}
	}
}

func TestFanoutLegacyRetriesUseStoredActivityTime(t *testing.T) {
	for _, comment := range []bool{false, true} {
		s := fanoutJobStore(101)
		s.scope.Private = false
		s.followers = []string{"follower"}
		job := BulkFanoutPayload{PostData: PostData{ID: "post"}}
		want := fanoutTestTime
		if comment {
			job.Type, job.CommentID, s.commentAuthor = "comment", "comment", "commenter"
			want = fanoutTestTime.Add(time.Hour)
		}
		for attempt := 0; attempt < 2; attempt++ {
			err := runPostFanout(context.Background(), s, job, func(_ context.Context, at time.Time, rows []feedIndexRow) error {
				if !at.Equal(want) {
					t.Fatalf("legacy retry used processing time instead of activity: %v want %v", at, want)
				}
				return nil
			}, func(context.Context, BulkFanoutPayload) error { return ErrNotConnected })
			if !errors.Is(err, ErrNotConnected) {
				t.Fatalf("missing publish failure: %v", err)
			}
		}
	}
}
