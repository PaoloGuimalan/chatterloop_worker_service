package media

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"
)

// Every folder Chatterloop has ever written into (realm folders are named
// after the realm type). The bucket is shared with NeonSystems: nothing
// outside these folders is ever touched.
var chatterloopKey = regexp.MustCompile(
	`^uploads/(entries|messages|posts|comments|pages|realms|servers|groups|channels|voices|conferences)/`,
)

// Report target types, as entity_report stores them.
var reportable = map[string]bool{"post": true, "comment": true, "message": true}

// Target is the content a file belonged to.
type Target struct {
	Type string `json:"type" bson:"type"`
	ID   string `json:"id" bson:"id"`
}

func (t *Target) same(o Target) bool { return t != nil && t.Type == o.Type && t.ID == o.ID }

// ReleaseItem is one deleted thing and the files it used.
type ReleaseItem struct {
	Target  *Target  `json:"target"`
	URLs    []string `json:"urls"`
	Context struct {
		ConversationID string `json:"conversationID"`
	} `json:"context"`
}

// ReleasePayload is a media_release job, published by the Node server
// (reusables/media/release.js publishRelease), user_service
// (newsfeed/services/media_release.py) and cron_service's media_cleanup sweep.
//
// DryRun (only the sweep's --preview sets it) decides every file and logs the
// decision, but writes and deletes nothing.
type ReleasePayload struct {
	Items  []ReleaseItem `json:"items"`
	DryRun bool          `json:"dryRun"`
	Source string        `json:"source"`
}

// FileRecord is the part of a `files` document this package reads and writes.
// Older records (no version) carry none of these but the URL.
type FileRecord struct {
	Ref        any      `bson:"_id"`
	Version    int      `bson:"version"`
	Status     string   `bson:"status"`
	Key        string   `bson:"key"`
	AttachedTo []Target `bson:"attachedTo"`
	Multipart  struct {
		UploadID string `bson:"uploadId"`
	} `bson:"multipart"`
	FileDetails struct {
		Data string `bson:"data"`
	} `bson:"fileDetails"`
	HoldReason string    `bson:"holdReason,omitempty"`
	DeletedAt  time.Time `bson:"deletedAt,omitempty"`
}

// Store is everything a release decision reads and writes.
type Store interface {
	FindRecord(ctx context.Context, url, key string) (*FileRecord, error)
	SaveRecord(ctx context.Context, r *FileRecord) error
	IsReported(ctx context.Context, t Target) (bool, error)
	IsTargetLive(ctx context.Context, t Target) (bool, error)
	URLInUse(ctx context.Context, url, key string, except *Target, conversationID string) (bool, error)
}

// Bucket is what a release does to storage.
type Bucket interface {
	KeyFromURL(raw string) string
	Remove(ctx context.Context, key string) error
	AbortMultipart(ctx context.Context, key, uploadID string) error
}

// Outcome is what happened to one file: deleted | held | in_use | skipped |
// missing, or would_delete on a dry run.
type Outcome struct {
	URL      string
	Decision string
	Reason   string
}

// URLOf is the URL half of a stored value ("url%%%name" in old rows).
func URLOf(value string) string {
	return strings.TrimSpace(strings.SplitN(value, "%%%", 2)[0])
}

// ReleaseFile decides one file and acts on it (unless dryRun):
//
//  1. not a link to our bucket, or outside Chatterloop's folders -> skipped
//  2. the content being deleted was reported (any report, any status)
//     -> held, never deleted automatically for now
//  3. still used by something live - another post, a message, a comment, an
//     avatar or cover, a moment poster -> kept
//  4. otherwise -> removed from storage, record marked deleted
func ReleaseFile(ctx context.Context, store Store, bucket Bucket, rawURL string, target *Target, conversationID string, dryRun bool) (Outcome, error) {
	u := URLOf(rawURL)
	out := func(decision, reason string) (Outcome, error) {
		return Outcome{URL: u, Decision: decision, Reason: reason}, nil
	}

	key := bucket.KeyFromURL(u)
	if key == "" {
		return out("skipped", "not in our storage")
	}
	if !chatterloopKey.MatchString(key) {
		return out("skipped", "outside Chatterloop's folders")
	}

	record, err := store.FindRecord(ctx, u, key)
	if err != nil {
		return Outcome{}, fmt.Errorf("find record: %w", err)
	}
	if record != nil && record.Status == "deleted" {
		return out("missing", "already deleted")
	}

	// The thing being deleted no longer counts as a user of the file.
	var others []Target
	if record != nil {
		for _, a := range record.AttachedTo {
			if !target.same(a) {
				others = append(others, a)
			}
		}
	}
	save := func() error {
		if record == nil || dryRun {
			return nil
		}
		record.AttachedTo = others
		return store.SaveRecord(ctx, record)
	}

	reported := false
	if target != nil && reportable[target.Type] {
		if reported, err = store.IsReported(ctx, *target); err != nil {
			return Outcome{}, fmt.Errorf("report check: %w", err)
		}
	}
	if reported || (record != nil && record.Status == "held") {
		if record != nil && !dryRun {
			record.Status = "held"
			record.HoldReason = "reported"
		}
		if err := save(); err != nil {
			return Outcome{}, err
		}
		return out("held", "its content was reported")
	}

	for _, other := range others {
		live, err := store.IsTargetLive(ctx, other)
		if err != nil {
			return Outcome{}, fmt.Errorf("liveness check: %w", err)
		}
		if live {
			if err := save(); err != nil {
				return Outcome{}, err
			}
			return out("in_use", fmt.Sprintf("still used by %s %s", other.Type, other.ID))
		}
	}

	inUse, err := store.URLInUse(ctx, u, key, target, conversationID)
	if err != nil {
		return Outcome{}, fmt.Errorf("usage check: %w", err)
	}
	if inUse {
		if err := save(); err != nil {
			return Outcome{}, err
		}
		return out("in_use", "still used elsewhere")
	}

	if dryRun {
		return out("would_delete", "unused")
	}

	if record != nil && record.Status == "pending" && record.Multipart.UploadID != "" {
		if err := bucket.AbortMultipart(ctx, key, record.Multipart.UploadID); err != nil {
			return Outcome{}, fmt.Errorf("abort multipart: %w", err)
		}
	}
	if err := bucket.Remove(ctx, key); err != nil {
		return Outcome{}, fmt.Errorf("remove: %w", err)
	}
	if record != nil {
		record.Status = "deleted"
		record.DeletedAt = time.Now()
		if err := save(); err != nil {
			return Outcome{}, err
		}
	}
	reason := "unused"
	if target != nil {
		reason = target.Type + " deleted"
	}
	return out("deleted", reason)
}

// HandleRelease runs one media_release job. An error makes the consumer retry
// it once; whatever is still left behind after that, the cleanup sweeps find.
func HandleRelease(ctx context.Context, store Store, bucket Bucket, job ReleasePayload) error {
	if bucket == nil {
		slog.Warn("media_release: storage not configured, nothing deleted")
		return nil
	}
	deleted := 0
	for _, item := range job.Items {
		for _, raw := range item.URLs {
			outcome, err := ReleaseFile(ctx, store, bucket, raw, item.Target, item.Context.ConversationID, job.DryRun)
			if err != nil {
				return fmt.Errorf("release %s: %w", raw, err)
			}
			if job.DryRun {
				slog.Info("media_release preview",
					"source", job.Source, "decision", outcome.Decision,
					"url", outcome.URL, "reason", outcome.Reason)
			}
			if outcome.Decision == "deleted" {
				deleted++
			}
		}
	}
	if deleted > 0 {
		slog.Info("media_release: deleted files", "count", deleted, "source", job.Source)
	}
	return nil
}
