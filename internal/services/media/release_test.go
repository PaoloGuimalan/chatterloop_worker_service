package media

import (
	"context"
	"reflect"
	"testing"
)

var spaces = NewStorage(
	"neon-systems-bucket",
	"https://sgp1.digitaloceanspaces.com",
	"sgp1.cdn.digitaloceanspaces.com",
	"https://media.neonsystems.net/",
	nil,
)

func TestKeyFromURL(t *testing.T) {
	const key = "uploads/entries/9/x y.jpg"
	for _, u := range []string{
		"https://media.neonsystems.net/uploads/entries/9/x%20y.jpg",
		"https://neon-systems-bucket.sgp1.cdn.digitaloceanspaces.com/uploads/entries/9/x%20y.jpg",
		"https://neon-systems-bucket.sgp1.digitaloceanspaces.com/uploads/entries/9/x%20y.jpg",
		"https://sgp1.digitaloceanspaces.com/neon-systems-bucket/uploads/entries/9/x%20y.jpg",
	} {
		if got := spaces.KeyFromURL(u); got != key {
			t.Errorf("KeyFromURL(%s) = %q, want %q", u, got, key)
		}
	}
	for _, u := range []string{
		"https://storage.googleapis.com/x/a.png",
		"https://other.sgp1.cdn.digitaloceanspaces.com/uploads/a.png",
		"https://sgp1.digitaloceanspaces.com/other-bucket/uploads/a.png",
		"not a url",
	} {
		if got := spaces.KeyFromURL(u); got != "" {
			t.Errorf("KeyFromURL(%s) = %q, want none", u, got)
		}
	}
}

// fakes

type fakeStore struct {
	record    *FileRecord
	reported  bool
	inUse     bool
	live      map[string]bool // target id -> live
	saved     []FileRecord
	usedCheck *Target
}

func (f *fakeStore) FindRecord(context.Context, string, string) (*FileRecord, error) {
	return f.record, nil
}
func (f *fakeStore) SaveRecord(_ context.Context, r *FileRecord) error {
	f.saved = append(f.saved, *r)
	return nil
}
func (f *fakeStore) IsReported(context.Context, Target) (bool, error) { return f.reported, nil }
func (f *fakeStore) IsTargetLive(_ context.Context, t Target) (bool, error) {
	return f.live[t.ID], nil
}
func (f *fakeStore) URLInUse(_ context.Context, _, _ string, except *Target, _ string) (bool, error) {
	f.usedCheck = except
	return f.inUse, nil
}

type fakeBucket struct {
	removed []string
	aborted []string
}

func (b *fakeBucket) KeyFromURL(raw string) string { return spaces.KeyFromURL(raw) }
func (b *fakeBucket) Remove(_ context.Context, key string) error {
	b.removed = append(b.removed, key)
	return nil
}
func (b *fakeBucket) AbortMultipart(_ context.Context, key, _ string) error {
	b.aborted = append(b.aborted, key)
	return nil
}

const ours = "https://media.neonsystems.net/uploads/entries/7/f/photo.jpg"

func post(id string) *Target { return &Target{Type: "post", ID: id} }

func release(t *testing.T, s *fakeStore, b *fakeBucket, url string, target *Target, dry bool) Outcome {
	t.Helper()
	out, err := ReleaseFile(context.Background(), s, b, url, target, "", dry)
	if err != nil {
		t.Fatalf("ReleaseFile: %v", err)
	}
	return out
}

func TestUnusedFileOfDeletedContentIsRemoved(t *testing.T) {
	s := &fakeStore{record: &FileRecord{Status: "attached", AttachedTo: []Target{*post("p1")}}}
	b := &fakeBucket{}
	out := release(t, s, b, ours, post("p1"), false)
	if out.Decision != "deleted" {
		t.Fatalf("decision = %s", out.Decision)
	}
	if !reflect.DeepEqual(b.removed, []string{"uploads/entries/7/f/photo.jpg"}) {
		t.Fatalf("removed = %v", b.removed)
	}
	if got := s.saved[len(s.saved)-1].Status; got != "deleted" {
		t.Fatalf("record status = %s", got)
	}
}

func TestReportedContentIsHeld(t *testing.T) {
	s := &fakeStore{record: &FileRecord{Status: "attached", AttachedTo: []Target{*post("p1")}}, reported: true}
	b := &fakeBucket{}
	out := release(t, s, b, ours, post("p1"), false)
	if out.Decision != "held" || len(b.removed) != 0 {
		t.Fatalf("decision = %s, removed = %v", out.Decision, b.removed)
	}
	if got := s.saved[len(s.saved)-1].Status; got != "held" {
		t.Fatalf("record status = %s", got)
	}
}

func TestFileUsedByAnotherLivePostIsKept(t *testing.T) {
	s := &fakeStore{
		record: &FileRecord{Status: "attached", AttachedTo: []Target{*post("p1"), *post("p2")}},
		live:   map[string]bool{"p2": true},
	}
	b := &fakeBucket{}
	out := release(t, s, b, ours, post("p1"), false)
	if out.Decision != "in_use" || len(b.removed) != 0 {
		t.Fatalf("decision = %s, removed = %v", out.Decision, b.removed)
	}
	// p1 is no longer recorded as a user; p2 still is.
	if got := s.saved[len(s.saved)-1].AttachedTo; !reflect.DeepEqual(got, []Target{*post("p2")}) {
		t.Fatalf("attachedTo = %v", got)
	}
}

func TestFileStillUsedByURLIsKept(t *testing.T) {
	s := &fakeStore{inUse: true}
	b := &fakeBucket{}
	out := release(t, s, b, ours, post("p1"), false)
	if out.Decision != "in_use" || len(b.removed) != 0 {
		t.Fatalf("decision = %s, removed = %v", out.Decision, b.removed)
	}
	if s.usedCheck == nil || s.usedCheck.ID != "p1" {
		t.Fatalf("usage check must exclude the deleted post, got %v", s.usedCheck)
	}
}

func TestNothingOutsideChatterloopIsTouched(t *testing.T) {
	for _, u := range []string{
		"https://media.neonsystems.net/pos/receipts/1.pdf",
		"https://media.neonsystems.net/uploads/invoices/1.pdf",
		"https://storage.googleapis.com/b/a.png",
		"https://example.com/a.png",
	} {
		b := &fakeBucket{}
		out := release(t, &fakeStore{}, b, u, post("p1"), false)
		if out.Decision != "skipped" || len(b.removed) != 0 {
			t.Errorf("%s: decision = %s, removed = %v", u, out.Decision, b.removed)
		}
	}
}

func TestDryRunNeverWrites(t *testing.T) {
	s := &fakeStore{record: &FileRecord{Status: "ready"}}
	b := &fakeBucket{}
	out := release(t, s, b, ours, nil, true)
	if out.Decision != "would_delete" || len(b.removed) != 0 || len(s.saved) != 0 {
		t.Fatalf("decision = %s, removed = %v, saved = %v", out.Decision, b.removed, s.saved)
	}
}

func TestLegacyNameSuffixIsIgnored(t *testing.T) {
	b := &fakeBucket{}
	out := release(t, &fakeStore{}, b, ours+"%%%photo.jpg", post("p1"), false)
	if out.Decision != "deleted" || !reflect.DeepEqual(b.removed, []string{"uploads/entries/7/f/photo.jpg"}) {
		t.Fatalf("decision = %s, removed = %v", out.Decision, b.removed)
	}
}

func TestAlreadyDeletedIsNotDeletedTwice(t *testing.T) {
	b := &fakeBucket{}
	out := release(t, &fakeStore{record: &FileRecord{Status: "deleted"}}, b, ours, post("p1"), false)
	if out.Decision != "missing" || len(b.removed) != 0 {
		t.Fatalf("decision = %s, removed = %v", out.Decision, b.removed)
	}
}

func TestUnfinishedMultipartIsAborted(t *testing.T) {
	r := &FileRecord{Status: "pending"}
	r.Multipart.UploadID = "UP"
	b := &fakeBucket{}
	out := release(t, &fakeStore{record: r}, b, ours, nil, false)
	if out.Decision != "deleted" || len(b.aborted) != 1 {
		t.Fatalf("decision = %s, aborted = %v", out.Decision, b.aborted)
	}
}

func TestHandleReleaseWithoutStorageDoesNothing(t *testing.T) {
	if err := HandleRelease(context.Background(), &fakeStore{}, nil, ReleasePayload{
		Items: []ReleaseItem{{Target: post("p1"), URLs: []string{ours}}},
	}); err != nil {
		t.Fatal(err)
	}
}
