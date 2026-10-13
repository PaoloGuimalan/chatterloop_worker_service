package media

import (
	"strings"
	"testing"
)

func TestNotesFolderCleanupAndReattachment(t *testing.T) {
	raw := "https://media.neonsystems.net/uploads/notes/account/folder/Plan.pdf"
	target := &Target{Type: "note", ID: "note1"}
	for _, inUse := range []bool{false, true} {
		store := &fakeStore{record: &FileRecord{Status: "attached", AttachedTo: []Target{*target}}, inUse: inUse}
		bucket := &fakeBucket{}
		outcome := release(t, store, bucket, raw, target, false)
		if inUse {
			if outcome.Decision != "in_use" || len(bucket.removed) != 0 {
				t.Fatalf("reattached file: %+v", outcome)
			}
		} else if outcome.Decision != "deleted" || len(bucket.removed) != 1 {
			t.Fatalf("unreferenced note: %+v", outcome)
		}
	}
}

func TestNotesLivenessQuery(t *testing.T) {
	query := targetLivenessSQL("note")
	if !strings.Contains(query, "FROM notes_entry WHERE id = $1") {
		t.Fatalf("wrong Notes query: %s", query)
	}
}
