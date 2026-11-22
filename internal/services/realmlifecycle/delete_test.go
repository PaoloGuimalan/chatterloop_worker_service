package realmlifecycle

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

type deletionFixture struct {
	scope            DeletionScope
	fail, calls      string
	completed        map[string]bool
	sourceRowsRemain bool
	finalized        bool
}

func (s *deletionFixture) LoadDeletionScope(_ context.Context, job DeletionJob) (DeletionScope, error) {
	if s.finalized {
		return DeletionScope{NoOp: true}, nil
	}
	if s.scope.Job != job {
		return DeletionScope{}, ErrIntentMismatch
	}
	return s.scope, nil
}
func (s *deletionFixture) step(phase string) error {
	s.calls += phase + ","
	if phase == "finalize" {
		for _, prerequisite := range []string{"inventory", "calls", "prepare", "indexes", "media", "content"} {
			if !s.completed[prerequisite] {
				return errors.New("finalized before prerequisites")
			}
		}
		s.finalized, s.sourceRowsRemain = true, false
	} else if !s.sourceRowsRemain {
		return errors.New("lost source rows before cleanup completed")
	}
	s.completed[phase] = true
	if s.fail == phase {
		s.fail = ""
		return errors.New("interrupted " + phase)
	}
	return nil
}
func (s *deletionFixture) CheckInventory(context.Context) error { return s.step("inventory") }
func (s *deletionFixture) StopCalls(context.Context, DeletionScope) error {
	return s.step("calls")
}
func (s *deletionFixture) PrepareContent(context.Context, DeletionScope) error {
	return s.step("prepare")
}
func (s *deletionFixture) RemoveIndexes(context.Context, DeletionScope) error {
	return s.step("indexes")
}
func (s *deletionFixture) ReleaseMedia(context.Context, DeletionScope) error {
	return s.step("media")
}
func (s *deletionFixture) PurgeContent(context.Context, DeletionScope) error {
	return s.step("content")
}
func (s *deletionFixture) Finalize(context.Context, DeletionScope) error {
	return s.step("finalize")
}

func fixtureDeletion() *deletionFixture {
	return &deletionFixture{scope: DeletionScope{Job: DeletionJob{"realm", time.Now().UTC()}},
		completed: map[string]bool{}, sourceRowsRemain: true}
}

func TestDeletionResumesAfterEveryPhaseWithoutLosingSourceScope(t *testing.T) {
	for _, phase := range []string{"inventory", "calls", "prepare", "indexes", "media", "content", "finalize"} {
		t.Run(phase, func(t *testing.T) {
			store := fixtureDeletion()
			store.fail = phase
			job := store.scope.Job
			if err := HandleDeletion(t.Context(), store, job); err == nil {
				t.Fatal("injected failure was swallowed")
			}
			if phase != "finalize" && (!store.sourceRowsRemain || store.finalized) {
				t.Fatal("cleanup lost its restartable realm scope")
			}
			if err := HandleDeletion(t.Context(), store, job); err != nil {
				t.Fatal(err)
			}
			if !store.finalized {
				t.Fatal("retry did not converge")
			}
			before := store.calls
			if err := HandleDeletion(t.Context(), store, job); err != nil || store.calls != before {
				t.Fatal("duplicate completed job touched data again")
			}
		})
	}
}

func TestDeletionDoesNothingWithoutMatchingIntent(t *testing.T) {
	store := fixtureDeletion()
	for _, job := range []DeletionJob{{RealmID: "realm"}, {RealmID: "other", RequestedAt: store.scope.Job.RequestedAt}} {
		if err := HandleDeletion(t.Context(), store, job); !errors.Is(err, ErrIntentMismatch) {
			t.Fatalf("invalid intent accepted: %v", err)
		}
		if !reflect.DeepEqual(store.completed, map[string]bool{}) {
			t.Fatal("invalid job performed cleanup")
		}
	}
}
