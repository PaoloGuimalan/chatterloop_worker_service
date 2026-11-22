package realmlifecycle

import (
	"context"
	"fmt"
)

type DeletionStore interface {
	LoadDeletionScope(context.Context, DeletionJob) (DeletionScope, error)
	CheckInventory(context.Context) error
	StopCalls(context.Context, DeletionScope) error
	PrepareContent(context.Context, DeletionScope) error
	RemoveIndexes(context.Context, DeletionScope) error
	ReleaseMedia(context.Context, DeletionScope) error
	PurgeContent(context.Context, DeletionScope) error
	Finalize(context.Context, DeletionScope) error
}

// Every step is restartable from existing rows. No phase checkpoint collection
// is needed: intent and media-bearing references survive until finalization.
func HandleDeletion(ctx context.Context, store DeletionStore, job DeletionJob) error {
	if job.RealmID == "" || job.RequestedAt.IsZero() {
		return ErrIntentMismatch
	}
	scope, err := store.LoadDeletionScope(ctx, job)
	if err != nil || scope.NoOp {
		return err
	}
	if err := store.CheckInventory(ctx); err != nil {
		return fmt.Errorf("realm cleanup inventory: %w", err)
	}
	steps := []struct {
		name string
		run  func(context.Context, DeletionScope) error
	}{
		{"call shutdown", store.StopCalls},
		{"prepare content", store.PrepareContent},
		{"remove indexes", store.RemoveIndexes},
		{"release media", store.ReleaseMedia},
		{"purge content", store.PurgeContent},
		{"finalize realm", store.Finalize},
	}
	for _, step := range steps {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := step.run(ctx, scope); err != nil {
			return fmt.Errorf("realm %s %s: %w", job.RealmID, step.name, err)
		}
		Progress(ctx)
	}
	return nil
}
