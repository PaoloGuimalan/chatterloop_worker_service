package realmlifecycle

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func fixtureNode(id, kind, parent string) RealmNode {
	return RealmNode{ID: id, RealmID: id, EntityID: id + "-entity", EntityKind: "realm", Kind: kind, ParentID: parent}
}

func TestScopeRequiresExactCommittedIntent(t *testing.T) {
	stamp := time.Date(2026, 11, 21, 0, 0, 0, 0, time.UTC)
	job := DeletionJob{RealmID: "realm", RequestedAt: stamp}
	for _, marked := range []*time.Time{nil, ptrTime(stamp.Add(time.Second))} {
		root := fixtureNode("realm", "community", "")
		root.RequestedAt = marked
		if _, err := BuildDeletionScope(job, &root, []RealmNode{root}, false); !errors.Is(err, ErrIntentMismatch) {
			t.Fatalf("unmarked/mismatched job accepted: %v", err)
		}
	}
	scope, err := BuildDeletionScope(job, nil, nil, false)
	if err != nil || !scope.NoOp {
		t.Fatalf("already deleted root must be a successful no-op: %+v %v", scope, err)
	}
}

func TestScopeEveryRealmKind(t *testing.T) {
	stamp := time.Now().UTC()
	for _, kind := range []string{"community", "page", "server", "group", "conference", "channel", "voice"} {
		t.Run(kind, func(t *testing.T) {
			root := fixtureNode("realm", kind, "")
			root.RequestedAt = &stamp
			scope, err := BuildDeletionScope(DeletionJob{"realm", stamp}, &root, []RealmNode{root}, false)
			if err != nil || len(scope.Nodes) != 1 || scope.EntityIDs[0] != root.EntityID {
				t.Fatalf("wrong scope: %+v %v", scope, err)
			}
		})
	}
}

func TestDeletionRejectsUnsupportedDescendantKinds(t *testing.T) {
	stamp := time.Now().UTC()
	root := fixtureNode("server", "server", "")
	root.RequestedAt = &stamp
	for _, kind := range []string{"page", "community", "server", "conference"} {
		child := fixtureNode("child", kind, root.ID)
		if _, err := BuildDeletionScope(DeletionJob{root.ID, stamp}, &root, []RealmNode{root, child}, false); !errors.Is(err, ErrInvalidScope) {
			t.Fatalf("unsafe %s descendant accepted: %v", kind, err)
		}
	}
}

func TestScopeServerIncludesPrivateVoiceAndLegacyChildrenButNoOtherRoot(t *testing.T) {
	stamp := time.Now().UTC()
	root := fixtureNode("server", "server", "")
	root.RequestedAt = &stamp
	public := fixtureNode("public", "channel", root.ID)
	private := fixtureNode("private", "channel", root.ID)
	private.IsPrivate = true
	voice := fixtureNode("voice", "voice", root.ID)
	legacy := fixtureNode("legacy", "group", root.ID)
	outside := fixtureNode("outside", "community", "")
	scope, err := BuildDeletionScope(DeletionJob{root.ID, stamp}, &root,
		[]RealmNode{outside, voice, root, public, private, legacy}, false)
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, len(scope.Nodes))
	for i, node := range scope.Nodes {
		ids[i] = node.ID
	}
	if !reflect.DeepEqual(ids, []string{"legacy", "private", "public", "voice", "server"}) {
		t.Fatalf("must be deterministic child-first and exclude other roots: %v", ids)
	}
	private.RequestedAt = &stamp
	single, err := BuildDeletionScope(DeletionJob{private.ID, stamp}, &private,
		[]RealmNode{root, public, private, voice}, false)
	if err != nil || len(single.Nodes) != 1 || single.Nodes[0].ID != private.ID {
		t.Fatalf("channel deletion included parent or siblings: %+v %v", single, err)
	}
}

func TestScopeRejectsCyclesAndPageOwnershipBlockers(t *testing.T) {
	stamp := time.Now().UTC()
	root := fixtureNode("root", "page", "")
	root.RequestedAt = &stamp
	if _, err := BuildDeletionScope(DeletionJob{root.ID, stamp}, &root, []RealmNode{root}, true); !errors.Is(err, ErrOwnedRealms) {
		t.Fatalf("page ownership dependency ignored: %v", err)
	}
	root.Kind, root.ParentID = "server", "child"
	child := fixtureNode("child", "channel", root.ID)
	if _, err := BuildDeletionScope(DeletionJob{root.ID, stamp}, &root, []RealmNode{root, child}, false); !errors.Is(err, ErrInvalidScope) {
		t.Fatalf("cycle accepted: %v", err)
	}
}

func ptrTime(value time.Time) *time.Time { return &value }

func TestScopeCannotDeleteAPersonalOrBotIdentityThroughMalformedRealmData(t *testing.T) {
	stamp := time.Now().UTC()
	for _, kind := range []string{"user", "bot", ""} {
		root := fixtureNode("realm", "page", "")
		root.EntityKind, root.RequestedAt = kind, &stamp
		if _, err := BuildDeletionScope(DeletionJob{root.ID, stamp}, &root, []RealmNode{root}, false); !errors.Is(err, ErrInvalidScope) {
			t.Fatalf("non-realm identity entered deletion scope: %s %v", kind, err)
		}
	}
}
