// Package realmlifecycle consumes committed realm deletion intent. It never
// grants permissions or initiates deletion on the authority of a queue message.
package realmlifecycle

import (
	"errors"
	"fmt"
	"sort"
	"time"
)

var (
	ErrIntentMismatch = errors.New("realm deletion intent does not match")
	ErrInvalidScope   = errors.New("invalid realm deletion hierarchy")
	ErrOwnedRealms    = errors.New("page still owns independent realms")
)

const BatchSize = 100

type DeletionJob struct {
	RealmID     string    `json:"realm_id"`
	RequestedAt time.Time `json:"deletion_requested_at"`
}

type RealmNode struct {
	ID, RealmID, EntityID, ParentID, Kind string
	EntityKind                            string
	IsPrivate, IsActive                   bool
	RequestedAt                           *time.Time
}

type DeletionScope struct {
	Job                DeletionJob
	Nodes              []RealmNode // Children before parents; roots retained until finalization.
	RealmIDs           []string
	EntityIDs          []string
	CommunityEntityIDs []string
	ConversationIDs    []string
	RetainEntityIDs    []string // Surviving historical references outside the SQL graph.
	NoOp               bool
}

func matchingIntent(job DeletionJob, root *RealmNode) bool {
	return root != nil && job.RealmID != "" && !job.RequestedAt.IsZero() &&
		root.ID == job.RealmID && root.RequestedAt != nil && root.RequestedAt.Equal(job.RequestedAt)
}

func BuildDeletionScope(job DeletionJob, root *RealmNode, nodes []RealmNode, ownsOutside bool) (DeletionScope, error) {
	scope := DeletionScope{Job: job}
	if root == nil {
		scope.NoOp = true
		return scope, nil
	}
	if !matchingIntent(job, root) {
		return scope, ErrIntentMismatch
	}
	if root.ParentID != "" && root.Kind != "group" && root.Kind != "channel" && root.Kind != "voice" {
		return scope, ErrInvalidScope
	}
	if root.Kind == "page" && ownsOutside {
		return scope, ErrOwnedRealms
	}
	byID := map[string]RealmNode{}
	children := map[string][]string{}
	for _, node := range nodes {
		if _, duplicate := byID[node.ID]; duplicate {
			return scope, ErrInvalidScope
		}
		byID[node.ID] = node
		children[node.ParentID] = append(children[node.ParentID], node.ID)
	}
	if _, present := byID[root.ID]; !present {
		return scope, ErrInvalidScope
	}
	for parent := range children {
		sort.Strings(children[parent])
	}
	visiting, visited := map[string]bool{}, map[string]bool{}
	var visit func(string) error
	visit = func(id string) error {
		if visiting[id] || visited[id] {
			return ErrInvalidScope
		}
		visiting[id] = true
		node := byID[id]
		if node.ID == "" || node.RealmID != node.ID || node.EntityID == "" || node.EntityKind != "realm" {
			return ErrInvalidScope
		}
		switch node.Kind {
		case "community", "page", "server", "group", "conference", "channel", "voice":
		default:
			return fmt.Errorf("%w: unsupported kind %q", ErrInvalidScope, node.Kind)
		}
		for _, child := range children[id] {
			kind := byID[child].Kind
			if node.Kind != "server" || (kind != "group" && kind != "channel" && kind != "voice") {
				return ErrInvalidScope
			}
			if err := visit(child); err != nil {
				return err
			}
		}
		visiting[id], visited[id] = false, true
		scope.Nodes = append(scope.Nodes, node)
		scope.RealmIDs = append(scope.RealmIDs, node.ID)
		scope.EntityIDs = append(scope.EntityIDs, node.EntityID)
		scope.ConversationIDs = append(scope.ConversationIDs, node.RealmID)
		if node.Kind == "community" {
			scope.CommunityEntityIDs = append(scope.CommunityEntityIDs, node.EntityID)
		}
		return nil
	}
	if err := visit(root.ID); err != nil {
		return DeletionScope{}, err
	}
	return scope, nil
}
