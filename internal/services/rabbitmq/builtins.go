package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"worker_service/internal/connections"

	"go.mongodb.org/mongo-driver/bson"
)

// The built-in commands.
//
// # WHY THESE THREE, AND WHY THEY ARE CHEAP
//
// Each answers from a query, not from a model. Asking who is in a conversation
// is a lookup, and paying for a language model to read a member list back
// would be slow, expensive and less accurate than the list itself.
//
// # EACH ONE IS A PURE FUNCTION OF THE ENVELOPE
//
// They return text; runSystemCommand posts it. None of them touches the send
// path, so none of them can get it wrong, and every one of them is testable
// without a conversation to post into.
//
// # REGISTERED BY NAME, MATCHING A ROW
//
// A built-in runs only when a bot_commands row names it, so the set that
// exists in the database and the set this build implements are two halves of
// the same thing. The row makes it resolvable and describes it for /help; this
// file makes it do something. bot/migrations/0005_system_commands.py writes
// the rows.

// memberLimit caps how many participants /members names.
//
// A realm can hold thousands, and a reply naming all of them would be a wall
// of text nobody reads and a message body large enough to hurt every client
// that loads that conversation's history afterwards.
const memberLimit = 50

func init() {
	RegisterSystemCommand("members", membersCommand)
	RegisterSystemCommand("created", createdCommand)
	RegisterSystemCommand("help", helpCommand)
}

// membersCommand answers with who is in this conversation.
//
// Handles are written WITHOUT a leading @. The send path deliberately does not
// resolve mentions for a system reply - see systemreply.go - so an @handle
// here would look like a mention, and be the one thing in the message that
// does not behave like one.
func membersCommand(ctx context.Context, envelope json.RawMessage) (string, error) {
	var parsed commandEnvelope
	if err := json.Unmarshal(envelope, &parsed); err != nil {
		return "", fmt.Errorf("bad envelope: %w", err)
	}

	participants, _ := conversationFacts(ctx, parsed.Conversation.ID)
	if len(participants) == 0 {
		return "I could not read this conversation's members.", nil
	}

	people, err := resolveEntities(ctx, participants)
	if err != nil {
		return "", err
	}
	if len(people) == 0 {
		return "I could not read this conversation's members.", nil
	}

	return formatMembers(people), nil
}

// formatMembers is split out so the cap and the wording can be tested without
// a conversation, a database or a Mongo.
func formatMembers(people []entityName) string {
	shown := people
	extra := 0
	if len(shown) > memberLimit {
		extra = len(shown) - memberLimit
		shown = shown[:memberLimit]
	}

	var out strings.Builder
	fmt.Fprintf(&out, "%d in this conversation:\n", len(people))
	for _, person := range shown {
		if person.Handle == "" {
			fmt.Fprintf(&out, "• %s\n", person.Name)
			continue
		}
		fmt.Fprintf(&out, "• %s (%s)\n", person.Name, person.Handle)
	}
	if extra > 0 {
		fmt.Fprintf(&out, "…and %d more", extra)
	}
	return strings.TrimRight(out.String(), "\n")
}

// createdCommand answers with when this conversation started.
//
// createdAt is mongoose's, stamped when the conversation document was first
// written. That is the first message rather than the moment somebody opened
// the thread - and it is the only date either service actually has, so saying
// it plainly beats inventing a more precise-sounding one.
func createdCommand(ctx context.Context, envelope json.RawMessage) (string, error) {
	var parsed commandEnvelope
	if err := json.Unmarshal(envelope, &parsed); err != nil {
		return "", fmt.Errorf("bad envelope: %w", err)
	}

	conversations := connections.Collection("conversations")
	if conversations == nil {
		return "", fmt.Errorf("mongo is not connected")
	}

	var found struct {
		CreatedAt time.Time `bson:"createdAt"`
	}
	err := conversations.FindOne(ctx,
		bson.M{"conversationID": parsed.Conversation.ID}).Decode(&found)
	if err != nil {
		return "I could not find when this conversation started.", nil
	}
	if found.CreatedAt.IsZero() {
		// Written before timestamps were on the schema. Honest is better than
		// a date of 1 January year one.
		return "This conversation started before I began keeping track.", nil
	}

	return fmt.Sprintf("This conversation started on %s.",
		found.CreatedAt.UTC().Format("2 January 2006")), nil
}

// helpCommand lists the commands that work HERE.
//
// Scoped to this conversation on purpose: a command belongs to a bot, and one
// whose bot is not in the room cannot be run from it. Listing those too would
// be a menu of things that silently do nothing.
func helpCommand(ctx context.Context, envelope json.RawMessage) (string, error) {
	var parsed commandEnvelope
	if err := json.Unmarshal(envelope, &parsed); err != nil {
		return "", fmt.Errorf("bad envelope: %w", err)
	}

	participants, _ := conversationFacts(ctx, parsed.Conversation.ID)

	// The same reach rule commandResolver.js applies when deciding what a typed
	// command can resolve to: a system bot everywhere, any other bot only where
	// it is a participant. The two must agree, or /help lists a command that
	// then refuses to run.
	const query = `
		SELECT c.name, c.description, b.handle, b.is_system
		  FROM bot_commands c
		  JOIN bot_bot b ON b.id = c.bot_id
		 WHERE c.is_active AND b.is_active
		   AND (b.is_system OR b.entity_id = ANY($1::text[]))
		 ORDER BY b.is_system DESC, c.name ASC`

	rows, err := connections.Pool().Query(ctx, query, participants)
	if err != nil {
		return "", fmt.Errorf("could not list commands: %w", err)
	}
	defer rows.Close()

	var lines []string
	for rows.Next() {
		var name, description, handle string
		var isSystem bool
		if err := rows.Scan(&name, &description, &handle, &isSystem); err != nil {
			return "", err
		}

		// A bot's command is written with its target. Two bots can each own a
		// "summarize", and /summarize alone would be ambiguous between them -
		// so the listing shows the form that is never ambiguous.
		label := "/" + name
		if !isSystem && handle != "" {
			label += ":" + handle
		}
		if description != "" {
			label += " — " + description
		}
		lines = append(lines, "• "+label)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}

	if len(lines) == 0 {
		return "No commands are available in this conversation.", nil
	}
	return "Commands you can use here:\n" + strings.Join(lines, "\n"), nil
}

// entityName is one participant, resolved for display.
type entityName struct {
	Name   string
	Handle string
}

// resolveEntities turns entity ids into names across all three namespaces.
//
// THREE, not two. The notification queries in handlers.go join user_account
// and community_realm only, which is why a bot shows up in them as a raw UUID.
// A conversation can hold all three kinds, and a member list that renders one
// of them as a UUID is a member list with a bug in it.
//
// Every column is coalesced: community_realm.slug is nullable and populated on
// most rows but not all, and one NULL would otherwise fail the whole scan and
// take the entire answer with it.
//
// NOTHING IS FILTERED OUT HERE - not inactive accounts, not unverified ones,
// not system bots. That is the opposite of the entity SEARCH query, and
// deliberately so: search decides who may be FOUND, this decides who is IN THE
// ROOM. Somebody who is a participant is a participant, and a member list that
// omits one is simply wrong.
func resolveEntities(ctx context.Context, entityIDs []string) ([]entityName, error) {
	const query = `
		SELECT name, handle FROM (
			SELECT btrim(coalesce(first_name,'') || ' ' || coalesce(last_name,'')) AS name,
			       coalesce(username,'') AS handle
			  FROM user_account WHERE entity_id = ANY($1::text[])
			UNION ALL
			SELECT coalesce(name,''), coalesce(slug,'')
			  FROM community_realm WHERE entity_id = ANY($1::text[])
			UNION ALL
			SELECT coalesce(name,''), coalesce(handle,'')
			  FROM bot_bot WHERE entity_id = ANY($1::text[])
		) AS members
		ORDER BY lower(coalesce(nullif(name,''), handle)), handle`

	rows, err := connections.Pool().Query(ctx, query, entityIDs)
	if err != nil {
		return nil, fmt.Errorf("could not resolve members: %w", err)
	}
	defer rows.Close()

	var people []entityName
	for rows.Next() {
		var person entityName
		if err := rows.Scan(&person.Name, &person.Handle); err != nil {
			return nil, err
		}
		if person.Name == "" && person.Handle == "" {
			continue
		}
		if person.Name == "" {
			person.Name = person.Handle
		}
		people = append(people, person)
	}
	return people, rows.Err()
}
