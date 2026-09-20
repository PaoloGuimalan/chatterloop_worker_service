package rabbitmq

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// Every built-in the migration creates must actually be implemented in this
// build, or a command resolves, runs and answers nothing.
func TestEveryMigratedBuiltInIsRegistered(t *testing.T) {
	// The names in bot/migrations/0005_system_commands.py.
	for _, name := range []string{"members", "created", "help"} {
		if _, ok := SystemCommands[name]; !ok {
			t.Fatalf("no built-in registered for %q", name)
		}
	}
}

func TestMembersIsCappedAndCounted(t *testing.T) {
	var people []entityName
	for i := 0; i < memberLimit+7; i++ {
		people = append(people, entityName{
			Name:   fmt.Sprintf("Person %d", i),
			Handle: fmt.Sprintf("person%d", i),
		})
	}

	out := formatMembers(people)

	// The COUNT is the real total, not the number shown. Somebody reading
	// "50 in this conversation" in a room of 57 has been told something false.
	if !strings.HasPrefix(out, "57 in this conversation:") {
		t.Fatalf("the total must be the real one, got: %q", firstLine(out))
	}
	if !strings.HasSuffix(out, "…and 7 more") {
		t.Fatalf("the remainder must be reported, got: %q", out[len(out)-20:])
	}
	if got := strings.Count(out, "•"); got != memberLimit {
		t.Fatalf("expected %d listed, got %d", memberLimit, got)
	}
}

// A handle written bare, never as @handle: the send path deliberately does not
// resolve mentions for a system reply, so an @ here would look like a mention
// and be the one thing in the message that does not behave like one.
func TestMembersNeverWritesAMention(t *testing.T) {
	out := formatMembers([]entityName{{Name: "Juan Dela Cruz", Handle: "juan"}})

	if strings.Contains(out, "@") {
		t.Fatalf("a member list must not contain an @mention: %q", out)
	}
	if !strings.Contains(out, "Juan Dela Cruz (juan)") {
		t.Fatalf("unexpected rendering: %q", out)
	}
}

// A realm has a name and no person behind it; a row with no handle at all is
// possible too. Neither should render an empty pair of brackets.
func TestMembersWithoutAHandle(t *testing.T) {
	out := formatMembers([]entityName{{Name: "Neon Support"}})

	if strings.Contains(out, "(") {
		t.Fatalf("no handle means no brackets: %q", out)
	}
	if !strings.Contains(out, "• Neon Support") {
		t.Fatalf("unexpected rendering: %q", out)
	}
}

// A built-in is a pure function of the envelope, so a malformed one is its
// error rather than a panic inside a consumer.
func TestBuiltInsRejectABadEnvelope(t *testing.T) {
	for name, fn := range map[string]SystemCommand{
		"members": membersCommand,
		"created": createdCommand,
		"help":    helpCommand,
	} {
		if _, err := fn(t.Context(), json.RawMessage(`{"conversation":`)); err == nil {
			t.Fatalf("%s accepted a truncated envelope", name)
		}
	}
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}
