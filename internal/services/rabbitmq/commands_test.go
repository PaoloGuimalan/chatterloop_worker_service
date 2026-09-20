package rabbitmq

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
)

// The pure half of running a command: how a stored definition becomes an
// outbound request. Everything either side of it is a database read or a
// network call; this is the part that can be wrong quietly.

func envelopeFixture() json.RawMessage {
	return json.RawMessage(`{
		"command":{"name":"summarize","args":"the last hour","target":null},
		"invoker":{"entity_id":"entity-paulo","handle":"paologuimalan"},
		"conversation":{"id":"conv-1","type":"group"},
		"message":{"id":"msg-1","replying_to":null}
	}`)
}

func rowFixture() commandRow {
	return commandRow{
		Name:       "summarize",
		Category:   "webhook",
		WebhookURL: "https://example.test/{team}/hook",
		WebhookRequest: map[string]map[string]any{
			"params":  {"team": "ops"},
			"query":   {"mode": "brief"},
			"headers": {"Authorization": "Bearer secret"},
			"payload": {"source": "chat"},
		},
	}
}

func TestWebhookRequestCarriesEveryPart(t *testing.T) {
	request, err := buildWebhookRequest(rowFixture(), envelopeFixture())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := request.URL.String(); got != "https://example.test/ops/hook?mode=brief" {
		t.Fatalf("unexpected url: %s", got)
	}
	if got := request.Header.Get("Authorization"); got != "Bearer secret" {
		t.Fatalf("header missing: %q", got)
	}
	if got := request.Header.Get("Content-Type"); got != "application/json" {
		t.Fatalf("content type: %q", got)
	}

	body := decodeBody(t, request)
	if body["source"] != "chat" {
		t.Fatalf("payload missing: %v", body["source"])
	}
	invoker, _ := body["invoker"].(map[string]any)
	if invoker["entity_id"] != "entity-paulo" {
		t.Fatalf("envelope missing: %v", body["invoker"])
	}
}

// A definition able to overwrite `invoker` would be a way to make a request
// claim it came from somebody else - which is exactly what deriving the
// envelope server-side prevents.
func TestThePayloadCannotOverwriteTheEnvelope(t *testing.T) {
	row := rowFixture()
	row.WebhookRequest["payload"] = map[string]any{
		"invoker": map[string]any{"entity_id": "somebody-else"},
	}

	request, err := buildWebhookRequest(row, envelopeFixture())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	invoker, _ := decodeBody(t, request)["invoker"].(map[string]any)
	if invoker["entity_id"] != "entity-paulo" {
		t.Fatalf("the payload overwrote the envelope: %v", invoker)
	}
}

func TestAnExistingQueryStringIsKept(t *testing.T) {
	row := rowFixture()
	row.WebhookURL = "https://example.test/hook?fixed=1"
	row.WebhookRequest["params"] = nil

	request, err := buildWebhookRequest(row, envelopeFixture())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	query := request.URL.Query()
	if query.Get("fixed") != "1" || query.Get("mode") != "brief" {
		t.Fatalf("unexpected query: %s", request.URL.RawQuery)
	}
}

func TestABareDefinitionStillBuilds(t *testing.T) {
	row := commandRow{
		Name:           "ping",
		WebhookURL:     "https://example.test/hook",
		WebhookRequest: map[string]map[string]any{},
	}

	request, err := buildWebhookRequest(row, envelopeFixture())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if request.URL.String() != "https://example.test/hook" {
		t.Fatalf("unexpected url: %s", request.URL)
	}
}

// --- placeholders ----------------------------------------------------------

func TestApplyParamsFillsFromTheDefinition(t *testing.T) {
	if got := applyParams("https://x.test/{team}/hook", map[string]any{"team": "ops"}); got != "https://x.test/ops/hook" {
		t.Fatalf("unexpected: %s", got)
	}
}

// A value must not be able to climb out of its own path segment.
func TestApplyParamsEscapesTheValue(t *testing.T) {
	got := applyParams("https://x.test/{team}", map[string]any{"team": "../admin"})
	if got != "https://x.test/..%2Fadmin" {
		t.Fatalf("unescaped: %s", got)
	}
}

// Blanking an unknown placeholder yields a URL that silently points elsewhere.
// Leaving it visible is recognisably mis-wired.
func TestApplyParamsLeavesUnknownPlaceholders(t *testing.T) {
	if got := applyParams("https://x.test/{nope}", map[string]any{"team": "ops"}); got != "https://x.test/{nope}" {
		t.Fatalf("unexpected: %s", got)
	}
}

func TestApplyParamsWithNoParams(t *testing.T) {
	if got := applyParams("https://x.test/hook", nil); got != "https://x.test/hook" {
		t.Fatalf("unexpected: %s", got)
	}
}

// --- the built-in registry -------------------------------------------------

func TestRegisterSystemCommandIsCaseInsensitive(t *testing.T) {
	RegisterSystemCommand("Members", func(ctx context.Context, e json.RawMessage) (string, error) { return "", nil })
	if _, ok := SystemCommands["members"]; !ok {
		t.Fatal("a built-in must be findable by its lowercase name")
	}
	delete(SystemCommands, "members")
}

func decodeBody(t *testing.T, request *http.Request) map[string]any {
	t.Helper()
	body, err := request.GetBody()
	if err != nil {
		t.Fatalf("no body: %v", err)
	}
	defer body.Close()

	raw, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("unreadable body: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("body is not json: %v", err)
	}
	return decoded
}
