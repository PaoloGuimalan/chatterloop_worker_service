package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
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

// --- the verb --------------------------------------------------------------

// A row written before the column existed, or one carrying a verb this build
// does not know, behaves like the old code rather than failing.
func TestTheDefaultVerbIsPost(t *testing.T) {
	for _, stored := range []string{"", "   ", "TRACE", "nonsense"} {
		row := rowFixture()
		row.WebhookMethod = stored

		request, err := buildWebhookRequest(row, envelopeFixture())
		if err != nil {
			t.Fatalf("%q: unexpected error: %v", stored, err)
		}
		if request.Method != http.MethodPost {
			t.Fatalf("%q should fall back to POST, got %s", stored, request.Method)
		}
	}
}

func TestTheStoredVerbIsUsed(t *testing.T) {
	for stored, want := range map[string]string{
		"get":    http.MethodGet,
		"PUT":    http.MethodPut,
		"patch":  http.MethodPatch,
		"DELETE": http.MethodDelete,
	} {
		row := rowFixture()
		row.WebhookMethod = stored

		request, err := buildWebhookRequest(row, envelopeFixture())
		if err != nil {
			t.Fatalf("%q: unexpected error: %v", stored, err)
		}
		if request.Method != want {
			t.Fatalf("%q should be %s, got %s", stored, want, request.Method)
		}
	}
}

// PUT and PATCH carry a body exactly the way POST always did.
func TestAWritingVerbStillCarriesTheBody(t *testing.T) {
	row := rowFixture()
	row.WebhookMethod = "PATCH"

	request, err := buildWebhookRequest(row, envelopeFixture())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
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

// Go will attach a body to a GET and a fair number of servers and proxies will
// drop it - so the envelope travels in a header, and the receiver still learns
// who typed the command and where.
func TestABodylessVerbPutsTheEnvelopeInAHeader(t *testing.T) {
	for _, method := range []string{"GET", "DELETE"} {
		row := rowFixture()
		row.WebhookMethod = method

		request, err := buildWebhookRequest(row, envelopeFixture())
		if err != nil {
			t.Fatalf("%s: unexpected error: %v", method, err)
		}

		if request.Body != nil {
			t.Fatalf("%s should carry no body", method)
		}
		if got := request.Header.Get("Content-Type"); got != "" {
			t.Fatalf("%s should set no content type, got %q", method, got)
		}

		var carried map[string]any
		header := request.Header.Get(envelopeHeader)
		if err := json.Unmarshal([]byte(header), &carried); err != nil {
			t.Fatalf("%s: the header is not the envelope: %v", method, err)
		}
		conversation, _ := carried["conversation"].(map[string]any)
		if conversation["id"] != "conv-1" {
			t.Fatalf("%s: the envelope lost the conversation: %v", method, carried)
		}
		invoker, _ := carried["invoker"].(map[string]any)
		if invoker["handle"] != "paologuimalan" {
			t.Fatalf("%s: the envelope lost the invoker: %v", method, carried)
		}
	}
}

// THE REGRESSION THIS EXISTS FOR.
//
// The envelope used to go into the query string, and newsdata.io answers 422
// "You can't use the conversation.id parameter" to a single added key - so a
// /top-news that worked when pasted into a browser failed as a command. The
// query string is the definition's alone.
func TestABodylessVerbSendsOnlyTheStoredQuery(t *testing.T) {
	row := rowFixture()
	row.WebhookMethod = "GET"
	row.WebhookURL = "https://news.test/latest?apikey=secret"
	row.WebhookRequest["params"] = nil
	row.WebhookRequest["query"] = map[string]any{"country": "ph"}

	request, err := buildWebhookRequest(row, envelopeFixture())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	query := request.URL.Query()
	if len(query) != 2 || query.Get("apikey") != "secret" || query.Get("country") != "ph" {
		t.Fatalf("the query string must carry only what was configured: %v", query)
	}
}

// `payload` is a body definition. Sending it anywhere else would mean one row
// means two different things depending on its verb.
func TestABodylessVerbDoesNotSendThePayload(t *testing.T) {
	row := rowFixture()
	row.WebhookMethod = "GET"

	request, err := buildWebhookRequest(row, envelopeFixture())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, present := request.URL.Query()["source"]; present {
		t.Fatal("the payload must not leak into the query string")
	}
}

// A stored header may override the Content-Type - some endpoints want a
// vendor type - but must not be able to say who sent the command.
func TestAStoredHeaderCannotForgeTheEnvelope(t *testing.T) {
	row := rowFixture()
	row.WebhookMethod = "GET"
	row.WebhookRequest["headers"] = map[string]any{
		envelopeHeader: `{"invoker":{"handle":"somebody-else"}}`,
	}

	request, err := buildWebhookRequest(row, envelopeFixture())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var carried map[string]any
	if err := json.Unmarshal([]byte(request.Header.Get(envelopeHeader)), &carried); err != nil {
		t.Fatalf("the header is not the envelope: %v", err)
	}
	invoker, _ := carried["invoker"].(map[string]any)
	if invoker["handle"] != "paologuimalan" {
		t.Fatalf("a stored header overwrote the envelope: %v", carried)
	}
}

// A newline typed after the command would otherwise be a second header. JSON
// escapes every control character, so the value is safe by construction - and
// this proves the transport agrees.
func TestTheEnvelopeHeaderSurvivesAwkwardText(t *testing.T) {
	row := rowFixture()
	row.WebhookMethod = "GET"
	envelope := json.RawMessage(
		`{"command":{"name":"summarize","args":"line one\nline two\r\ntabs\there"},` +
			`"conversation":{"id":"conv-1"},"invoker":{"handle":"ana"}}`)

	request, err := buildWebhookRequest(row, envelope)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	value := request.Header.Get(envelopeHeader)
	if strings.ContainsAny(value, "\n\r") {
		t.Fatalf("a raw newline in a header value is a second header: %q", value)
	}

	var carried map[string]any
	if err := json.Unmarshal([]byte(value), &carried); err != nil {
		t.Fatalf("the header is not the envelope: %v", err)
	}
	command, _ := carried["command"].(map[string]any)
	if command["args"] != "line one\nline two\r\ntabs\there" {
		t.Fatalf("the text did not survive: %q", command["args"])
	}

	// And the transport accepts it, which is the assertion that actually
	// matters - Go rejects an invalid header value at write time.
	if err := request.Header.Write(io.Discard); err != nil {
		t.Fatalf("the transport refused the header: %v", err)
	}
}

// A webhook that fires without context beats one that does not fire because
// somebody typed an essay.
func TestAnOversizeEnvelopeIsDroppedNotFatal(t *testing.T) {
	row := rowFixture()
	row.WebhookMethod = "GET"
	envelope := json.RawMessage(
		`{"command":{"args":"` + strings.Repeat("x", envelopeHeaderLimit+100) + `"}}`)

	request, err := buildWebhookRequest(row, envelope)
	if err != nil {
		t.Fatalf("an oversize envelope must not fail the request: %v", err)
	}
	if got := request.Header.Get(envelopeHeader); got != "" {
		t.Fatalf("it should have been dropped, got %d bytes", len(got))
	}
	if request.URL.Host != "example.test" {
		t.Fatalf("the request should still be built: %s", request.URL)
	}
}

// A body verb already carries the envelope in the body; sending it twice
// would be two sources of truth a receiver has to choose between.
func TestABodyVerbSendsNoEnvelopeHeader(t *testing.T) {
	for _, method := range []string{"POST", "PUT", "PATCH"} {
		row := rowFixture()
		row.WebhookMethod = method

		request, err := buildWebhookRequest(row, envelopeFixture())
		if err != nil {
			t.Fatalf("%s: unexpected error: %v", method, err)
		}
		if got := request.Header.Get(envelopeHeader); got != "" {
			t.Fatalf("%s should not duplicate the envelope in a header: %q", method, got)
		}
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

// --- what a webhook answered -----------------------------------------------
//
// Only reached by a `responds=system` command, where the endpoint's answer is
// posted into the conversation as System. Everything here is about what a
// person ends up reading, so the expectations are written as the Markdown
// both clients render rather than as intermediate structures.

func TestWebhookAnswerPrefersTheHumanField(t *testing.T) {
	// The shape Neon's own control endpoint returns, and the reason `message`
	// is in the list at all.
	body := []byte(`{"status":true,"data":{"is_online":true},"message":"@neon is now awake."}`)

	if answer := webhookAnswer(body); answer != "@neon is now awake." {
		t.Fatalf("wanted the message, got %q", answer)
	}
}

func TestWebhookAnswerTriesTheKeysInOrder(t *testing.T) {
	body := []byte(`{"message":"second","text":"first","content":"third"}`)

	if answer := webhookAnswer(body); answer != "first" {
		t.Fatalf("text should win over message, got %q", answer)
	}
}

// An endpoint that answers in plain text is answering perfectly well.
func TestWebhookAnswerPostsPlainTextAsWritten(t *testing.T) {
	if answer := webhookAnswer([]byte("  Deployed to staging.\n")); answer != "Deployed to staging." {
		t.Fatalf("wanted the trimmed text, got %q", answer)
	}
}

// No known key means no guess about WHICH field was the answer: every field is
// listed, in the order the endpoint wrote them.
func TestWebhookAnswerListsAFlatObject(t *testing.T) {
	body := []byte(`{"handle":"neon","is_online":true,"answered":1200}`)

	want := "- **handle**: neon\n- **is_online**: true\n- **answered**: 1200"
	if answer := webhookAnswer(body); answer != want {
		t.Fatalf("wanted\n%s\ngot\n%s", want, answer)
	}
}

// Go randomises map iteration, so a decoded object would list its fields in a
// different order on every call - the answer appearing to change when nothing
// about it has.
func TestWebhookAnswerKeepsTheResponseOrder(t *testing.T) {
	body := []byte(`{"zebra":1,"apple":2,"mango":3}`)

	want := "- **zebra**: 1\n- **apple**: 2\n- **mango**: 3"
	for range 20 {
		if answer := webhookAnswer(body); answer != want {
			t.Fatalf("field order is not stable: got\n%s", answer)
		}
	}
}

// A number keeps the spelling the endpoint used. Decoding into `any` makes it
// a float64, and one that size then prints as 1.23456789012e+11.
func TestWebhookAnswerDoesNotRewriteNumbers(t *testing.T) {
	body := []byte(`{"id":123456789012,"price":10.50,"ratio":1e3}`)

	want := "- **id**: 123456789012\n- **price**: 10.50\n- **ratio**: 1e3"
	if answer := webhookAnswer(body); answer != want {
		t.Fatalf("wanted\n%s\ngot\n%s", want, answer)
	}
}

// Anything with structure in it goes in a fence: exact, monospace, and
// impossible to mangle. Guessing how deep to render somebody's payload would
// mean sometimes dropping a field, and a flattened `data.uptime.days` reads as
// a field name that does not exist.
func TestWebhookAnswerFencesANestedObject(t *testing.T) {
	body := []byte(`{"status":true,"data":{"is_online":true}}`)

	want := "```json\n{\n  \"status\": true,\n  \"data\": {\n    \"is_online\": true\n  }\n}\n```"
	if answer := webhookAnswer(body); answer != want {
		t.Fatalf("wanted\n%s\ngot\n%s", want, answer)
	}
}

// One nested field is enough. A list that is bullets for some fields and raw
// JSON for others is harder to read than either.
func TestWebhookAnswerFencesAMostlyFlatObject(t *testing.T) {
	body := []byte(`{"env":"prod","ok":true,"reviewers":["alice","bob"]}`)

	if answer := webhookAnswer(body); !strings.HasPrefix(answer, "```json") {
		t.Fatalf("wanted a fence, got\n%s", answer)
	}
}

// Past a dozen fields the block is easier to scan than the list.
func TestWebhookAnswerFencesAWideObject(t *testing.T) {
	fields := make([]string, 0, maxListedFields+5)
	for index := range maxListedFields + 5 {
		fields = append(fields, fmt.Sprintf(`"k%d":%d`, index, index))
	}
	body := []byte(`{` + strings.Join(fields, ",") + `}`)

	if answer := webhookAnswer(body); !strings.HasPrefix(answer, "```json") {
		t.Fatalf("wanted a fence, got\n%s", answer)
	}
}

// A value carrying emphasis punctuation would render as italics. It goes in a
// code span instead - a value is data, and data has to survive being shown.
func TestWebhookAnswerProtectsMarkupInAValue(t *testing.T) {
	body := []byte(`{"formula":"2*3*4","note":"plain words"}`)

	want := "- **formula**: `2*3*4`\n- **note**: plain words"
	if answer := webhookAnswer(body); answer != want {
		t.Fatalf("wanted\n%s\ngot\n%s", want, answer)
	}
}

// A backtick closes any code span, so there is nowhere safe to put it - the
// whole object falls back to the fence.
func TestWebhookAnswerFencesAValueWithABacktick(t *testing.T) {
	answer := webhookAnswer([]byte("{\"cmd\":\"run `ls`\"}"))

	if !strings.HasPrefix(answer, "```json") {
		t.Fatalf("wanted a fence, got %q", answer)
	}
}

// A key that would be read as markup is refused for the same reason.
func TestWebhookAnswerFencesAnUnprintableKey(t *testing.T) {
	answer := webhookAnswer([]byte(`{"we*rd":"value"}`))

	if !strings.HasPrefix(answer, "```json") {
		t.Fatalf("wanted a fence, got %q", answer)
	}
}

func TestWebhookAnswerListsAnArrayOfScalars(t *testing.T) {
	body := []byte(`["alice","bob","carol"]`)

	want := "- alice\n- bob\n- carol"
	if answer := webhookAnswer(body); answer != want {
		t.Fatalf("wanted\n%s\ngot\n%s", want, answer)
	}
}

// A 204, or an endpoint that succeeded and had nothing to say. The caller
// posts nothing rather than "(no response)".
func TestWebhookAnswerIsEmptyForAnEmptyBody(t *testing.T) {
	for _, body := range []string{"", "   \n"} {
		if answer := webhookAnswer([]byte(body)); answer != "" {
			t.Fatalf("%q should not produce an answer, got %q", body, answer)
		}
	}
}

// A key that is present but says nothing is not the answer. `{"message": 42}`
// is a field that happens to share a name with a sentence, and taking it as
// the whole answer would drop every other field to say "42".
func TestWebhookAnswerIgnoresABlankOrNonStringKey(t *testing.T) {
	cases := map[string]string{
		`{"message":"   ","id":7}`: "- **message**: `\"   \"`\n- **id**: 7",
		`{"message":null,"id":7}`:  "- **message**: null\n- **id**: 7",
		`{"message":42,"id":7}`:    "- **message**: 42\n- **id**: 7",
	}
	for body, want := range cases {
		if answer := webhookAnswer([]byte(body)); answer != want {
			t.Fatalf("%s\nwanted\n%s\ngot\n%s", body, want, answer)
		}
	}
}

// However big the answer is, its fences have to balance - an unterminated one
// reads as an open code block for the rest of the message.
func TestWebhookAnswerBalancesItsFences(t *testing.T) {
	items := make([]string, 0, 400)
	for index := range 400 {
		items = append(items, fmt.Sprintf(`{"n":%d}`, index))
	}
	body := []byte("[" + strings.Join(items, ",") + "]")

	answer := webhookAnswer(body)

	if !strings.HasSuffix(answer, "\n```") {
		t.Fatalf("the block must end closed, the tail was %q", answer[len(answer)-40:])
	}
	if strings.Count(answer, "```")%2 != 0 {
		t.Fatal("the fences must be balanced")
	}
}

// --- the envelope ----------------------------------------------------------

func TestConversationOfReadsTheID(t *testing.T) {
	id, err := conversationOf(envelopeFixture())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id != "conv-1" {
		t.Fatalf("wanted conv-1, got %q", id)
	}
}

// An answer with nowhere to go is a failure worth logging, not an empty
// conversation id handed to the send path.
func TestConversationOfRefusesAnEnvelopeWithoutOne(t *testing.T) {
	for _, envelope := range []string{`{}`, `{"conversation":{}}`, `{"conversation":{"id":""}}`, `not json`} {
		if _, err := conversationOf(json.RawMessage(envelope)); err == nil {
			t.Fatalf("%s should not resolve to a conversation", envelope)
		}
	}
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
