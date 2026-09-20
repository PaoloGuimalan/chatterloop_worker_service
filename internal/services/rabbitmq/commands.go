package rabbitmq

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"
	"worker_service/internal/connections"

	"github.com/jackc/pgx/v5"
)

// Running a /command somebody typed in a conversation.
//
// # WHAT ARRIVES, AND WHAT DOES NOT
//
// The job carries a command ID and an envelope - never the command's
// definition. `webhook_request` holds credentials in plain text, so putting
// the row on the queue would put them in the broker, its logs and any
// dead-letter queue they land in. Loading the row here also means the CURRENT
// definition runs rather than a snapshot taken before somebody edited it.
//
// # THE SENDER ALREADY DECIDED REACH
//
// Node resolved which bots this command belongs to and that they are reachable
// in that conversation. One job was published per bot, so this handler runs
// exactly one command for exactly one bot and has no fan-out to do.

// commandTimeout bounds an outbound webhook. Deliberately shorter than the
// consumer's own timeout so a slow endpoint fails as a webhook failure with a
// log line, rather than as a message that silently exceeded its deadline.
const commandTimeout = 20 * time.Second

// Who answers, from bot_commands.responds. Defined as CommandResponder in
// user_service/bot/models.py.
//
// worker_service runs the `system` and `webhook` categories, and running them
// IS the platform running them - it holds no bot's token and cannot speak as
// one. So of the three values, the two that mean something here are `none`
// (say nothing) and `system` (say it as the System bot). A `bot` command is
// never queued at all, which is why its responder is the bot's own business.
const (
	respondsNone   = "none"
	respondsSystem = "system"
)

// webhookBodyLimit bounds what is read back from a webhook.
//
// The body is read even when nothing will be done with it, because it has to
// be drained for the connection to go back to the pool - so the limit is what
// stops an endpoint answering with a gigabyte from being read into memory by
// a command anybody in a conversation can type.
const webhookBodyLimit = 64 * 1024

// webhookAnswerLimit bounds what is POSTED, in runes.
//
// A message is read by people rather than parsed by anything, and an endpoint
// that answers with a page of JSON should not be able to put a page of JSON
// into somebody's conversation. Deliberately well under the body limit: the
// difference is room for an answer to be extracted out of a larger response.
const webhookAnswerLimit = 1500

// RunCommandPayload is what Node publishes to `run_command`.
type RunCommandPayload struct {
	CommandID string          `json:"command_id"`
	Envelope  json.RawMessage `json:"envelope"`
}

// commandRow is bot_commands joined to the bot that owns it.
type commandRow struct {
	ID             string
	Name           string
	Category       string
	Responds       string
	WebhookURL     string
	WebhookMethod  string
	WebhookRequest map[string]map[string]any
	BotID          string
	BotEntityID    string
	BotHandle      string
	BotIsSystem    bool
}

// SystemCommands is the map a system command is dispatched through, keyed on
// the command NAME.
//
// There is no codename column: /members looks up "members", and a second
// identifier would only be a way for the two to disagree.
//
// A name this map has no entry for is possible and not preventable - a
// rollback, or a row created before the deploy that implements it. That case
// is REPORTED rather than ignored, because a command that quietly stopped
// working is indistinguishable from a typo.
var SystemCommands = map[string]SystemCommand{}

// SystemCommand is one built-in.
//
// It RETURNS its answer rather than posting it. Posting is identical for every
// built-in - same sender, same collection, same frame - so keeping it in one
// place means each one is a pure function of the envelope, testable without a
// database, and none of them can get the send path subtly wrong on its own.
//
// An empty string is a legitimate answer: a built-in that changes state and
// has nothing to report says nothing.
type SystemCommand func(ctx context.Context, envelope json.RawMessage) (string, error)

// RegisterSystemCommand adds a built-in. Called from an init() in whichever
// file implements it, so adding one is a file rather than an edit to a switch
// that everybody has to touch.
func RegisterSystemCommand(name string, fn SystemCommand) {
	SystemCommands[strings.ToLower(name)] = fn
}

// commandEnvelope is the part of the envelope this file reads.
//
// Deliberately partial: the envelope is a contract with webhooks and with
// whatever is written next, so decoding only what is used here means a field
// added on the Node side needs no change on this one.
type commandEnvelope struct {
	Conversation struct {
		ID string `json:"id"`
	} `json:"conversation"`
}

// RunCommand loads the command and dispatches it by category.
func RunCommand(ctx context.Context, payload RunCommandPayload) {
	if payload.CommandID == "" {
		slog.Warn("run_command with no command id")
		return
	}

	row, err := loadCommand(ctx, payload.CommandID)
	if err != nil {
		if err == pgx.ErrNoRows {
			// Deleted between being queued and being run. Ordinary, not an
			// error worth alarming about.
			slog.Info("command no longer exists", "command_id", payload.CommandID)
			return
		}
		slog.Error("could not load command", "command_id", payload.CommandID, "error", err)
		return
	}

	switch row.Category {
	case "system":
		runSystemCommand(ctx, row, payload.Envelope)
	case "webhook":
		runWebhookCommand(ctx, row, payload.Envelope)
	case "bot":
		// Not ours to run, and not queued either - Node filters these out
		// before publishing. A bot already receives a frame for every message
		// in its conversations, and that frame carries the command that was
		// typed, so a bot learns about one exactly the way it learns about a
		// mention. Whether it HAS the command, and what it does about it, is
		// the bot's own business.
		//
		// Reaching here means something published a job Node would not have,
		// which is worth saying rather than ignoring.
		slog.Warn("bot commands are the bot's to run, not worker_service's",
			"command", row.Name, "bot", row.BotHandle)
	default:
		slog.Warn("unknown command category", "category", row.Category, "command", row.Name)
	}
}

func runSystemCommand(ctx context.Context, row commandRow, envelope json.RawMessage) {
	fn, ok := SystemCommands[strings.ToLower(row.Name)]
	if !ok {
		slog.Warn("no built-in for this command name",
			"command", row.Name,
			"hint", "a row exists for it but this build has no function - a rollback, or a deploy still to come")
		return
	}

	answer, err := fn(ctx, envelope)
	if err != nil {
		// Not posted into the conversation. An internal failure is for the logs;
		// telling a room that a database call failed helps nobody in it, and a
		// command whose answer is sometimes an error message is one nothing can
		// rely on.
		slog.Error("built-in command failed", "command", row.Name, "error", err)
		return
	}

	// `none` means the command was not meant to say anything - /stop changes
	// state, and posting "stopped" into the thread somebody just asked to
	// quieten is exactly wrong. A built-in that returns text anyway is a
	// mis-configured row rather than a reason to override the setting.
	//
	// An empty answer is the same outcome by a different route: a built-in
	// that had nothing to report said so by returning nothing.
	if row.Responds == respondsNone || answer == "" {
		return
	}

	// Anything else is posted as System, whichever bot owns the row and
	// whichever of the two responders the row names. worker_service holds no
	// bot's token and cannot speak as one; what it can do is speak as the
	// platform, which is what a built-in answer is.
	conversationID, err := conversationOf(envelope)
	if err != nil {
		slog.Error("could not read the envelope", "command", row.Name, "error", err)
		return
	}

	if err := PostSystemReply(ctx, conversationID, answer); err != nil {
		slog.Error("could not post the reply",
			"command", row.Name, "conversation_id", conversationID, "error", err)
	}
}

// conversationOf pulls the conversation id out of the envelope.
//
// Both runners need it and neither can do anything useful without it, so the
// decode lives in one place rather than being repeated with two slightly
// different error messages.
func conversationOf(envelope json.RawMessage) (string, error) {
	var parsed commandEnvelope
	if err := json.Unmarshal(envelope, &parsed); err != nil {
		return "", err
	}
	if parsed.Conversation.ID == "" {
		return "", fmt.Errorf("the envelope names no conversation")
	}
	return parsed.Conversation.ID, nil
}

// runWebhookCommand calls the endpoint, and - if the row asks for it - puts
// what came back into the conversation.
//
// THE TWO SHAPES OF WEBHOOK COMMAND
//
//	responds=system  a question. What the endpoint answered is posted as
//	                 System, because worker_service cannot speak as the bot
//	                 that owns the row - and the answer is not a bot's anyway,
//	                 it is somebody's API's.
//	anything else    a trigger. The call IS the command; deploy something,
//	                 flip a flag, page somebody. Nothing is posted, and the
//	                 response is drained and forgotten. This is what every
//	                 webhook command did before `system` was honoured here, so
//	                 an existing row keeps behaving exactly as it did.
//
// A receiver that would rather answer in its own name needs neither: it holds
// a token and can send a message like anything else, which is a thing it does
// on its own account rather than as this command answering.
func runWebhookCommand(ctx context.Context, row commandRow, envelope json.RawMessage) {
	// Read once, used by every branch below, so the failure paths and the
	// success path cannot drift about whether an answer was wanted.
	wantsAnswer := row.Responds == respondsSystem

	request, err := buildWebhookRequest(row, envelope)
	if err != nil {
		slog.Error("could not build webhook request", "command", row.Name, "error", err)
		reportWebhookFailure(ctx, row, envelope, wantsAnswer, 0)
		return
	}

	ctx, cancel := context.WithTimeout(ctx, commandTimeout)
	defer cancel()
	request = request.WithContext(ctx)

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		slog.Error("webhook command failed", "command", row.Name, "error", err)
		reportWebhookFailure(ctx, row, envelope, wantsAnswer, 0)
		return
	}
	defer response.Body.Close()

	// Read up to the limit whether or not it will be used, then drain the
	// rest: an unread body holds the connection out of the pool.
	body, readErr := io.ReadAll(io.LimitReader(response.Body, webhookBodyLimit))
	_, _ = io.Copy(io.Discard, response.Body)

	if response.StatusCode >= 400 {
		slog.Error("webhook command rejected",
			"command", row.Name, "status", response.StatusCode, "url", row.WebhookURL)
		reportWebhookFailure(ctx, row, envelope, wantsAnswer, response.StatusCode)
		return
	}
	slog.Info("webhook command delivered",
		"command", row.Name, "bot", row.BotHandle, "status", response.StatusCode)

	if !wantsAnswer {
		return
	}
	if readErr != nil {
		slog.Error("could not read the webhook's answer", "command", row.Name, "error", readErr)
		reportWebhookFailure(ctx, row, envelope, wantsAnswer, response.StatusCode)
		return
	}

	answer := webhookAnswer(body)
	if answer == "" {
		// A 204, or an endpoint that succeeded and had nothing to say. The
		// same rule a built-in follows: an empty answer is an answer, and
		// "(no response)" in a conversation is noise.
		slog.Info("the webhook answered with nothing to post", "command", row.Name)
		return
	}

	conversationID, err := conversationOf(envelope)
	if err != nil {
		slog.Error("could not read the envelope", "command", row.Name, "error", err)
		return
	}
	if err := PostSystemReply(ctx, conversationID, answer); err != nil {
		slog.Error("could not post the webhook's answer",
			"command", row.Name, "conversation_id", conversationID, "error", err)
	}
}

// reportWebhookFailure says, once and briefly, that a command promising an
// answer has none.
//
// ONLY FOR responds=system. A trigger that fails is a log line: nobody in the
// conversation was waiting to be told anything, and a room does not want to
// hear about somebody's 502. A command that promised an answer is different -
// silence there is indistinguishable from a typo, which is the same reason
// /help answers "not available here" rather than nothing.
//
// The status is named and the URL is not. Whoever wired the command knows
// which endpoint it is; everyone else in the conversation does not need to
// learn that it exists.
func reportWebhookFailure(ctx context.Context, row commandRow, envelope json.RawMessage, wantsAnswer bool, statusCode int) {
	if !wantsAnswer {
		return
	}

	conversationID, err := conversationOf(envelope)
	if err != nil {
		return
	}

	text := fmt.Sprintf("/%s could not be completed.", row.Name)
	if statusCode > 0 {
		text = fmt.Sprintf("/%s could not be completed (%d).", row.Name, statusCode)
	}

	// A context that may already be cancelled - the timeout above fires on
	// exactly the failure worth reporting - so the notice gets its own.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), commandTimeout)
	defer cancel()

	if err := PostSystemReply(ctx, conversationID, text); err != nil {
		slog.Error("could not post the webhook failure",
			"command", row.Name, "conversation_id", conversationID, "error", err)
	}
}

// webhookAnswer turns a response body into something worth reading.
//
// WHY THE FORMATTING IS DONE HERE
//
// A raw body is what a program sends a program. `{"handle":"neon",
// "is_online":true,"since":"2026-09-20T10:04:00Z"}` on one line in a
// conversation is technically the answer and practically unreadable - and the
// place to fix that is here, not in two clients. Both of them already render
// a message as Markdown, with the same grammar on both sides (fenced code,
// bullets, `**bold**`, tables), so this produces Markdown and neither client
// needs to know that webhooks exist.
//
// WHAT COMES OUT
//
//	a human field    posted as prose, because the endpoint already wrote a
//	                 sentence for a person
//	a flat object    a bullet per field, `**key**: value`
//	anything nested  pretty-printed inside a ```json fence: exact, monospace,
//	                 horizontally scrollable, and impossible to mangle
//
// The fence is the fallback ON PURPOSE. A nested object rendered as prose
// would mean guessing which of somebody's fields matter and how deep to go,
// and a guess that drops a field is worse than a block that shows all of them.
func webhookAnswer(body []byte) string {
	raw := strings.TrimSpace(string(body))
	if raw == "" {
		return ""
	}
	return clipAnswer(formatWebhookBody(raw))
}

// answerKeys are the fields tried, in order, when a JSON object is searched
// for a line meant for a person. The first non-empty string wins.
//
// `text` and `message` lead because they are what a response envelope calls
// its human-readable line - Neon's own bot control endpoint answers
// `{"status":true,"data":{...},"message":"@neon is now awake."}`, and that
// sentence is the whole answer.
//
// Top level only. A nested match would mean guessing which of several strings
// deep in somebody's payload was written for a reader.
var answerKeys = []string{"text", "message", "content", "answer"}

func formatWebhookBody(raw string) string {
	if fields, ok := topLevelFields(raw); ok {
		if line := humanLine(fields); line != "" {
			return line
		}
		if list, ok := fieldList(fields); ok {
			return list
		}
		return jsonFence(raw)
	}

	if items, ok := topLevelItems(raw); ok {
		if list, ok := itemList(items); ok {
			return list
		}
		return jsonFence(raw)
	}

	// A bare JSON scalar: `42`, `true`, or a quoted sentence. Unquoted, it is
	// already the answer.
	if text, ok := jsonScalar(raw); ok {
		return text
	}

	// Not JSON. Prose is posted as written - it is already what somebody meant
	// to say, and Markdown in it renders the way Markdown in any message does.
	//
	// A body opening with a tag is the exception: an endpoint answering with a
	// page rather than a result. Fenced, so it reads as raw output instead of
	// as a wall of angle brackets somebody has to squint at.
	if strings.HasPrefix(raw, "<") && !strings.Contains(raw, "```") {
		return "```\n" + raw + "\n```"
	}
	return raw
}

// jsonField is one top-level key and its value, IN THE ORDER THE RESPONSE
// WROTE THEM.
//
// Decoding into a map would lose that, and Go randomises map iteration - so
// the same webhook would list its fields in a different order every time it
// ran, which looks like the answer changing when nothing has.
type jsonField struct {
	Key   string
	Value json.RawMessage
}

// topLevelFields walks a JSON object one token at a time, keeping each value
// undecoded. Order survives, and so does the exact text of every number -
// decoding into `any` turns 1200 into a float64 and prints it as 1200 or
// 1.2e+03 depending on its size, which is not what the endpoint said.
func topLevelFields(raw string) ([]jsonField, bool) {
	if !strings.HasPrefix(raw, "{") {
		return nil, false
	}

	decoder := json.NewDecoder(strings.NewReader(raw))
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		return nil, false
	}

	var fields []jsonField
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, false
		}
		key, ok := token.(string)
		if !ok {
			return nil, false
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, false
		}
		fields = append(fields, jsonField{Key: key, Value: value})
	}

	// The closing brace, and then nothing: a body with a second document after
	// it is not an object, whatever its first character says.
	if _, err := decoder.Token(); err != nil {
		return nil, false
	}
	if decoder.More() {
		return nil, false
	}
	return fields, true
}

// topLevelItems needs no token walk - a slice is ordered already.
func topLevelItems(raw string) ([]json.RawMessage, bool) {
	if !strings.HasPrefix(raw, "[") {
		return nil, false
	}
	var items []json.RawMessage
	if json.Unmarshal([]byte(raw), &items) != nil {
		return nil, false
	}
	return items, true
}

func humanLine(fields []jsonField) string {
	for _, key := range answerKeys {
		for _, field := range fields {
			if field.Key != key {
				continue
			}
			// A STRING, specifically. `{"message": 42}` is a field that
			// happens to share a name with a sentence, and posting "42" as
			// the whole answer would drop every other field to say it.
			if !strings.HasPrefix(strings.TrimSpace(string(field.Value)), `"`) {
				continue
			}
			if text, ok := scalarText(field.Value); ok {
				if trimmed := strings.TrimSpace(text); trimmed != "" {
					return trimmed
				}
			}
		}
	}
	return ""
}

// maxListedFields is where a bullet list stops being easier to read than the
// block it replaces. Past this, the fence is the kinder answer.
const maxListedFields = 12

// maxInlineValue is how long a single value may be before the whole object
// goes in the fence - a paragraph hanging off a bullet is not a list.
const maxInlineValue = 160

// fieldList renders a FLAT object as one bullet per field.
//
// ALL OR NOTHING. One nested value, one key that would be read as markup, one
// value too long - and the whole object goes to the fence instead. A list
// where some fields are prose and others are raw JSON is harder to read than
// either, and mixing them would also mean deciding which of the two a reader
// should trust.
//
// Nesting is the fence's job and not this function's. Flattening it into
// dotted paths was tried and undone: `data.uptime.days` reads as a field name
// that does not exist, and both clients parse lists flat - an indented line is
// a continuation of the item above it, never a child - so there is no shape a
// list can show here that the block does not show better.
func fieldList(fields []jsonField) (string, bool) {
	if len(fields) == 0 || len(fields) > maxListedFields {
		return "", false
	}

	lines := make([]string, 0, len(fields))
	for _, field := range fields {
		if !plainKey(field.Key) {
			return "", false
		}
		text, ok := scalarText(field.Value)
		if !ok {
			return "", false
		}
		value, ok := inlineValue(text)
		if !ok {
			return "", false
		}
		lines = append(lines, "- **"+field.Key+"**: "+value)
	}
	return strings.Join(lines, "\n"), true
}

// itemList renders an array of scalars as a plain bullet list.
func itemList(items []json.RawMessage) (string, bool) {
	if len(items) == 0 || len(items) > maxListedFields {
		return "", false
	}

	lines := make([]string, 0, len(items))
	for _, item := range items {
		text, ok := scalarText(item)
		if !ok {
			return "", false
		}
		value, ok := inlineValue(text)
		if !ok {
			return "", false
		}
		lines = append(lines, "- "+value)
	}
	return strings.Join(lines, "\n"), true
}

// scalarText is a value's text, or false if it is an object or an array.
//
// A string comes back unquoted; everything else comes back exactly as the
// endpoint wrote it, which is the point of holding it as RawMessage.
func scalarText(value json.RawMessage) (string, bool) {
	trimmed := strings.TrimSpace(string(value))
	if trimmed == "" {
		return "", false
	}
	switch trimmed[0] {
	case '{', '[':
		return "", false
	case '"':
		var text string
		if json.Unmarshal(value, &text) != nil {
			return "", false
		}
		return text, true
	default:
		return trimmed, true
	}
}

// jsonScalar is the same thing for a body that is nothing BUT a scalar.
func jsonScalar(raw string) (string, bool) {
	if strings.HasPrefix(raw, "{") || strings.HasPrefix(raw, "[") {
		return "", false
	}
	var decoded any
	if json.Unmarshal([]byte(raw), &decoded) != nil {
		return "", false
	}
	if text, ok := decoded.(string); ok {
		return text, true
	}
	return raw, true
}

// plainKey says whether a key can be bolded without being read as markup.
//
// `**is_online**` is safe - both clients require emphasis delimiters to hug
// non-whitespace AND sit on a word boundary, so a single underscore inside a
// word never opens an italic. A doubled one would, and `*` inside the key
// would close the bold early, so both are refused and the object goes in the
// fence rather than rendering somebody's field name in italics.
func plainKey(key string) bool {
	if key == "" || len(key) > 40 || strings.Contains(key, "__") {
		return false
	}
	for _, r := range key {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '_', r == '-', r == '.', r == ' ':
		default:
			return false
		}
	}
	return true
}

// inlineValue is a value as it goes on the right of a bullet.
//
// Plain text stays plain, so a sentence reads as a sentence and a URL still
// autolinks. Anything carrying emphasis or link punctuation goes in a code
// span, where both clients' inline rules leave it alone - a value is data,
// and data must survive being displayed.
//
// A backtick has nowhere safe to go: the inline code rule on both sides is
// single-backtick only, so there is no delimiter that a backtick cannot close.
// That value refuses, and the caller falls back to the fence.
func inlineValue(text string) (string, bool) {
	if strings.ContainsAny(text, "`\n\r") || len(text) > maxInlineValue {
		return "", false
	}
	if strings.TrimSpace(text) == "" {
		// Quoted and monospaced rather than left blank: "- **note**: " reads
		// as a bug, and a value that is genuinely empty - or genuinely three
		// spaces - should be visible as what it is.
		return "`\"" + text + "\"`", true
	}
	if strings.ContainsAny(text, "*_~[]") {
		return "`" + text + "`", true
	}
	return text, true
}

// jsonFence is the exact response, pretty-printed, in a ```json block.
//
// `json.Indent` reformats the TEXT rather than re-encoding a decoded value, so
// key order and every number's spelling are the endpoint's own.
func jsonFence(raw string) string {
	if strings.Contains(raw, "```") {
		// A fence inside the body would close ours early and spill the rest
		// into the message as markup.
		return raw
	}

	var pretty bytes.Buffer
	if err := json.Indent(&pretty, []byte(raw), "", "  "); err != nil {
		// It parsed a moment ago, so this should not happen - post what came
		// back rather than nothing.
		return raw
	}
	return "```json\n" + pretty.String() + "\n```"
}

// clipAnswer bounds what is posted.
func clipAnswer(answer string) string {
	// runes := []rune(answer)
	// if len(runes) <= webhookAnswerLimit {
	// 	return answer
	// }

	// // Cut on runes, not bytes: half a character is not a shorter message, it
	// // is a broken one.
	// clipped := strings.TrimSpace(string(runes[:webhookAnswerLimit])) + "…"

	// // A cut inside a fence leaves it open. Both clients still render an
	// // unterminated fence as code, so this is about the message looking
	// // finished rather than about it working.
	// if strings.Count(clipped, "```")%2 == 1 {
	// 	clipped += "\n```"
	// }
	// return clipped
	return answer
}

// bodylessMethods carry the envelope in a header instead.
//
// Go will happily attach a body to a GET, and a fair number of servers,
// proxies and CDNs will quietly drop it - so a GET webhook that sent its
// envelope in a body would work in testing and lose the invoker in
// production, which is the worst way for this to fail.
var bodylessMethods = map[string]bool{
	http.MethodGet:    true,
	http.MethodDelete: true,
}

// envelopeHeader carries the envelope on a verb that has no body.
//
// WHY NOT THE QUERY STRING, WHICH IS WHAT THIS DID FIRST
//
// Because a webhook command points at somebody else's API, and a strict one
// rejects every parameter it does not recognise. newsdata.io answers 422
// "You can't use the conversation.id parameter" to a single added key - so a
// /top-news that worked when its URL was pasted into a browser failed as a
// command, for a reason nothing in the conversation could explain.
//
// An unknown HEADER is ignored by everything. That asymmetry is the whole
// argument: a receiver written for chatterloop still learns who typed the
// command and where, and a receiver that has never heard of chatterloop is
// not broken by being told.
//
// Only on bodyless verbs. POST, PUT and PATCH already carry the envelope in
// the body, and sending it twice would be two sources of truth that a
// receiver has to choose between.
const envelopeHeader = "X-Chatterloop-Envelope"

// envelopeHeaderLimit keeps a long command out of a 431.
//
// `command.args` is whatever somebody typed after the command, so it is the
// one unbounded field in the envelope. Most servers cap a header line around
// 8KB; past this the header is dropped and the request still goes, because a
// webhook that fires without context beats one that does not fire.
const envelopeHeaderLimit = 4096

// buildWebhookRequest assembles the outbound call from the stored definition.
//
// Four optional parts, all from the row and NONE from the invoker:
//
//	params   substituted into {placeholders} in the URL
//	query    appended to the query string
//	headers  added to the request
//	payload  merged into the body ALONGSIDE the envelope
//
// THE VERB DECIDES WHERE THE ENVELOPE GOES
//
// POST, PUT and PATCH carry it as JSON in the body, beside `payload`. GET and
// DELETE have no body, so it goes in the X-Chatterloop-Envelope header - see
// that constant for why a header and not the query string.
//
// `payload` is not sent on a bodyless verb. A definition that quietly moved
// into the query string would mean one row means two different things
// depending on its verb, and it would put a stored value somewhere the
// endpoint never agreed to read it from.
//
// THE QUERY STRING IS THE DEFINITION'S ALONE. Nothing this service derives
// goes in it, so a command against a strict API sends exactly the parameters
// somebody configured and no others.
//
// The envelope wins on a collision, in both encodings. A definition able to
// overwrite `invoker` would be a way to make a request claim it came from
// somebody else, which is exactly what deriving the envelope server-side
// prevents - so it is written after the stored parts.
//
// A user-supplied value must never reach `params`: the caller would then
// choose the host, and the host is the whole security boundary of an outbound
// call.
func buildWebhookRequest(row commandRow, envelope json.RawMessage) (*http.Request, error) {
	method := webhookMethod(row)
	target := applyParams(row.WebhookURL, row.WebhookRequest["params"])

	parsed, err := url.Parse(target)
	if err != nil {
		return nil, fmt.Errorf("bad webhook url: %w", err)
	}

	var envelopeFields map[string]any
	if len(envelope) > 0 {
		if err := json.Unmarshal(envelope, &envelopeFields); err != nil {
			return nil, fmt.Errorf("bad envelope: %w", err)
		}
	}

	query := parsed.Query()
	for key, value := range row.WebhookRequest["query"] {
		query.Set(key, fmt.Sprint(value))
	}
	parsed.RawQuery = query.Encode()

	var reader io.Reader
	var encoded []byte

	if !bodylessMethods[method] {
		body := map[string]any{}
		for key, value := range row.WebhookRequest["payload"] {
			body[key] = value
		}
		// Decoded and re-encoded so the envelope's keys sit beside the
		// payload's at the top level, and so the envelope overwrites rather
		// than nests.
		for key, value := range envelopeFields {
			body[key] = value
		}
		if encoded, err = json.Marshal(body); err != nil {
			return nil, err
		}
		reader = bytes.NewReader(encoded)
	}

	request, err := http.NewRequest(method, parsed.String(), reader)
	if err != nil {
		return nil, err
	}
	if encoded != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	// Before the envelope header, so a stored header can override the
	// Content-Type above - some endpoints want a vendor type - and cannot
	// touch what this service says about who sent the command.
	for key, value := range row.WebhookRequest["headers"] {
		request.Header.Set(key, fmt.Sprint(value))
	}
	if bodylessMethods[method] {
		setEnvelopeHeader(request, row, envelope)
	}
	return request, nil
}

// setEnvelopeHeader attaches the envelope to a request that has no body.
//
// The envelope as it arrived, compacted. JSON escapes every control character,
// so the result is a valid header value by construction - a newline somebody
// typed after the command comes out as `\n` rather than as a second header.
func setEnvelopeHeader(request *http.Request, row commandRow, envelope json.RawMessage) {
	if len(envelope) == 0 {
		return
	}

	var compact bytes.Buffer
	if err := json.Compact(&compact, envelope); err != nil {
		slog.Warn("could not compact the envelope for the header",
			"command", row.Name, "error", err)
		return
	}

	if compact.Len() > envelopeHeaderLimit {
		// The request still goes. A webhook that fires without context beats
		// one that does not fire because somebody typed an essay.
		slog.Warn("the envelope is too long to send as a header; sending without it",
			"command", row.Name, "bytes", compact.Len(), "limit", envelopeHeaderLimit)
		return
	}

	request.Header.Set(envelopeHeader, compact.String())
}

// webhookMethod is the row's verb, upper-cased, falling back to POST.
//
// The column is NULL for every command that makes no request, and may be NULL
// on a webhook row too - a caller that does not care about the verb does not
// have to name one. The query COALESCEs it to an empty string, which lands
// here alongside the other reasons a verb might not be readable: a row
// written before the column existed, or one carrying a value this build does
// not know. All of them mean POST, which is what every webhook command did
// before the column existed - so an unreadable verb behaves like the old code
// rather than failing.
func webhookMethod(row commandRow) string {
	switch strings.ToUpper(strings.TrimSpace(row.WebhookMethod)) {
	case http.MethodGet:
		return http.MethodGet
	case http.MethodPut:
		return http.MethodPut
	case http.MethodPatch:
		return http.MethodPatch
	case http.MethodDelete:
		return http.MethodDelete
	default:
		return http.MethodPost
	}
}

// applyParams fills {placeholders} from the stored definition.
//
// An unknown placeholder is left as written rather than blanked, so a
// mis-wired command produces a URL somebody can recognise instead of one that
// quietly points somewhere else. Values are escaped, so a value cannot climb
// out of its segment.
func applyParams(raw string, params map[string]any) string {
	if len(params) == 0 {
		return raw
	}
	out := raw
	for key, value := range params {
		out = strings.ReplaceAll(
			out, "{"+key+"}", url.PathEscape(fmt.Sprint(value)))
	}
	return out
}

func loadCommand(ctx context.Context, id string) (commandRow, error) {
	const query = `
		SELECT c.id, c.name, c.category, c.responds,
		       c.webhook_url, COALESCE(c.webhook_method, ''), c.webhook_request,
		       b.id, b.entity_id, b.handle, b.is_system
		  FROM bot_commands c
		  JOIN bot_bot b ON b.id = c.bot_id
		 WHERE c.id = $1 AND c.is_active AND b.is_active`

	var row commandRow
	var request []byte

	err := connections.Pool().QueryRow(ctx, query, id).Scan(
		&row.ID, &row.Name, &row.Category, &row.Responds,
		&row.WebhookURL, &row.WebhookMethod, &request,
		&row.BotID, &row.BotEntityID, &row.BotHandle, &row.BotIsSystem,
	)
	if err != nil {
		return commandRow{}, err
	}

	if len(request) > 0 {
		// Malformed JSON here must not take the command down - the four parts
		// are all optional, and an empty map runs the command without them.
		if err := json.Unmarshal(request, &row.WebhookRequest); err != nil {
			slog.Warn("webhook_request is not the expected shape",
				"command_id", id, "error", err)
			row.WebhookRequest = nil
		}
	}
	if row.WebhookRequest == nil {
		row.WebhookRequest = map[string]map[string]any{}
	}
	return row, nil
}
