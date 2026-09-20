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
	if row.Responds == "none" || answer == "" {
		return
	}

	// Always as System, whichever bot owns the row. worker_service holds no
	// bot's token and cannot speak as one; what it can do is speak as the
	// platform, which is what a built-in answer is.
	var parsed commandEnvelope
	if err := json.Unmarshal(envelope, &parsed); err != nil {
		slog.Error("could not read the envelope", "command", row.Name, "error", err)
		return
	}

	if err := PostSystemReply(ctx, parsed.Conversation.ID, answer); err != nil {
		slog.Error("could not post the reply",
			"command", row.Name, "conversation_id", parsed.Conversation.ID, "error", err)
	}
}

func runWebhookCommand(ctx context.Context, row commandRow, envelope json.RawMessage) {
	request, err := buildWebhookRequest(row, envelope)
	if err != nil {
		slog.Error("could not build webhook request", "command", row.Name, "error", err)
		return
	}

	ctx, cancel := context.WithTimeout(ctx, commandTimeout)
	defer cancel()
	request = request.WithContext(ctx)

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		slog.Error("webhook command failed", "command", row.Name, "error", err)
		return
	}
	defer response.Body.Close()

	// Drained and discarded: the receiver answers in the conversation, not
	// here, and an unread body holds the connection out of the pool.
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))

	if response.StatusCode >= 400 {
		slog.Error("webhook command rejected",
			"command", row.Name, "status", response.StatusCode, "url", row.WebhookURL)
		return
	}
	slog.Info("webhook command delivered",
		"command", row.Name, "bot", row.BotHandle, "status", response.StatusCode)
}

// buildWebhookRequest assembles the outbound call from the stored definition.
//
// Four optional parts, all from the row and NONE from the invoker:
//
//	params   substituted into {placeholders} in the URL
//	query    appended to the query string
//	headers  added to the request
//	payload  merged into the body ALONGSIDE the envelope
//
// The envelope wins on a key collision. A definition able to overwrite
// `invoker` would be a way to make a request claim it came from somebody else,
// which is exactly what deriving the envelope server-side prevents.
//
// A user-supplied value must never reach `params`: the caller would then
// choose the host, and the host is the whole security boundary of an outbound
// call.
func buildWebhookRequest(row commandRow, envelope json.RawMessage) (*http.Request, error) {
	target := applyParams(row.WebhookURL, row.WebhookRequest["params"])

	parsed, err := url.Parse(target)
	if err != nil {
		return nil, fmt.Errorf("bad webhook url: %w", err)
	}
	query := parsed.Query()
	for key, value := range row.WebhookRequest["query"] {
		query.Set(key, fmt.Sprint(value))
	}
	parsed.RawQuery = query.Encode()

	body := map[string]any{}
	for key, value := range row.WebhookRequest["payload"] {
		body[key] = value
	}
	// Decoded and re-encoded so the envelope's keys sit beside the payload's
	// at the top level, and so the envelope overwrites rather than nests.
	var envelopeFields map[string]any
	if len(envelope) > 0 {
		if err := json.Unmarshal(envelope, &envelopeFields); err != nil {
			return nil, fmt.Errorf("bad envelope: %w", err)
		}
	}
	for key, value := range envelopeFields {
		body[key] = value
	}

	encoded, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	request, err := http.NewRequest(http.MethodPost, parsed.String(), bytes.NewReader(encoded))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	for key, value := range row.WebhookRequest["headers"] {
		request.Header.Set(key, fmt.Sprint(value))
	}
	return request, nil
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
		       c.webhook_url, c.webhook_request,
		       b.id, b.entity_id, b.handle, b.is_system
		  FROM bot_commands c
		  JOIN bot_bot b ON b.id = c.bot_id
		 WHERE c.id = $1 AND c.is_active AND b.is_active`

	var row commandRow
	var request []byte

	err := connections.Pool().QueryRow(ctx, query, id).Scan(
		&row.ID, &row.Name, &row.Category, &row.Responds,
		&row.WebhookURL, &request,
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
