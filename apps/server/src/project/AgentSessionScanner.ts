import * as NodeOS from "node:os";

import {
  AgentSessionScanError,
  ClaudeSettings,
  CodexSettings,
  ProviderDriverKind,
  ProviderInstanceId,
  resolveProviderInstanceEnabled,
  type AgentSessionImportSkipReason,
  type AgentSessionImportSource,
  type AgentSessionProjectCandidate,
  type AgentSessionProjectGit,
  type AgentSessionScanResult,
  type ProviderInstanceConfig,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";

import {
  normalizeGitRemoteUrl,
  isBareGitConfig,
  parseGitHubRepositoryNameWithOwnerFromRemoteUrl,
  parseOriginUrlFromGitConfig,
} from "@t3tools/shared/git";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { normalizeProjectPathForComparison } from "@t3tools/shared/path";
import {
  DROID_HOME_DIRECTORY,
  DROID_SESSIONS_DIRECTORY,
  droidSessionParentId,
  droidTranscriptEntries,
  droidTranscriptMessage,
  foldDroidTranscriptFields,
  isDroidSubagentSessionStart,
  isDroidTranscriptHistoryRecord,
  type DroidTranscriptActivity,
} from "@t3tools/provider-droid/server";

import * as ServerConfig from "../config.ts";
import * as ProjectStore from "../orchestration-v2/ProjectStore.ts";
import { resolveCodexHomeLayout } from "../provider/Drivers/CodexHomeLayout.ts";
import { expandHomePath } from "@t3tools/provider-core/server/pathExpansion";
import * as ServerSettings from "../serverSettings.ts";
import {
  createTranscriptJsonReader,
  createTranscriptJsonSelector,
  TranscriptJsonLimitError,
} from "./AgentSessionJson.ts";

const TRANSCRIPT_PREFIX_BYTES = 32 * 1024;
const METADATA_READ_BYTES = 8 * 1024;
const MAX_TRANSCRIPT_SCAN_BYTES = 1024 * 1024;

const MAX_TRANSCRIPTS_PER_SOURCE = 5000;

const MAX_DISCOVERY_OPERATIONS_PER_SOURCE = MAX_TRANSCRIPTS_PER_SOURCE * 4;
const MAX_METADATA_BYTES_PER_SOURCE = 64 * 1024 * 1024;
const MAX_METADATA_OPERATIONS_PER_SOURCE = MAX_TRANSCRIPTS_PER_SOURCE * 4;
const MAX_METADATA_RECORDS_PER_SOURCE = 100_000;
const MAX_METADATA_RECORDS_PER_TRANSCRIPT = 1_000;
const RECENT_THREAD_WINDOW_MS = 30 * 24 * 60 * 60 * 1000;
/**
 * Large tool results (especially screenshots) can make an otherwise ordinary
 * Codex transcript several GiB. Streaming field selection avoids allocating
 * those payloads.
 */
const MAX_IMPORTED_TRANSCRIPT_BYTES = 4 * 1024 * 1024 * 1024;
const MAX_IMPORTED_MESSAGES = 200;
const MAX_IMPORT_HISTORY_BYTES = 32 * 1024 * 1024;
/**
 * Droid sessions import whole, tool calls included, so the user can continue where they were.
 * Past this budget the oldest history is dropped (the first prompt stays) instead of the session.
 */
const MAX_DROID_IMPORT_HISTORY_BYTES = 128 * 1024 * 1024;
// A tool row shows only the last 8,000 characters, but an edit's +/- counts parse its whole result.
const MAX_DROID_TOOL_RESULT_CHARS = 64 * 1024;
const MAX_IMPORT_BYTES = 4 * 1024 * 1024 * 1024;
const MAX_IMPORT_TRANSCRIPTS = 100;
const MAX_IMPORT_RECORDS = 100_000;

const TranscriptContentBlock = Schema.Struct({
  type: Schema.optional(Schema.String),
  text: Schema.optional(Schema.String),
});

const TranscriptMessage = Schema.Struct({
  role: Schema.optional(Schema.String),
  content: Schema.optional(Schema.Union([Schema.String, Schema.Array(TranscriptContentBlock)])),
  model: Schema.optional(Schema.String),
  // Droid's; Unknown so another source's same-named field cannot fail its record.
  modelId: Schema.optional(Schema.Unknown),
  visibility: Schema.optional(Schema.Unknown),
});

const CodexTurnMetadata = Schema.Struct({
  turn_id: Schema.optional(Schema.Union([Schema.String, Schema.Null])),
});

const TranscriptRecord = Schema.Struct({
  type: Schema.optional(Schema.String),
  timestamp: Schema.optional(Schema.String),
  cwd: Schema.optional(Schema.String),
  sessionId: Schema.optional(Schema.String),
  aiTitle: Schema.optional(Schema.String),
  customTitle: Schema.optional(Schema.String),
  isSidechain: Schema.optional(Schema.Boolean),
  isMeta: Schema.optional(Schema.Boolean),
  isCompactSummary: Schema.optional(Schema.Boolean),
  // Droid's; Unknown so another source's same-named field cannot fail its record.
  id: Schema.optional(Schema.Unknown),
  title: Schema.optional(Schema.Unknown),
  message: Schema.optional(TranscriptMessage),
  payload: Schema.optional(
    Schema.Struct({
      id: Schema.optional(Schema.String),
      session_id: Schema.optional(Schema.String),
      type: Schema.optional(Schema.String),
      role: Schema.optional(Schema.String),
      message: Schema.optional(Schema.String),
      model: Schema.optional(Schema.String),
      cwd: Schema.optional(Schema.String),
      content: Schema.optional(Schema.Array(TranscriptContentBlock)),
      internal_chat_message_metadata_passthrough: Schema.optional(Schema.Unknown),
    }),
  ),
});

// Droid keeps tool calls and results as content blocks; only Droid imports read them.
const DroidTranscriptRecord = Schema.Struct({
  ...TranscriptRecord.fields,
  message: Schema.optional(
    Schema.Struct({
      ...TranscriptMessage.fields,
      content: Schema.optional(
        Schema.Union([
          Schema.String,
          Schema.Array(
            Schema.Struct({
              ...TranscriptContentBlock.fields,
              id: Schema.optional(Schema.Unknown),
              name: Schema.optional(Schema.Unknown),
              input: Schema.optional(Schema.Unknown),
              tool_use_id: Schema.optional(Schema.Unknown),
              is_error: Schema.optional(Schema.Unknown),
              thinking: Schema.optional(Schema.String),
              // Typed, not Unknown, so image data in a result is never read.
              content: Schema.optional(
                Schema.Union([Schema.String, Schema.Array(TranscriptContentBlock)]),
              ),
            }),
          ),
        ]),
      ),
    }),
  ),
});

const decodeClaudeSettings = Schema.decodeUnknownOption(ClaudeSettings);
const decodeCodexSettings = Schema.decodeUnknownOption(CodexSettings);
const decodeTranscriptRecord = Schema.decodeUnknownOption(Schema.fromJsonString(TranscriptRecord));
const decodeDroidTranscriptRecord = Schema.decodeUnknownOption(
  Schema.fromJsonString(DroidTranscriptRecord),
);
const decodeTranscriptValue = Schema.decodeUnknownOption(TranscriptRecord);
const decodeDroidTranscriptValue = Schema.decodeUnknownOption(DroidTranscriptRecord);
const selectTranscriptPath = createTranscriptJsonSelector(TranscriptRecord);
const selectDroidTranscriptPath = createTranscriptJsonSelector(DroidTranscriptRecord);
const decodeCodexTurnMetadata = Schema.decodeUnknownOption(CodexTurnMetadata);
// Droid keeps a session's model and reasoning effort beside its transcript, not in it.
const decodeDroidSessionSettings = Schema.decodeUnknownOption(
  Schema.fromJsonString(
    Schema.Struct({
      model: Schema.optional(Schema.String),
      reasoningEffort: Schema.optional(Schema.String),
    }),
  ),
);
const MAX_DROID_SESSION_SETTINGS_BYTES = 1024 * 1024;

const droidStringLimit = (path: ReadonlyArray<string | number | null>) =>
  path[0] === "message" && path[1] === "content" && path[3] === "content"
    ? MAX_DROID_TOOL_RESULT_CHARS
    : Number.POSITIVE_INFINITY;

type DecodedTranscriptRecord = typeof TranscriptRecord.Type;

interface AgentSessionTranscriptMetadata {
  readonly source: AgentSessionSource;
  readonly providerInstanceId: ProviderInstanceId;
  readonly fallbackSessionId: string;
  readonly lastActiveAtMs: number;
}

export interface AgentSessionThreadMessage {
  readonly role: "user" | "assistant";
  readonly text: string;
  readonly createdAt: string;
}

/** Work shown between messages: Droid's tool calls, subagents, and notices, or a note about the import. */
export interface AgentSessionThreadActivity {
  /** How many of the thread's messages come before it. */
  readonly position: number;
  readonly startedAt: string;
  readonly completedAt: string;
  readonly activity: DroidTranscriptActivity;
}

export interface AgentSessionWorktree {
  readonly path: string;
  readonly branch: string | null;
}

export interface AgentSessionThread {
  readonly source: AgentSessionSource;
  readonly providerInstanceId: ProviderInstanceId;
  readonly providerSessionId: string;
  readonly title: string;
  readonly model: string | null;
  /** The effort the session ran `model` at, when the provider records one. */
  readonly reasoningEffort?: string;
  /** The linked worktree of the project the session ran in. */
  readonly worktree?: AgentSessionWorktree;
  readonly createdAt: string;
  readonly updatedAt: string;
  readonly messages: ReadonlyArray<AgentSessionThreadMessage>;
  readonly activities: ReadonlyArray<AgentSessionThreadActivity>;
}

const TRIMMED_HISTORY_NOTICE =
  "Earlier history was too large to import. The conversation continues from its most recent part.";

// Raised when a Droid import starts showing more of a session (2: thinking). Threads imported
// with an older version get their history again on the next import.
const DROID_HISTORY_VERSION = 2;

const historyVersionFor = (provider: AgentSessionSource) =>
  provider === "droid" ? DROID_HISTORY_VERSION : 1;

const hasCurrentHistory = (source: AgentSessionImportSource) =>
  (source.historyVersion ?? 1) >= historyVersionFor(source.provider);

export type AgentSessionRecentThread =
  | {
      readonly _tag: "Importable";
      readonly thread: AgentSessionThread;
      readonly source: AgentSessionImportSource;
      /** The transcript was imported before, with less of its history than an import shows now. */
      readonly refreshHistory: boolean;
      /** Sessions forked or rewound from this one, directly or through each other. */
      readonly continuedIn?: ReadonlyArray<string>;
    }
  | { readonly _tag: "AlreadyImported"; readonly source: AgentSessionImportSource }
  | { readonly _tag: "Duplicate"; readonly source: AgentSessionImportSource }
  | { readonly _tag: "Skipped"; readonly reason: AgentSessionSkipReason };

/** Why a session was not imported; "empty" sessions had no conversation to import. */
export type AgentSessionSkipReason = AgentSessionImportSkipReason | "empty";

export class AgentSessionScanner extends Context.Service<
  AgentSessionScanner,
  {
    readonly scan: Effect.Effect<AgentSessionScanResult, AgentSessionScanError>;
    readonly recentThreads: (
      workspaceRoot: string,
      completedSources?: ReadonlyArray<AgentSessionImportSource>,
    ) => Stream.Stream<AgentSessionRecentThread, AgentSessionScanError>;
  }
>()("t3/project/AgentSessionScanner") {}

type AgentSessionSource = AgentSessionProjectCandidate["sources"][number];

interface RawCandidate {
  readonly cwd: string;
  readonly source: AgentSessionSource;
  readonly providerInstanceId: ProviderInstanceId;
  readonly threadCount: number;
  readonly lastActiveAtMs: number | null;
  readonly transcripts: ReadonlyArray<{
    readonly filePath: string;
    readonly mtimeMs: number | null;
    /** Droid: the session this one was forked or rewound from. */
    readonly parentSessionId?: string;
  }>;
}

interface TranscriptCandidate {
  readonly filePath: string;
  readonly mtimeMs: number;
  readonly providerInstanceId: ProviderInstanceId;
  readonly size: number;
}

interface MetadataReadBudget {
  bytesRemaining: number;
  operationsRemaining: number;
  recordsRemaining: number;
  truncated: boolean;
}

function selectMetadataTranscripts(transcripts: ReadonlyArray<TranscriptCandidate>) {
  const selected: Array<TranscriptCandidate> = [];
  let pending = Array.from(
    Map.groupBy(transcripts, (transcript) => transcript.providerInstanceId).values(),
    (entries) => entries.values(),
  );
  while (pending.length > 0 && selected.length < MAX_TRANSCRIPTS_PER_SOURCE) {
    const nextRound: typeof pending = [];
    for (const iterator of pending) {
      if (selected.length === MAX_TRANSCRIPTS_PER_SOURCE) break;
      const next = iterator.next();
      if (next.done) continue;
      selected.push(next.value);
      nextRound.push(iterator);
    }
    pending = nextRound;
  }
  return selected;
}

function splitTranscriptRecords(contents: string, limit: number): string[] {
  const records = contents.endsWith("\n") ? contents.slice(0, -1) : contents;
  return records.split("\n", limit);
}

function isTextContentBlock(block: typeof TranscriptContentBlock.Type): boolean {
  return block.type === "text" || block.type === "input_text" || block.type === "output_text";
}

function extractText(
  content: string | ReadonlyArray<typeof TranscriptContentBlock.Type> | undefined,
): string {
  if (typeof content === "string") return content.trim();
  if (content === undefined) return "";
  return content
    .filter(isTextContentBlock)
    .map((block) => block.text?.trim() ?? "")
    .filter((text) => text.length > 0)
    .join("\n");
}

function normalizeTimestamp(value: string | undefined, fallback: string): string {
  if (value === undefined) return fallback;
  const parsed = DateTime.make(value);
  return Option.isSome(parsed) ? DateTime.formatIso(parsed.value) : fallback;
}

function codexTurnId(metadata: unknown): string | null {
  const decoded = decodeCodexTurnMetadata(metadata);
  if (
    Option.isNone(decoded) ||
    typeof decoded.value.turn_id !== "string" ||
    decoded.value.turn_id.trim().length === 0
  ) {
    return null;
  }
  return decoded.value.turn_id;
}

function isHiddenClaudeRecord(record: DecodedTranscriptRecord): boolean {
  return record.isSidechain === true || record.isMeta === true || record.isCompactSummary === true;
}

// Claude repeats session fields on records that need not be retained.
interface ClaudeFields {
  readonly sessionId: string | undefined;
  readonly title: string | undefined;
  readonly customTitle: string | null;
  readonly model: string | undefined;
}

const emptyClaudeFields: ClaudeFields = {
  sessionId: undefined,
  title: undefined,
  customTitle: null,
  model: undefined,
};

function foldClaudeFields(fields: ClaudeFields, record: DecodedTranscriptRecord): ClaudeFields {
  if (isHiddenClaudeRecord(record)) return fields;
  const model = record.message?.model?.trim();
  return {
    sessionId: record.sessionId?.trim() || fields.sessionId,
    title: record.aiTitle?.trim() || fields.title,
    // Claude flushes the AI title after the user's rename; an empty rename clears it.
    customTitle:
      record.customTitle === undefined ? fields.customTitle : record.customTitle.trim() || null,
    // Claude uses this sentinel for local error responses. It is not a
    // model ID that can be selected when the imported session resumes.
    model: model && model !== "<synthetic>" ? model : fields.model,
  };
}

// Codex records setup context as user text ahead of the real request. Skip
// these leading blocks when deriving a title; imported messages keep them.
const CODEX_TITLE_PREAMBLE =
  /^(?:\s*(?:<(recommended_plugins|environment_context|user_instructions|INSTRUCTIONS|in-app-browser-context)(?:\s[^>]*)?>[\s\S]*?<\/\1>|# AGENTS\.md instructions\b[^\n]*|## My request for Codex:))+/;

function deriveTitle(source: AgentSessionSource, text: string): string | undefined {
  const request = source === "codex" ? text.replace(CODEX_TITLE_PREAMBLE, "") : text;
  return request.trim().split("\n")[0]?.slice(0, 100).trim() || undefined;
}

export function parseAgentSessionTranscript(
  input: AgentSessionTranscriptMetadata & {
    readonly contents: string;
  },
  lines = splitTranscriptRecords(input.contents, MAX_IMPORT_RECORDS + 1),
): AgentSessionThread | null {
  if (lines.length > MAX_IMPORT_RECORDS) return null;
  const decode = input.source === "droid" ? decodeDroidTranscriptRecord : decodeTranscriptRecord;
  const records = lines.flatMap(
    (line): ReadonlyArray<DecodedTranscriptRecord> => Option.toArray(decode(line)),
  );
  const claudeFields =
    input.source === "claudeAgent"
      ? records.reduce(foldClaudeFields, emptyClaudeFields)
      : input.source === "droid"
        ? records.reduce(foldDroidTranscriptFields, emptyClaudeFields)
        : emptyClaudeFields;
  return parseAgentSessionRecords(input, records, claudeFields);
}

function parseAgentSessionRecords(
  input: AgentSessionTranscriptMetadata,
  records: ReadonlyArray<DecodedTranscriptRecord>,
  claudeFields: ClaudeFields,
  omittedRecords = 0,
): AgentSessionThread | null {
  const fallbackTimestamp = DateTime.formatIso(DateTime.makeUnsafe(input.lastActiveAtMs));
  // Claude filenames are session IDs. Codex rollout filenames include extra
  // timestamp text, so only transcript metadata can provide a resumable ID.
  let providerSessionId =
    input.source === "codex" ? "" : (claudeFields.sessionId ?? input.fallbackSessionId);
  const title = claudeFields.title ?? null;
  let model = claudeFields.model ?? null;
  let derivedTitle: string | undefined;
  let hasCodexSessionId = false;
  const messages: Array<AgentSessionThreadMessage & { readonly codexResponseUser: boolean }> = [];
  let firstUserMessage:
    | (AgentSessionThreadMessage & { readonly codexResponseUser: boolean })
    | undefined;
  const canonicalCodexResponseUserIndices = new Set<number>();
  let canonicalUserTextsInTurn = new Set<string>();
  let responseUsersInTurn: Array<{
    readonly index: number;
    readonly turnId: string;
    readonly text: string;
  }> = [];
  const finishCodexTurn = () => {
    const canonicalTurnIds = new Set(
      responseUsersInTurn.flatMap((responseUser) =>
        canonicalUserTextsInTurn.has(responseUser.text) ? [responseUser.turnId] : [],
      ),
    );
    for (const responseUser of responseUsersInTurn) {
      if (canonicalTurnIds.has(responseUser.turnId)) {
        canonicalCodexResponseUserIndices.add(responseUser.index);
      }
    }
    canonicalUserTextsInTurn = new Set();
    responseUsersInTurn = [];
  };
  if (input.source === "codex") {
    let recordIndex = -1;
    for (const record of records) {
      recordIndex += 1;
      if (
        record.type === "response_item" &&
        record.payload?.type === "message" &&
        record.payload.role === "assistant"
      ) {
        finishCodexTurn();
        continue;
      }
      if (record.type === "event_msg" && record.payload?.type === "user_message") {
        const text = record.payload.message?.trim() ?? "";
        if (text.length > 0) canonicalUserTextsInTurn.add(text);
        continue;
      }
      if (
        record.type === "response_item" &&
        record.payload?.type === "message" &&
        record.payload.role === "user"
      ) {
        const turnId = codexTurnId(record.payload.internal_chat_message_metadata_passthrough);
        const text = extractText(record.payload.content);
        if (turnId !== null && text.length > 0) {
          responseUsersInTurn.push({ index: recordIndex, turnId, text });
        }
      }
    }
    finishCodexTurn();
  }

  const retainMessage = (
    message: AgentSessionThreadMessage & { readonly codexResponseUser: boolean },
  ) => {
    if (firstUserMessage === undefined && message.role === "user") {
      firstUserMessage = message;
    }
    if (derivedTitle === undefined && message.role === "user") {
      derivedTitle = deriveTitle(input.source, message.text);
    }
    messages.push(message);
    if (input.source !== "droid" && messages.length > MAX_IMPORTED_MESSAGES) messages.shift();
  };

  const activities: Array<AgentSessionThreadActivity> = [];
  if (input.source === "droid") {
    for (const entry of droidTranscriptEntries(records)) {
      if (entry.type === "message") {
        retainMessage({
          role: entry.role,
          text: entry.text,
          createdAt: normalizeTimestamp(entry.timestamp, fallbackTimestamp),
          codexResponseUser: false,
        });
        continue;
      }
      activities.push({
        position: messages.length,
        startedAt: normalizeTimestamp(entry.startedAt, fallbackTimestamp),
        completedAt: normalizeTimestamp(entry.completedAt, fallbackTimestamp),
        activity: entry.activity,
      });
    }
    if (omittedRecords > 0 && messages.length > 0) {
      // The first prompt is always kept; the note sits where the dropped history was.
      const at = messages[0]!.createdAt;
      const index = activities.findIndex((activity) => activity.position >= 1);
      activities.splice(index === -1 ? activities.length : index, 0, {
        position: 1,
        startedAt: at,
        completedAt: at,
        activity: { kind: "notice", message: TRIMMED_HISTORY_NOTICE },
      });
    }
  }

  const hasMatchingCodexEventInTurn = (text: string) => {
    const comparisonText = text.trim();
    for (let index = messages.length - 1; index >= 0; index--) {
      const message = messages[index];
      if (message?.role === "assistant") return false;
      if (
        message?.role === "user" &&
        !message.codexResponseUser &&
        message.text.trim() === comparisonText
      ) {
        return true;
      }
    }
    return false;
  };

  let recordIndex = -1;
  for (const record of input.source === "droid" ? [] : records) {
    recordIndex += 1;
    if (input.source === "claudeAgent") {
      if (isHiddenClaudeRecord(record)) continue;
      if (record.type !== "user" && record.type !== "assistant") continue;
      const text = extractText(record.message?.content);
      if (text.length === 0) continue;
      retainMessage({
        role: record.type,
        text,
        createdAt: normalizeTimestamp(record.timestamp, fallbackTimestamp),
        codexResponseUser: false,
      });
      continue;
    }

    if (record.type === "session_meta") {
      const sessionId = record.payload?.id?.trim() || record.payload?.session_id?.trim();
      if (!hasCodexSessionId && sessionId) {
        providerSessionId = sessionId;
        hasCodexSessionId = true;
      }
      continue;
    }
    if (record.type === "turn_context" && record.payload?.model?.trim()) {
      model = record.payload.model.trim();
      continue;
    }
    if (record.type === "event_msg" && record.payload?.type === "user_message") {
      const text = record.payload.message ?? "";
      if (text.trim().length === 0) continue;
      // Codex can write the same prompt as both a response item and an event.
      // Remove only the matching response copy so mixed-format logs keep every
      // distinct user message.
      for (let index = messages.length - 1; index >= 0; index--) {
        const message = messages[index];
        if (message?.role === "assistant") break;
        if (message?.codexResponseUser === true && message.text.trim() === text.trim()) {
          if (firstUserMessage === message) firstUserMessage = undefined;
          messages.splice(index, 1);
          break;
        }
      }
      retainMessage({
        role: "user",
        text,
        createdAt: normalizeTimestamp(record.timestamp, fallbackTimestamp),
        codexResponseUser: false,
      });
      continue;
    }
    if (
      record.type !== "response_item" ||
      record.payload?.type !== "message" ||
      (record.payload.role !== "user" && record.payload.role !== "assistant")
    ) {
      continue;
    }

    const extractedText = extractText(record.payload.content);
    if (extractedText.length === 0) continue;
    if (record.payload.role === "user" && canonicalCodexResponseUserIndices.has(recordIndex)) {
      continue;
    }
    if (record.payload.role === "user" && hasMatchingCodexEventInTurn(extractedText)) {
      continue;
    }
    retainMessage({
      role: record.payload.role,
      text: extractedText,
      createdAt: normalizeTimestamp(record.timestamp, fallbackTimestamp),
      codexResponseUser: record.payload.role === "user",
    });
  }

  const visibleMessages = messages.map(
    ({ codexResponseUser: _codexResponseUser, ...message }) => message,
  );
  if (providerSessionId.trim().length === 0 || firstUserMessage === undefined) return null;
  const firstUserMessageRetained = messages.includes(firstUserMessage);
  const { codexResponseUser: _codexResponseUser, ...visibleFirstUserMessage } = firstUserMessage;
  const retainedMessages = firstUserMessageRetained
    ? visibleMessages
    : [visibleFirstUserMessage, ...visibleMessages.slice(-(MAX_IMPORTED_MESSAGES - 1))];

  return {
    source: input.source,
    providerInstanceId: input.providerInstanceId,
    providerSessionId,
    title: claudeFields.customTitle ?? title ?? derivedTitle ?? "Imported thread",
    model,
    createdAt: retainedMessages[0]?.createdAt ?? fallbackTimestamp,
    updatedAt: fallbackTimestamp,
    messages: retainedMessages,
    activities,
  };
}

function extractDecodedCwd(record: DecodedTranscriptRecord): string | null {
  const cwd = record.cwd?.trim() || record.payload?.cwd?.trim();
  return cwd && cwd.length > 0 ? cwd : null;
}

function shouldRetainDecodedRecord(
  source: AgentSessionSource,
  record: DecodedTranscriptRecord,
): boolean {
  if (source === "droid") return isDroidTranscriptHistoryRecord(record);
  if (source === "claudeAgent") {
    if (isHiddenClaudeRecord(record) || (record.type !== "user" && record.type !== "assistant")) {
      return false;
    }
    const content = record.message?.content;
    return typeof content === "string"
      ? content.trim().length > 0
      : (content?.some(
          (block) => isTextContentBlock(block) && (block.text?.trim().length ?? 0) > 0,
        ) ?? false);
  }
  if (extractDecodedCwd(record) !== null) return true;
  return (
    record.type === "session_meta" ||
    record.type === "turn_context" ||
    (record.type === "event_msg" && record.payload?.type === "user_message") ||
    (record.type === "response_item" &&
      record.payload?.type === "message" &&
      (record.payload.role === "user" || record.payload.role === "assistant"))
  );
}

function normalizeForWorktreeMatch(value: string, caseFold: boolean): string {
  const normalized = `${value.replaceAll("\\", "/")}/`;
  return caseFold ? normalized.toLowerCase() : normalized;
}

function isT3ManagedWorktree(
  candidatePath: string,
  worktreesDir: string,
  caseFold: boolean,
): boolean {
  const normalized = normalizeForWorktreeMatch(candidatePath, caseFold);
  return (
    normalized.startsWith(normalizeForWorktreeMatch(worktreesDir, caseFold)) ||
    normalized.includes("/.t3/worktrees/")
  );
}

interface ProjectPlacement {
  readonly projectPath: string;
  /** The project directory's identity. */
  readonly key: string;
  readonly git: AgentSessionProjectGit | null;
  readonly worktree?: AgentSessionWorktree;
}

interface TranscriptStart {
  readonly cwd: string;
  readonly parentSessionId?: string;
}

// false stops metadata scanning for an excluded rollout, even if later records contain a cwd.
function extractCwd(line: string, source: AgentSessionSource): TranscriptStart | null | false {
  let parsed: unknown;
  try {
    parsed = JSON.parse(line);
  } catch {
    return null;
  }
  if (typeof parsed !== "object" || parsed === null) return null;

  const record = parsed as Record<string, unknown>;
  if (source === "droid" && isDroidSubagentSessionStart(record)) return false;
  if (typeof record.cwd === "string" && record.cwd.trim().length > 0) {
    const parentSessionId = source === "droid" ? droidSessionParentId(record) : undefined;
    return { cwd: record.cwd, ...(parentSessionId === undefined ? {} : { parentSessionId }) };
  }
  const payload = record.payload;
  if (typeof payload === "object" && payload !== null) {
    if (source === "codex" && record.type === "session_meta") {
      const sessionSource = (payload as Record<string, unknown>).source;
      if (
        typeof sessionSource === "object" &&
        sessionSource !== null &&
        !Array.isArray(sessionSource) &&
        (Object.hasOwn(sessionSource, "subagent") || Object.hasOwn(sessionSource, "subAgent"))
      ) {
        return false;
      }
    }
    const nested = (payload as Record<string, unknown>).cwd;
    if (typeof nested === "string" && nested.trim().length > 0) {
      return { cwd: nested };
    }
  }
  return null;
}

function transcriptIdentity(filePath: string, stats: FileSystem.File.Info) {
  return {
    filePath,
    size: Number(stats.size),
    mtimeMs: Option.match(stats.mtime, { onNone: () => null, onSome: (date) => date.getTime() }),
    device: Number(stats.dev),
    inode: Option.match(stats.ino, { onNone: () => null, onSome: String }),
    birthtimeMs: Option.match(stats.birthtime, {
      onNone: () => null,
      onSome: (date) => date.getTime(),
    }),
  };
}

type TranscriptIdentity = Pick<
  AgentSessionImportSource,
  "filePath" | "size" | "mtimeMs" | "device" | "inode" | "birthtimeMs"
>;

const fileIdKey = (inode: TranscriptIdentity["inode"]) => (inode === null ? null : String(inode));

function sameTranscriptIdentity(left: TranscriptIdentity, right: TranscriptIdentity): boolean {
  // Unsafe legacy numbers may already have lost file ID precision.
  if (
    (typeof left.inode === "number" && !Number.isSafeInteger(left.inode)) ||
    (typeof right.inode === "number" && !Number.isSafeInteger(right.inode))
  ) {
    return false;
  }
  return (
    left.filePath === right.filePath &&
    left.size === right.size &&
    left.mtimeMs === right.mtimeMs &&
    left.device === right.device &&
    fileIdKey(left.inode) === fileIdKey(right.inode) &&
    left.birthtimeMs === right.birthtimeMs
  );
}

/** @public Service construction is part of the canonical Effect module API. */
export const make = Effect.gen(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  // Different project imports can arrive concurrently from multiple clients.
  // Only one transcript may hold its selected-history budget at a time.
  const importReadLock = yield* Semaphore.make(1);
  const path = yield* Path.Path;
  const serverConfig = yield* ServerConfig.ServerConfig;
  const serverSettings = yield* ServerSettings.ServerSettingsService;
  const projectStore = yield* ProjectStore.ProjectStoreV2;
  const baseDir = path.resolve(serverConfig.baseDir);
  const worktreesDir = path.resolve(serverConfig.worktreesDir);
  const foldWorktreeCase = (yield* HostProcess.Platform) === "win32";
  const hostEnvironment = yield* HostProcess.Environment;
  const homeDir = yield* HostProcess.HomeDirectory;
  // `/private/tmp` is what macOS reports for sessions started in `/tmp`.
  const excludedProjectRoots = new Set(
    [homeDir, NodeOS.tmpdir(), "/tmp", "/private/tmp"].map((directory) =>
      normalizeProjectPathForComparison(path.resolve(directory)),
    ),
  );
  const excludedProjectAncestors = [
    path.join(homeDir, "Downloads"),
    path.join(homeDir, "Documents", "Codex"),
  ];

  const isExcludedProjectPath = (candidatePath: string) =>
    excludedProjectRoots.has(normalizeProjectPathForComparison(candidatePath)) ||
    excludedProjectAncestors.some((ancestor) =>
      normalizeForWorktreeMatch(candidatePath, foldWorktreeCase).startsWith(
        normalizeForWorktreeMatch(ancestor, foldWorktreeCase),
      ),
    ) ||
    normalizeForWorktreeMatch(candidatePath, foldWorktreeCase).startsWith(
      normalizeForWorktreeMatch(baseDir, foldWorktreeCase),
    ) ||
    isT3ManagedWorktree(candidatePath, worktreesDir, foldWorktreeCase);

  const listDirectory = (directory: string) =>
    fileSystem.readDirectory(directory).pipe(Effect.orElseSucceed((): ReadonlyArray<string> => []));

  const statOption = (target: string) =>
    fileSystem.stat(target).pipe(Effect.asSome, Effect.orElseSucceed(Option.none));

  const readDroidSessionSettings = Effect.fn("AgentSessionScanner.readDroidSessionSettings")(
    function* (transcriptPath: string) {
      const settingsPath = transcriptPath.replace(/\.jsonl$/u, ".settings.json");
      const stats = yield* statOption(settingsPath);
      if (
        Option.isNone(stats) ||
        stats.value.type !== "File" ||
        Number(stats.value.size) > MAX_DROID_SESSION_SETTINGS_BYTES
      ) {
        return Option.none();
      }
      return yield* fileSystem.readFileString(settingsPath).pipe(
        Effect.map(decodeDroidSessionSettings),
        Effect.orElseSucceed(() => Option.none()),
      );
    },
  );

  /** Droid's model and effort for a session, keeping the transcript's model when it names one. */
  const withDroidSessionSettings = Effect.fn("AgentSessionScanner.withDroidSessionSettings")(
    function* (thread: AgentSessionThread, transcriptPath: string) {
      const settings = yield* readDroidSessionSettings(transcriptPath);
      if (Option.isNone(settings)) return thread;
      const { model: settingsModel, reasoningEffort } = settings.value;
      const model = thread.model ?? settingsModel ?? null;
      // The effort belongs to the settings' model; another model may not take it.
      const effortApplies =
        reasoningEffort !== undefined &&
        model !== null &&
        (settingsModel === undefined || settingsModel === model);
      return { ...thread, model, ...(effortApplies ? { reasoningEffort } : {}) };
    },
  );

  const directoryIdentity = Effect.fn("AgentSessionScanner.directoryIdentity")(function* (
    target: string,
    knownStats?: FileSystem.File.Info,
  ) {
    const resolved = path.resolve(target);
    const stats = knownStats === undefined ? yield* statOption(resolved) : Option.some(knownStats);
    if (Option.isSome(stats) && Option.isSome(stats.value.ino) && stats.value.ino.value > 0n) {
      return `inode:${stats.value.dev}:${stats.value.ino.value}`;
    }
    const realPath = yield* fileSystem
      .realPath(resolved)
      .pipe(Effect.orElseSucceed(() => resolved));
    return `path:${normalizeProjectPathForComparison(realPath)}`;
  });

  const readGitIdentity = Effect.fn("AgentSessionScanner.readGitIdentity")(function* (
    directory: string,
  ): Effect.fn.Return<
    | { readonly _tag: "Repository"; readonly git: AgentSessionProjectGit | null }
    | {
        readonly _tag: "Worktree";
        /** The checkout the worktree was added from, when its repository sits in `<checkout>/.git`. */
        readonly mainCheckout: string | null;
        readonly branch: string | null;
      }
    | { readonly _tag: "NotGit" }
  > {
    const gitPath = path.join(directory, ".git");
    const gitStats = yield* statOption(gitPath);
    if (Option.isNone(gitStats)) return { _tag: "NotGit" } as const;
    let gitDir = gitPath;
    if (gitStats.value.type !== "Directory") {
      const pointer = yield* fileSystem
        .readFileString(gitPath)
        .pipe(Effect.orElseSucceed(() => ""));
      const target = /^gitdir:\s*(.+)$/m.exec(pointer)?.[1]?.trim();
      if (target === undefined || target.length === 0) return { _tag: "NotGit" } as const;
      gitDir = path.resolve(directory, target);
    }
    const linkedWorktree =
      gitStats.value.type !== "Directory" && /[\\/]worktrees[\\/][^\\/]+[\\/]?$/.test(gitDir);
    let branch: string | null = null;
    if (linkedWorktree) {
      const head = yield* fileSystem
        .readFileString(path.join(gitDir, "HEAD"))
        .pipe(Effect.orElseSucceed(() => ""));
      branch = /^ref:\s*refs\/heads\/(.+)$/m.exec(head)?.[1]?.trim() || null;
      const commonDir = yield* fileSystem
        .readFileString(path.join(gitDir, "commondir"))
        .pipe(Effect.orElseSucceed(() => ""));
      if (commonDir.trim().length === 0) {
        return { _tag: "Worktree", mainCheckout: null, branch } as const;
      }
      gitDir = path.resolve(gitDir, commonDir.trim());
    }
    const configText = yield* fileSystem
      .readFileString(path.join(gitDir, "config"))
      .pipe(Effect.orElseSucceed(() => ""));
    // Bare repositories have no main checkout, so offer their linked worktrees independently.
    if (linkedWorktree && !isBareGitConfig(configText)) {
      const mainCheckout = path.basename(gitDir) === ".git" ? path.dirname(gitDir) : null;
      return { _tag: "Worktree", mainCheckout, branch } as const;
    }
    const originUrl = parseOriginUrlFromGitConfig(configText);
    return {
      _tag: "Repository",
      git: {
        remoteKey: originUrl === null ? null : normalizeGitRemoteUrl(originUrl),
        repository: parseGitHubRepositoryNameWithOwnerFromRemoteUrl(originUrl),
      },
    } as const;
  });

  const readCheckout = Effect.fnUntraced(function* (directory: string) {
    if (isExcludedProjectPath(directory)) return undefined;
    const stats = yield* statOption(directory);
    if (Option.isNone(stats) || stats.value.type !== "Directory") return undefined;
    const realPath = yield* fileSystem
      .realPath(directory)
      .pipe(Effect.orElseSucceed(() => directory));
    // A symlink can point into the worktrees directory even when its own
    // spelling doesn't; check again with links resolved.
    if (isExcludedProjectPath(realPath)) return undefined;
    return { stats: stats.value, gitIdentity: yield* readGitIdentity(directory) };
  });

  /** The project that sessions run in `directory` belong to, if any. */
  const projectPlacement = Effect.fn("AgentSessionScanner.projectPlacement")(function* (
    directory: string,
  ): Effect.fn.Return<ProjectPlacement | undefined> {
    const checkout = yield* readCheckout(directory);
    if (checkout === undefined) return undefined;
    const { gitIdentity } = checkout;
    if (gitIdentity._tag !== "Worktree") {
      return {
        projectPath: directory,
        key: yield* directoryIdentity(directory, checkout.stats),
        git: gitIdentity._tag === "Repository" ? gitIdentity.git : null,
      };
    }
    // A linked worktree's sessions belong to the checkout it was added from, and their threads
    // keep working in the worktree.
    if (gitIdentity.mainCheckout === null) return undefined;
    const main = yield* readCheckout(gitIdentity.mainCheckout);
    if (main === undefined || main.gitIdentity._tag === "Worktree") return undefined;
    return {
      projectPath: gitIdentity.mainCheckout,
      key: yield* directoryIdentity(gitIdentity.mainCheckout, main.stats),
      git: main.gitIdentity._tag === "Repository" ? main.gitIdentity.git : null,
      worktree: { path: directory, branch: gitIdentity.branch },
    };
  });

  const readCwd = Effect.fn("AgentSessionScanner.readCwd")(function* (
    source: AgentSessionSource,
    transcript: TranscriptCandidate,
    budget: MetadataReadBudget,
  ) {
    if (transcript.size === 0) return null;
    if (
      budget.bytesRemaining === 0 ||
      budget.operationsRemaining < 2 ||
      budget.recordsRemaining === 0
    ) {
      budget.truncated = true;
      return null;
    }
    budget.operationsRemaining -= 1;
    return yield* Effect.scoped(
      fileSystem.open(transcript.filePath, { flag: "r" }).pipe(
        Effect.flatMap((file) =>
          Effect.gen(function* () {
            const decoder = new TextDecoder();
            let remaining = "";
            let bytesRead = 0;
            let recordsRead = 0;
            const maxBytes = Math.min(MAX_TRANSCRIPT_SCAN_BYTES, transcript.size);
            const reserveRecord = () => {
              if (
                recordsRead === MAX_METADATA_RECORDS_PER_TRANSCRIPT ||
                budget.recordsRemaining === 0
              ) {
                budget.truncated = true;
                return false;
              }
              recordsRead += 1;
              budget.recordsRemaining -= 1;
              return true;
            };
            const readLastRecord = () => {
              const record = remaining + decoder.decode();
              return record.length === 0 || !reserveRecord()
                ? null
                : extractCwd(record.trim(), source);
            };

            while (bytesRead < maxBytes) {
              if (budget.bytesRemaining === 0 || budget.operationsRemaining === 0) {
                budget.truncated = true;
                return null;
              }
              const readSize = Math.min(
                METADATA_READ_BYTES,
                maxBytes - bytesRead,
                budget.bytesRemaining,
              );
              budget.operationsRemaining -= 1;
              budget.bytesRemaining -= readSize;
              const next = yield* file.readAlloc(readSize);
              if (Option.isNone(next)) {
                return readLastRecord();
              }

              bytesRead += next.value.byteLength;
              remaining += decoder.decode(next.value, { stream: true });
              const lines = remaining.split("\n");
              remaining = lines.pop() ?? "";

              for (const line of lines) {
                if (!reserveRecord()) return null;
                const cwd = extractCwd(line.trim(), source);
                if (cwd !== null) return cwd;
              }
            }

            if (bytesRead < transcript.size) {
              budget.truncated = true;
              return null;
            }
            return readLastRecord();
          }),
        ),
      ),
    ).pipe(Effect.orElseSucceed(() => null));
  });

  const readTranscript = Effect.fn("AgentSessionScanner.readTranscript")(function* (
    filePath: string,
    expected: ReturnType<typeof transcriptIdentity>,
    recordLimit: number,
    source: AgentSessionSource,
  ) {
    const skip = (reason: AgentSessionSkipReason): AgentSessionSkipReason => reason;
    // Past the records left in this import; with none used yet, the transcript alone is too big.
    const overRecordLimit = () =>
      skip(recordLimit < MAX_IMPORT_RECORDS ? "import-limit" : "too-large");
    if (expected.size > MAX_IMPORTED_TRANSCRIPT_BYTES) return skip("too-large");

    return yield* Effect.scoped(
      fileSystem.open(filePath, { flag: "r" }).pipe(
        Effect.flatMap((file) =>
          Effect.gen(function* () {
            if (!sameTranscriptIdentity(expected, transcriptIdentity(filePath, yield* file.stat))) {
              return skip("changed");
            }
            const droid = source === "droid";
            const historyBudget = droid ? MAX_DROID_IMPORT_HISTORY_BYTES : MAX_IMPORT_HISTORY_BYTES;
            const decodeValue = droid ? decodeDroidTranscriptValue : decodeTranscriptValue;
            // Slots of dropped records become undefined; `firstPrompt` is never dropped.
            const retained: Array<
              { readonly record: DecodedTranscriptRecord; readonly bytes: number } | undefined
            > = [];
            let firstPrompt: number | undefined;
            let nextToDrop = 0;
            let omittedRecords = 0;
            let cwd = Option.none<string>();
            let claudeFields = emptyClaudeFields;
            let historyBytes = 0;
            let recordBytes = 0;
            let recordCount = 0;
            let bytesRead = 0;
            const dropOldest = () => {
              for (; nextToDrop < retained.length; nextToDrop++) {
                const entry = retained[nextToDrop];
                if (entry === undefined || nextToDrop === firstPrompt) continue;
                retained[nextToDrop] = undefined;
                historyBytes -= entry.bytes;
                omittedRecords += 1;
                nextToDrop += 1;
                return true;
              }
              return false;
            };
            const reserve = (bytes: number) => {
              recordBytes += bytes;
              while (historyBytes + recordBytes > historyBudget) {
                if (!droid || !dropOldest()) {
                  throw new TranscriptJsonLimitError(
                    `Transcript selected history exceeds the ${historyBudget / 1024 / 1024} MiB memory budget`,
                  );
                }
              }
            };
            const newReader = () =>
              droid
                ? createTranscriptJsonReader(reserve, selectDroidTranscriptPath, {
                    stringLimit: droidStringLimit,
                  })
                : createTranscriptJsonReader(reserve, selectTranscriptPath);
            let reader = newReader();
            let decoder = new TextDecoder();
            let recordStarted = false;

            const finishRecord = () => {
              reader.write(decoder.decode());
              recordCount += 1;
              if (recordCount > recordLimit) return false;
              const decoded: Option.Option<DecodedTranscriptRecord> = decodeValue(reader.finish());
              if (Option.isSome(decoded)) {
                if (Option.isNone(cwd)) cwd = Option.fromNullOr(extractDecodedCwd(decoded.value));
                if (source === "claudeAgent") {
                  claudeFields = foldClaudeFields(claudeFields, decoded.value);
                } else if (droid) {
                  claudeFields = foldDroidTranscriptFields(claudeFields, decoded.value);
                }
                if (shouldRetainDecodedRecord(source, decoded.value)) {
                  if (
                    droid &&
                    firstPrompt === undefined &&
                    droidTranscriptMessage(decoded.value)?.role === "user"
                  ) {
                    firstPrompt = retained.length;
                  }
                  retained.push({ record: decoded.value, bytes: recordBytes });
                  historyBytes += recordBytes;
                }
              }
              recordBytes = 0;
              reader = newReader();
              decoder = new TextDecoder();
              recordStarted = false;
              return true;
            };

            while (bytesRead < expected.size) {
              const next = yield* file.readAlloc(
                Math.min(TRANSCRIPT_PREFIX_BYTES, expected.size - bytesRead),
              );
              if (Option.isNone(next)) {
                return skip("changed");
              }

              bytesRead += next.value.byteLength;
              const withinBudget = yield* Effect.try(() => {
                let start = 0;
                while (start < next.value.byteLength) {
                  const newline = next.value.indexOf(10, start);
                  const end = newline === -1 ? next.value.byteLength : newline;
                  recordStarted = true;
                  reader.write(decoder.decode(next.value.subarray(start, end), { stream: true }));
                  if (newline === -1) break;
                  if (!finishRecord()) return false;
                  start = newline + 1;
                }
                return true;
              });
              if (!withinBudget) return overRecordLimit();
            }

            if (recordStarted && !(yield* Effect.try(finishRecord))) return overRecordLimit();
            const records = retained.flatMap((entry) =>
              entry === undefined ? [] : [entry.record],
            );
            return sameTranscriptIdentity(expected, transcriptIdentity(filePath, yield* file.stat))
              ? { records, recordCount, cwd, claudeFields, omittedRecords }
              : skip("changed");
          }),
        ),
      ),
    ).pipe(
      Effect.catch((cause) =>
        Effect.logWarning("Could not read imported transcript", { filePath, cause }).pipe(
          Effect.as(
            skip(
              Cause.isUnknownError(cause) && cause.cause instanceof TranscriptJsonLimitError
                ? "too-large"
                : "unreadable",
            ),
          ),
        ),
      ),
    );
  });

  const resolveClaudeConfigDir = (homePath: string, environmentHome?: string): string => {
    const configured = homePath.trim();
    if (configured.length > 0) {
      return path.resolve(expandHomePath(configured, homeDir));
    }
    const fromEnvironment = environmentHome?.trim() ?? "";
    if (fromEnvironment.length > 0) {
      return path.resolve(expandHomePath(fromEnvironment, homeDir));
    }
    return path.join(homeDir, ".claude");
  };

  // Droid lays out `<home>/sessions` the same way: a folder per cwd of `<session id>.jsonl` files.
  const discoverClaudeTranscripts = Effect.fn("AgentSessionScanner.discoverClaudeTranscripts")(
    function* (
      homePath: string,
      providerInstanceId: ProviderInstanceId,
      operationBudget: number,
      projectsDirectory = "projects",
    ) {
      const projectsDir = path.join(homePath, projectsDirectory);
      let operationsRemaining = operationBudget;
      let truncated = false;
      const readDirectory = (directory: string) => {
        if (operationsRemaining <= 0) {
          truncated = true;
          return Effect.succeed<ReadonlyArray<string>>([]);
        }
        operationsRemaining -= 1;
        return listDirectory(directory);
      };
      const projectDirectories = yield* readDirectory(projectsDir);
      const transcripts: Array<TranscriptCandidate> = [];

      for (const projectDirectory of projectDirectories) {
        if (operationsRemaining <= 0) {
          truncated = true;
          break;
        }
        const directory = path.join(projectsDir, projectDirectory);
        const directoryTranscripts = (yield* readDirectory(directory))
          .filter((entry) => entry.endsWith(".jsonl"))
          .map((entry) => path.join(directory, entry));

        for (const filePath of directoryTranscripts) {
          if (operationsRemaining <= 0) {
            truncated = true;
            break;
          }
          operationsRemaining -= 1;
          const stats = yield* statOption(filePath);
          if (
            Option.isNone(stats) ||
            stats.value.type !== "File" ||
            Option.isNone(stats.value.mtime)
          ) {
            continue;
          }
          transcripts.push({
            filePath,
            mtimeMs: stats.value.mtime.value.getTime(),
            providerInstanceId,
            size: Number(stats.value.size),
          });
        }
      }
      return { transcripts, truncated };
    },
  );

  const discoverCodexTranscripts = Effect.fn("AgentSessionScanner.discoverCodexTranscripts")(
    function* (homePath: string, providerInstanceId: ProviderInstanceId, operationBudget: number) {
      const sessionsDir = path.join(homePath, "sessions");

      const transcripts: Array<TranscriptCandidate> = [];
      let operationsRemaining = operationBudget;
      let truncated = false;
      const readDirectory = (directory: string) => {
        if (operationsRemaining <= 0) {
          truncated = true;
          return Effect.succeed<ReadonlyArray<string>>([]);
        }
        operationsRemaining -= 1;
        return listDirectory(directory);
      };
      for (const year of (yield* readDirectory(sessionsDir)).toSorted().toReversed()) {
        if (operationsRemaining <= 0) {
          truncated = true;
          break;
        }
        for (const month of (yield* readDirectory(path.join(sessionsDir, year)))
          .toSorted()
          .toReversed()) {
          if (operationsRemaining <= 0) {
            truncated = true;
            break;
          }
          for (const day of (yield* readDirectory(path.join(sessionsDir, year, month)))
            .toSorted()
            .toReversed()) {
            if (operationsRemaining <= 0) {
              truncated = true;
              break;
            }
            const directory = path.join(sessionsDir, year, month, day);
            for (const entry of (yield* readDirectory(directory)).toSorted().toReversed()) {
              if (!entry.startsWith("rollout-") || !entry.endsWith(".jsonl")) continue;
              if (operationsRemaining <= 0) {
                truncated = true;
                break;
              }
              const filePath = path.join(directory, entry);
              operationsRemaining -= 1;
              const stats = yield* statOption(filePath);
              if (
                Option.isSome(stats) &&
                stats.value.type === "File" &&
                Option.isSome(stats.value.mtime)
              ) {
                transcripts.push({
                  filePath,
                  mtimeMs: stats.value.mtime.value.getTime(),
                  providerInstanceId,
                  size: Number(stats.value.size),
                });
              }
            }
          }
        }
      }
      return { transcripts, truncated };
    },
  );

  const groupTranscriptsByCwd = Effect.fn("AgentSessionScanner.groupTranscriptsByCwd")(function* (
    source: AgentSessionSource,
    transcripts: ReadonlyArray<TranscriptCandidate>,
    budget: MetadataReadBudget,
  ) {
    const byOwnerAndCwd = new Map<
      string,
      {
        cwd: string;
        providerInstanceId: ProviderInstanceId;
        lastActiveAtMs: number;
        transcripts: Array<RawCandidate["transcripts"][number]>;
      }
    >();

    for (const transcript of transcripts) {
      const start = yield* readCwd(source, transcript, budget);
      if (start === null || start === false) continue;
      const { cwd, parentSessionId } = start;
      const entry = {
        filePath: transcript.filePath,
        mtimeMs: transcript.mtimeMs,
        ...(parentSessionId === undefined ? {} : { parentSessionId }),
      };
      const key = `${transcript.providerInstanceId}\0${cwd}`;
      const existing = byOwnerAndCwd.get(key);
      if (existing) {
        existing.lastActiveAtMs = Math.max(existing.lastActiveAtMs, transcript.mtimeMs);
        existing.transcripts.push(entry);
      } else {
        byOwnerAndCwd.set(key, {
          cwd,
          providerInstanceId: transcript.providerInstanceId,
          lastActiveAtMs: transcript.mtimeMs,
          transcripts: [entry],
        });
      }
    }

    return Array.from(
      byOwnerAndCwd.values(),
      (group): RawCandidate => ({
        cwd: group.cwd,
        source,
        providerInstanceId: group.providerInstanceId,
        threadCount: group.transcripts.length,
        lastActiveAtMs: group.lastActiveAtMs,
        transcripts: group.transcripts,
      }),
    );
  });

  const collectCandidates = Effect.fn("AgentSessionScanner.collectCandidates")(function* () {
    const settings = yield* serverSettings.getSettings.pipe(
      Effect.mapError((cause) => new AgentSessionScanError({ operation: "read-settings", cause })),
    );

    const raw: Array<RawCandidate> = [];
    let truncated = false;

    for (const source of ["claudeAgent", "codex", "droid"] as const) {
      const instances: Array<{
        readonly instanceId: ProviderInstanceId;
        readonly config: ProviderInstanceConfig;
      }> = Object.entries(settings.providerInstances)
        .filter(
          ([, instance]) => instance.driver === source && resolveProviderInstanceEnabled(instance),
        )
        .map(([instanceId, config]) => ({
          instanceId: ProviderInstanceId.make(instanceId),
          config,
        }));
      if (!Object.hasOwn(settings.providerInstances, source)) {
        const defaultInstance = {
          instanceId: ProviderInstanceId.make(source),
          config: { driver: ProviderDriverKind.make(source) },
        };
        if (resolveProviderInstanceEnabled(defaultInstance.config)) {
          instances.push(defaultInstance);
        }
      }

      instances.sort((left, right) => {
        const leftDefault = left.instanceId === source ? 0 : 1;
        const rightDefault = right.instanceId === source ? 0 : 1;
        return leftDefault - rightDefault;
      });
      const homes: Array<{ homePath: string; providerInstanceId: ProviderInstanceId }> = [];
      const seenHomes = new Set<string>();
      for (const { instanceId, config: instance } of instances) {
        const homeVariable = source === "claudeAgent" ? "CLAUDE_CONFIG_DIR" : "CODEX_HOME";
        const environmentHome =
          instance.environment?.findLast((variable) => variable.name === homeVariable)?.value ??
          hostEnvironment[homeVariable];

        let homePath: string;
        if (source === "claudeAgent") {
          const config = decodeClaudeSettings(instance.config ?? {});
          if (Option.isNone(config)) continue;
          homePath = resolveClaudeConfigDir(config.value.homePath, environmentHome);
        } else if (source === "droid") {
          homePath = path.join(homeDir, DROID_HOME_DIRECTORY);
        } else {
          const config = decodeCodexSettings(instance.config ?? {});
          if (Option.isNone(config)) continue;
          const codexSettings =
            config.value.homePath.trim().length === 0 &&
            config.value.shadowHomePath.trim().length === 0 &&
            environmentHome?.trim()
              ? { ...config.value, homePath: environmentHome }
              : config.value;
          const layout = yield* resolveCodexHomeLayout(codexSettings).pipe(
            Effect.provideService(Path.Path, path),
          );
          homePath = layout.sharedHomePath;
        }

        const homeKey = `${source}\0${yield* directoryIdentity(homePath)}`;
        if (seenHomes.has(homeKey)) continue;
        seenHomes.add(homeKey);
        homes.push({ homePath, providerInstanceId: instanceId });
      }

      const transcriptCandidates: Array<TranscriptCandidate> = [];
      const baseOperationBudget = Math.floor(
        MAX_DISCOVERY_OPERATIONS_PER_SOURCE / Math.max(1, homes.length),
      );
      const extraOperationBudgets = MAX_DISCOVERY_OPERATIONS_PER_SOURCE % Math.max(1, homes.length);
      for (const [index, home] of homes.entries()) {
        const operationBudget = baseOperationBudget + (index < extraOperationBudgets ? 1 : 0);
        if (operationBudget === 0) {
          truncated = true;
          continue;
        }
        const discovered = yield* source === "codex"
          ? discoverCodexTranscripts(home.homePath, home.providerInstanceId, operationBudget)
          : discoverClaudeTranscripts(
              home.homePath,
              home.providerInstanceId,
              operationBudget,
              source === "droid" ? DROID_SESSIONS_DIRECTORY : undefined,
            );
        truncated ||= discovered.truncated;
        transcriptCandidates.push(...discovered.transcripts);
      }

      transcriptCandidates.sort(
        (left, right) =>
          right.mtimeMs - left.mtimeMs || left.filePath.localeCompare(right.filePath),
      );
      if (transcriptCandidates.length > MAX_TRANSCRIPTS_PER_SOURCE) {
        truncated = true;
      }
      const selectedTranscripts = selectMetadataTranscripts(transcriptCandidates);
      const metadataBudget: MetadataReadBudget = {
        bytesRemaining: MAX_METADATA_BYTES_PER_SOURCE,
        operationsRemaining: MAX_METADATA_OPERATIONS_PER_SOURCE,
        recordsRemaining: MAX_METADATA_RECORDS_PER_SOURCE,
        truncated: false,
      };
      raw.push(...(yield* groupTranscriptsByCwd(source, selectedTranscripts, metadataBudget)));
      truncated ||= metadataBudget.truncated;
    }

    return { candidates: raw, truncated };
  });

  let cachedCandidates: ReadonlyArray<RawCandidate> | null = null;

  const scan: AgentSessionScanner["Service"]["scan"] = Effect.gen(function* () {
    const { candidates: raw, truncated } = yield* collectCandidates();
    cachedCandidates = raw;

    const merged = new Map<
      string,
      {
        path: string;
        sources: Array<AgentSessionSource>;
        threadCount: number;
        lastActiveAtMs: number | null;
        git: AgentSessionProjectGit | null;
      }
    >();
    const placements = new Map<string, ProjectPlacement | undefined>();

    for (const candidate of raw) {
      const expanded = expandHomePath(candidate.cwd.trim(), homeDir);
      if (!path.isAbsolute(expanded)) continue;
      const resolved = path.resolve(expanded);
      if (!placements.has(resolved)) placements.set(resolved, yield* projectPlacement(resolved));
      const placement = placements.get(resolved);
      if (placement === undefined) continue;
      const key = placement.key;

      const existing = merged.get(key);
      if (!existing) {
        merged.set(key, {
          path: placement.projectPath,
          sources: [candidate.source],
          threadCount: candidate.threadCount,
          lastActiveAtMs: candidate.lastActiveAtMs,
          git: placement.git,
        });
        continue;
      }
      if (!existing.sources.includes(candidate.source)) {
        existing.sources.push(candidate.source);
      }
      existing.threadCount += candidate.threadCount;
      existing.lastActiveAtMs =
        existing.lastActiveAtMs === null || candidate.lastActiveAtMs === null
          ? (existing.lastActiveAtMs ?? candidate.lastActiveAtMs)
          : Math.max(existing.lastActiveAtMs, candidate.lastActiveAtMs);
    }

    // Resolve persisted roots too. A project and a transcript can name
    // different symlinks to the same directory.
    const importedProjects = yield* projectStore
      .listShells()
      .pipe(
        Effect.mapError(
          (cause) => new AgentSessionScanError({ operation: "read-projects", cause }),
        ),
      );
    const importedProjectsByRoot = new Map<string, (typeof importedProjects)[number]>();
    for (const project of importedProjects) {
      const projectRoot = path.resolve(expandHomePath(project.workspaceRoot, homeDir));
      importedProjectsByRoot.set(normalizeProjectPathForComparison(projectRoot), project);
      importedProjectsByRoot.set(yield* directoryIdentity(projectRoot), project);
    }

    const candidates: Array<AgentSessionProjectCandidate> = [];
    for (const [key, entry] of merged.entries()) {
      const importedProject =
        importedProjectsByRoot.get(normalizeProjectPathForComparison(entry.path)) ??
        importedProjectsByRoot.get(key);
      const candidatePath = importedProject?.workspaceRoot ?? entry.path;
      candidates.push({
        path: candidatePath,
        title: path.basename(candidatePath) || candidatePath,
        ...(importedProject === undefined ? {} : { projectId: importedProject.id }),
        sources: entry.sources,
        threadCount: entry.threadCount,
        lastActiveAt:
          entry.lastActiveAtMs === null
            ? null
            : DateTime.formatIso(DateTime.makeUnsafe(entry.lastActiveAtMs)),
        alreadyImported: importedProject !== undefined,
        git: entry.git,
      });
    }

    candidates.sort((left, right) => {
      if (left.lastActiveAt === right.lastActiveAt) return left.path.localeCompare(right.path);
      if (left.lastActiveAt === null) return 1;
      if (right.lastActiveAt === null) return -1;
      return right.lastActiveAt.localeCompare(left.lastActiveAt);
    });

    return {
      candidates,
      scannedAt: DateTime.formatIso(yield* DateTime.now),
      ...(truncated ? { truncated: true } : {}),
    };
  });

  const prepareRecentThreads = Effect.fn("AgentSessionScanner.prepareRecentThreads")(function* (
    workspaceRoot: string,
    completedSources: ReadonlyArray<AgentSessionImportSource>,
  ) {
    const root = path.resolve(expandHomePath(workspaceRoot, homeDir));
    const realRoot = yield* fileSystem.realPath(root).pipe(Effect.orElseSucceed(() => root));
    if (isExcludedProjectPath(root) || isExcludedProjectPath(realRoot)) return Stream.empty;
    const rootIdentity = yield* directoryIdentity(root);
    const nowMs = DateTime.toEpochMillis(yield* DateTime.now);
    const cutoffMs = nowMs - RECENT_THREAD_WINDOW_MS;

    const candidates = cachedCandidates ?? (yield* collectCandidates()).candidates;
    cachedCandidates = candidates;

    const worktreePlacements = new Map<
      string,
      { readonly worktree: AgentSessionWorktree } | null
    >();
    /** How sessions run in `directory` belong to this project; undefined when they do not. */
    const placeInProject = Effect.fnUntraced(function* (directory: string) {
      if ((yield* directoryIdentity(directory)) === rootIdentity) return {};
      let placement = worktreePlacements.get(directory);
      if (placement === undefined) {
        const found = yield* projectPlacement(directory);
        placement =
          found?.key === rootIdentity && found.worktree !== undefined
            ? { worktree: found.worktree }
            : null;
        worktreePlacements.set(directory, placement);
      }
      return placement ?? undefined;
    });

    const eligibleTranscripts: Array<{
      readonly candidate: RawCandidate;
      readonly transcript: RawCandidate["transcripts"][number] & { readonly mtimeMs: number };
    }> = [];
    for (const candidate of candidates) {
      const expanded = expandHomePath(candidate.cwd.trim(), homeDir);
      if (!path.isAbsolute(expanded)) continue;
      if ((yield* placeInProject(path.resolve(expanded))) === undefined) continue;

      for (const transcript of candidate.transcripts) {
        if (
          transcript.mtimeMs === null ||
          transcript.mtimeMs < cutoffMs ||
          transcript.mtimeMs > nowMs
        ) {
          continue;
        }
        eligibleTranscripts.push({
          candidate,
          transcript: { ...transcript, mtimeMs: transcript.mtimeMs },
        });
      }
    }

    eligibleTranscripts.sort((left, right) => {
      if (left.transcript.mtimeMs !== right.transcript.mtimeMs) {
        return right.transcript.mtimeMs - left.transcript.mtimeMs;
      }
      return left.transcript.filePath.localeCompare(right.transcript.filePath);
    });

    // Droid names each transcript after its session.
    const childSessions = new Map<string, Array<string>>();
    for (const candidate of candidates) {
      for (const { filePath, parentSessionId } of candidate.transcripts) {
        if (parentSessionId === undefined) continue;
        const key = `${candidate.providerInstanceId}\0${parentSessionId}`;
        childSessions.set(key, [
          ...(childSessions.get(key) ?? []),
          path.basename(filePath, ".jsonl"),
        ]);
      }
    }
    const continuedIn = (providerInstanceId: ProviderInstanceId, sessionId: string) => {
      const found = new Set<string>();
      const pending = [sessionId];
      for (let next = pending.pop(); next !== undefined; next = pending.pop()) {
        for (const child of childSessions.get(`${providerInstanceId}\0${next}`) ?? []) {
          if (child === sessionId || found.has(child)) continue;
          found.add(child);
          pending.push(child);
        }
      }
      return Array.from(found);
    };

    const completedByFile = Map.groupBy(
      completedSources,
      (source) => `${source.providerInstanceId}\0${source.filePath}`,
    );
    const importedSessions = new Set<string>();
    let bytesRemaining = MAX_IMPORT_BYTES;
    let transcriptsRemaining = MAX_IMPORT_TRANSCRIPTS;
    let recordsRemaining = MAX_IMPORT_RECORDS;
    const skipped = (reason: AgentSessionSkipReason) =>
      Option.some<AgentSessionRecentThread>({ _tag: "Skipped", reason });
    return Stream.fromIteratorSucceed(eligibleTranscripts.values(), 1).pipe(
      Stream.mapEffect(({ candidate, transcript }) =>
        Effect.gen(function* () {
          const completed = completedByFile.get(
            `${candidate.providerInstanceId}\0${transcript.filePath}`,
          );
          if (
            completed === undefined &&
            (transcriptsRemaining === 0 || bytesRemaining === 0 || recordsRemaining === 0)
          ) {
            return skipped("import-limit");
          }
          const stats = yield* statOption(transcript.filePath);
          if (Option.isNone(stats) || stats.value.type !== "File") {
            return skipped("unreadable");
          }
          const identity = transcriptIdentity(transcript.filePath, stats.value);
          const completedSource = completed?.find(
            (source) =>
              source.provider === candidate.source && sameTranscriptIdentity(source, identity),
          );
          if (completedSource !== undefined && hasCurrentHistory(completedSource)) {
            const sessionKey = `${completedSource.providerInstanceId}\0${completedSource.providerSessionId}`;
            if (importedSessions.has(sessionKey)) return Option.none<AgentSessionRecentThread>();
            importedSessions.add(sessionKey);
            return Option.some<AgentSessionRecentThread>({
              _tag: "AlreadyImported",
              source: completedSource,
            });
          }
          if (identity.size > MAX_IMPORTED_TRANSCRIPT_BYTES) return skipped("too-large");
          if (
            transcriptsRemaining === 0 ||
            recordsRemaining === 0 ||
            identity.size > bytesRemaining
          ) {
            return skipped("import-limit");
          }
          transcriptsRemaining -= 1;
          bytesRemaining -= identity.size;
          const snapshot = yield* readTranscript(
            transcript.filePath,
            identity,
            recordsRemaining,
            candidate.source,
          );
          if (typeof snapshot === "string") return skipped(snapshot);
          recordsRemaining -= snapshot.recordCount;

          if (Option.isNone(snapshot.cwd)) {
            return skipped(snapshot.records.length === 0 ? "empty" : "unreadable");
          }
          const expandedCwd = expandHomePath(snapshot.cwd.value.trim(), homeDir);
          const placement = path.isAbsolute(expandedCwd)
            ? yield* placeInProject(path.resolve(expandedCwd))
            : undefined;
          if (placement === undefined) return skipped("other-folder");

          const parsedThread = parseAgentSessionRecords(
            {
              source: candidate.source,
              providerInstanceId: candidate.providerInstanceId,
              fallbackSessionId: path.basename(transcript.filePath, ".jsonl"),
              lastActiveAtMs: transcript.mtimeMs,
            },
            snapshot.records,
            snapshot.claudeFields,
            snapshot.omittedRecords,
          );
          if (parsedThread === null) return skipped("empty");

          const historyVersion = historyVersionFor(parsedThread.source);
          const source: AgentSessionImportSource = {
            ...identity,
            provider: parsedThread.source,
            providerInstanceId: parsedThread.providerInstanceId,
            providerSessionId: parsedThread.providerSessionId,
            ...(historyVersion > 1 ? { historyVersion } : {}),
          };
          const sessionKey = `${parsedThread.providerInstanceId}\0${parsedThread.providerSessionId}`;
          if (importedSessions.has(sessionKey)) {
            return Option.some<AgentSessionRecentThread>({ _tag: "Duplicate", source });
          }
          importedSessions.add(sessionKey);
          const previous = completed?.filter((entry) => entry.provider === candidate.source) ?? [];
          const continuations = continuedIn(
            parsedThread.providerInstanceId,
            parsedThread.providerSessionId,
          );
          const thread =
            candidate.source === "droid"
              ? yield* withDroidSessionSettings(parsedThread, transcript.filePath)
              : parsedThread;
          return Option.some<AgentSessionRecentThread>({
            _tag: "Importable",
            thread: { ...thread, ...placement },
            source,
            refreshHistory: previous.length > 0 && !previous.some(hasCurrentHistory),
            ...(continuations.length > 0 ? { continuedIn: continuations } : {}),
          });
        }).pipe(importReadLock.withPermits(1)),
      ),
      Stream.map(Option.toArray),
      Stream.flattenIterable,
    );
  });

  const recentThreads: AgentSessionScanner["Service"]["recentThreads"] = (
    workspaceRoot,
    completedSources = [],
  ) => Stream.unwrap(prepareRecentThreads(workspaceRoot, completedSources));

  return AgentSessionScanner.of({ scan, recentThreads });
});

export const layer = Layer.effect(AgentSessionScanner, make);
