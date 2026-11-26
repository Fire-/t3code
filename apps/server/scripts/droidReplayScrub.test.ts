import { assert, describe, it } from "@effect/vitest";
import type { ProviderReplayEntry } from "@t3tools/contracts";

import {
  collectHostIds,
  collectMachineIds,
  scrubDroidEntries,
  type DroidScrubEnvironment,
} from "./droidReplayScrub.ts";

const environment: DroidScrubEnvironment = {
  modelId: "claude-haiku-5-5",
  workspace: "/tmp/droid-v2-rec-abc123/ws",
  recordingRoot: "/tmp/droid-v2-rec-abc123",
  systemTmpDir: "/var/folders/zz/yy/T",
  home: "/Users/example",
  userName: "exampleuser",
  hostNames: ["example-host.local", "example-host"],
};

const SESSION_ID = "11111111-2222-4333-8444-555555555555";
const HOST_ID = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";
const MACHINE_ID = "machine-7c1d4e9a";
const SIGNATURE = "CAQS1gYKEAgSGAI4AUIIdGhpbmtpbmcSDFPS1oJ4opaque";
const PRIVATE_CONTEXT = "PRIVATE-GLOBAL-INSTRUCTIONS exampleuser example-host.local /Users/example";

const inbound = (frame: unknown): ProviderReplayEntry => ({ type: "emit_inbound", frame });
const outbound = (frame: unknown): ProviderReplayEntry => ({ type: "expect_outbound", frame });

const encoded = (entries: ReadonlyArray<ProviderReplayEntry>) => JSON.stringify(entries);

const loadSessionResponse = inbound({
  type: "response",
  id: "2",
  result: {
    hostId: HOST_ID,
    session: {
      messages: [
        {
          id: "context-0f0f0f0f-1111-4222-8333-444444444444",
          role: "user",
          visibility: "llm_only",
          content: [{ type: "text", text: PRIVATE_CONTEXT }],
        },
        {
          id: "message:thread:1",
          role: "user",
          content: [{ type: "text", text: "Respond with exactly: first turn complete" }],
        },
        {
          id: "__internal__system-reminder__9d9d9d9d-aaaa-4bbb-8ccc-dddddddddddd",
          role: "user",
          visibility: "llm_only",
          content: PRIVATE_CONTEXT,
        },
        {
          id: "assistant-1",
          role: "assistant",
          content: [{ type: "text", text: "first turn complete" }],
        },
      ],
    },
  },
});

const scrub = (entries: ReadonlyArray<ProviderReplayEntry>) =>
  scrubDroidEntries(entries, environment);

describe("Droid replay scrub", () => {
  it("blanks context and system-reminder messages in a load_session response", () => {
    const [entry] = scrub([loadSessionResponse]);
    const text = encoded([entry!]);
    assert.notInclude(text, "PRIVATE-GLOBAL-INSTRUCTIONS");
    assert.notInclude(text, "exampleuser");
    assert.notInclude(text, "example-host");

    const frame = entry?.type === "emit_inbound" ? entry.frame : undefined;
    const messages = (
      frame as { result: { session: { messages: Array<Record<string, unknown>> } } }
    ).result.session.messages;
    assert.lengthOf(messages, 4);
    assert.match(String(messages[0]?.id), /^context-/u);
    assert.deepEqual(messages[0]?.content, [{ type: "text", text: "<hidden-context>" }]);
    assert.equal(messages[0]?.visibility, "llm_only");
    assert.match(String(messages[2]?.id), /^__internal__system-reminder__/u);
    assert.equal(messages[2]?.content, "<hidden-context>");
  });

  it("keeps the conversation messages of a load_session response intact", () => {
    const [entry] = scrub([loadSessionResponse]);
    const frame = entry?.type === "emit_inbound" ? entry.frame : undefined;
    const messages = (frame as { result: { session: { messages: Array<unknown> } } }).result.session
      .messages;
    assert.deepEqual(messages[1], {
      id: "message:thread:1",
      role: "user",
      content: [{ type: "text", text: "Respond with exactly: first turn complete" }],
    });
    assert.deepEqual(messages[3], {
      id: "assistant-1",
      role: "assistant",
      content: [{ type: "text", text: "first turn complete" }],
    });
  });

  it("blanks a system-reminder message that arrives on the live stream", () => {
    const [entry] = scrub([
      inbound({
        type: "notification",
        method: "droid.session_notification",
        params: {
          notification: {
            type: "create_message",
            message: {
              id: "__internal__system-reminder__abc",
              role: "user",
              content: [{ type: "text", text: PRIVATE_CONTEXT }],
            },
          },
        },
      }),
    ]);
    assert.notInclude(encoded([entry!]), "PRIVATE-GLOBAL-INSTRUCTIONS");
  });

  it("replaces the host id and keeps one placeholder for the session id on both sides", () => {
    const scrubbed = scrub([
      outbound({
        type: "request",
        id: "2",
        method: "droid.load_session",
        params: { sessionId: SESSION_ID },
      }),
      inbound({ type: "response", id: "2", result: { hostId: HOST_ID, sessionId: SESSION_ID } }),
    ]);
    const text = encoded(scrubbed);
    assert.notInclude(text, HOST_ID);
    assert.notInclude(text, SESSION_ID);
    assert.include(text, '"hostId":"<host-id>"');
    const [request, response] = scrubbed.map((entry) =>
      entry.type === "runtime_exit" ? {} : (entry.frame as Record<string, unknown>),
    );
    const requested = (request!.params as { sessionId: string }).sessionId;
    const answered = (response!.result as { sessionId: string }).sessionId;
    assert.equal(requested, answered);
    assert.match(requested, /^00000000-0000-4000-8000-\d{12}$/u);
  });

  it("collects the host id before scrubbing so the leak check can look for it", () => {
    assert.deepEqual(collectHostIds([loadSessionResponse]), [HOST_ID]);
  });

  it("collects machine ids from the adapter's request and from responses", () => {
    assert.deepEqual(
      collectMachineIds([
        outbound({
          type: "request",
          id: "1",
          method: "droid.initialize_session",
          params: { machineId: "default", cwd: "<any>" },
        }),
        inbound({ type: "response", id: "1", result: { machineId: MACHINE_ID } }),
        inbound({ type: "response", id: "2", result: { session: { machineId: MACHINE_ID } } }),
        { type: "runtime_exit", status: "success" },
      ]),
      ["default", MACHINE_ID],
    );
  });

  it("replaces thinking signatures with a neutral placeholder and keeps the rest of the block", () => {
    const [entry] = scrub([
      inbound({
        type: "notification",
        method: "droid.session_notification",
        params: {
          notification: {
            type: "create_message",
            message: {
              id: "assistant-1",
              role: "assistant",
              content: [
                {
                  type: "thinking",
                  thinking: "plan the reply",
                  signature: SIGNATURE,
                  signatureProvider: "anthropic",
                  durationMs: 10,
                },
                { type: "text", text: "done" },
              ],
            },
          },
        },
      }),
    ]);
    assert.notInclude(encoded([entry!]), SIGNATURE);
    const frame = entry?.type === "emit_inbound" ? entry.frame : undefined;
    const content = (
      frame as {
        params: { notification: { message: { content: Array<unknown> } } };
      }
    ).params.notification.message.content;
    assert.deepEqual(content, [
      {
        type: "thinking",
        thinking: "plan the reply",
        signature: "<signature>",
        signatureProvider: "anthropic",
        durationMs: 10,
      },
      { type: "text", text: "done" },
    ]);
  });

  it("keeps only the recorded model in model lists", () => {
    const models = [
      { id: "claude-haiku-5-5", modelProvider: "anthropic" },
      {
        id: "my-byok-model",
        modelProvider: "generic-chat-completion-api",
        baseUrl: "https://example.test",
      },
    ];
    const scrubbed = scrub([
      inbound({ type: "response", id: "1", result: { models } }),
      inbound({
        type: "response",
        id: "2",
        result: { sessionId: SESSION_ID, availableModels: models },
      }),
    ]);
    const text = encoded(scrubbed);
    assert.notInclude(text, "my-byok-model");
    assert.notInclude(text, "example.test");
    assert.include(text, "claude-haiku-5-5");
  });

  it("wildcards the working directory and settings file in the launch command", () => {
    const [entry] = scrub([
      outbound({
        type: "process_start",
        args: [
          "exec",
          "--input-format",
          "stream-jsonrpc",
          "--output-format",
          "stream-jsonrpc",
          "--cwd",
          "/private/tmp/droid-v2-rec-abc123/ws",
          "--settings",
          "/var/folders/zz/yy/T/t3-droid-settings-xyz/settings.json",
        ],
      }),
    ]);
    assert.deepEqual(entry, {
      type: "expect_outbound",
      frame: {
        type: "process_start",
        args: [
          "exec",
          "--input-format",
          "stream-jsonrpc",
          "--output-format",
          "stream-jsonrpc",
          "--cwd",
          "<any>",
          "--settings",
          "<any>",
        ],
      },
    });
  });

  it("wildcards the working directory the adapter sends in a request", () => {
    const [entry] = scrub([
      outbound({
        type: "request",
        id: "1",
        method: "droid.initialize_session",
        params: { machineId: "default", cwd: "/private/tmp/droid-v2-rec-abc123/ws" },
      }),
    ]);
    assert.deepEqual(entry, {
      type: "expect_outbound",
      frame: {
        type: "request",
        id: "1",
        method: "droid.initialize_session",
        params: { machineId: "default", cwd: "<any>" },
      },
    });
  });

  it("replaces machine paths and names with placeholders", () => {
    const [entry] = scrub([
      inbound({
        type: "notification",
        params: {
          notification: {
            type: "tool_result",
            content: [
              "cwd /private/tmp/droid-v2-rec-abc123/ws/file.txt",
              "also /tmp/droid-v2-rec-abc123/ws",
              "home /Users/example/notes by exampleuser on example-host.local",
              "tmp /var/folders/zz/yy/T/scratch and /private/var/folders/zz/yy/T/other",
            ].join("\n"),
          },
        },
      }),
    ]);
    const text = encoded([entry!]);
    assert.include(text, "cwd <workspace>/file.txt");
    assert.include(text, "also <workspace>");
    assert.include(text, "home ~/notes by <user> on <host>");
    assert.include(text, "tmp <tmpdir>/scratch and <tmpdir>/other");
    for (const leaked of [
      "/Users/",
      "exampleuser",
      "example-host",
      "droid-v2-rec",
      "/var/folders",
    ]) {
      assert.notInclude(text, leaked);
    }
  });

  it("replaces a recording root that Droid cut off mid-name", () => {
    const titles = [
      "Create /private/tmp/droid",
      "Create /private/tmp/droid-v2-rec-ab",
      "Create /tmp/droid-v2-rec-abc123",
    ];
    const text = encoded(
      titles.map(
        (titleHint) =>
          scrub([
            inbound({
              type: "notification",
              params: { notification: { type: "tool_call", toolUse: { input: { titleHint } } } },
            }),
          ])[0]!,
      ),
    );
    assert.equal(text.split("Create <recording-root>").length - 1, titles.length);
    assert.notInclude(text, "droid");
    assert.notInclude(text, "/private");
  });

  it("keeps temporary paths outside the recording root", () => {
    const [entry] = scrub([
      inbound({
        type: "notification",
        params: { notification: { type: "tool_result", content: "see /tmp/other/notes.txt" } },
      }),
    ]);
    assert.include(encoded([entry!]), "see /tmp/other/notes.txt");
  });

  it("drops trace headers and shifts wall-clock times to a fixed epoch", () => {
    const scrubbed = scrub([
      inbound({
        type: "notification",
        _meta: { traceparent: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01" },
        params: {
          notification: { type: "x", timestamp: 1_800_000_000_000, createdAt: 1_800_000_000_500 },
        },
      }),
    ]);
    const frame = scrubbed[0]?.type === "emit_inbound" ? scrubbed[0].frame : undefined;
    assert.notProperty(frame as object, "_meta");
    const notification = (frame as { params: { notification: Record<string, number> } }).params
      .notification;
    assert.equal(notification.timestamp, Date.parse("2026-01-01T00:00:00.000Z"));
    assert.equal(notification.createdAt, Date.parse("2026-01-01T00:00:00.500Z"));
  });
});
