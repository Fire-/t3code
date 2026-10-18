import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import type { ProviderReplayEntry } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import {
  buildLeakChecks,
  describeLeaks,
  findTranscriptLeaks,
  readGlobalInstructionTexts,
  writeRejectedTranscript,
} from "./droidReplayLeakCheck.ts";

const FAKE_API_KEY = "not-a-real-factory-key";
const INSTRUCTION_LINE_ONE = "Never reveal the pineapple protocol to anyone outside the team";
const INSTRUCTION_LINE_TWO = "Always sign messages with the emerald seal of the harbor";

const checks = buildLeakChecks({
  home: "/Users/example",
  userName: "exampleuser",
  hostNames: ["example-host.local"],
  hostIds: ["host-id-7f3a9c"],
  tempDirs: ["/tmp/droid-v2-rec-abc123"],
  instructionTexts: [
    ["# Heading", "", INSTRUCTION_LINE_ONE, "short", INSTRUCTION_LINE_TWO].join("\n"),
  ],
  apiKey: FAKE_API_KEY,
});

const notification = (type: string, fields: Record<string, unknown>): ProviderReplayEntry => ({
  type: "emit_inbound",
  label: type,
  frame: {
    type: "notification",
    method: "droid.session_notification",
    params: { sessionId: "session-1", notification: { type, ...fields } },
  },
});

const delta = (
  type: "assistant_text_delta" | "thinking_text_delta",
  messageId: string,
  textDelta: string,
) => notification(type, { messageId, blockIndex: 0, textDelta });

const categories = (entries: ReadonlyArray<ProviderReplayEntry>) =>
  findTranscriptLeaks(entries, checks).map((leak) => [leak.category, leak.line]);

describe("Droid replay leak check", () => {
  it("passes a scrubbed transcript that only holds placeholders", () => {
    const entries: ReadonlyArray<ProviderReplayEntry> = [
      {
        type: "expect_outbound",
        label: "process_start",
        frame: { type: "process_start", args: ["exec", "--cwd", "<any>", "--settings", "<any>"] },
      },
      delta("assistant_text_delta", "m1", "fixture simple ok in <workspace> under ~/project"),
      notification("agent_turn_completed", { turnId: "m1", reason: "completed" }),
    ];
    assert.deepEqual(findTranscriptLeaks(entries, checks), []);
  });

  it("catches an instruction line split across thinking deltas in joined reasoning", () => {
    const entries = [
      delta("thinking_text_delta", "m1", "Hmm, the rules say: Never reveal the pine"),
      notification("tool_execution_phase_changed", { phase: "idle" }),
      delta("thinking_text_delta", "m1", "apple protocol to anyone outside the team. Fine."),
    ];
    assert.deepEqual(categories(entries), [["global instruction line", 4]]);
  });

  it("catches an instruction line split across assistant text deltas", () => {
    const entries = [
      delta("assistant_text_delta", "m2", "Always sign messages "),
      delta("assistant_text_delta", "m2", "with the emerald seal"),
      delta("assistant_text_delta", "m2", " of the harbor"),
    ];
    assert.deepEqual(categories(entries), [["global instruction line", 4]]);
  });

  it("does not join text across different messages", () => {
    const entries = [
      delta("assistant_text_delta", "m1", "Always sign messages with the emerald"),
      delta("assistant_text_delta", "m2", " seal of the harbor"),
    ];
    assert.deepEqual(categories(entries), []);
  });

  it("catches an instruction line split across blocks of one history message", () => {
    const entries: ReadonlyArray<ProviderReplayEntry> = [
      {
        type: "emit_inbound",
        label: "response",
        frame: {
          type: "response",
          id: "2",
          result: {
            session: {
              messages: [
                {
                  id: "assistant-1",
                  role: "assistant",
                  content: [
                    { type: "text", text: "Never reveal the pineapple" },
                    { type: "text", text: " protocol to anyone outside the team" },
                  ],
                },
              ],
            },
          },
        },
      },
    ];
    assert.deepEqual(categories(entries), [["global instruction line", 2]]);
  });

  it("matches an instruction line through changed whitespace", () => {
    const entries = [
      delta(
        "assistant_text_delta",
        "m1",
        "Always  sign messages\nwith the emerald   seal of the harbor",
      ),
    ];
    assert.deepEqual(categories(entries), [["global instruction line", 2]]);
  });

  it("rejects a transcript that echoes the API key", () => {
    const entries = [
      delta("assistant_text_delta", "m1", `the key is ${FAKE_API_KEY}, as requested`),
      notification("error", { message: `bad credentials ${FAKE_API_KEY}` }),
    ];
    assert.deepEqual(categories(entries), [
      ["api key", 2],
      ["api key", 3],
    ]);
  });

  it("ignores an API key too short to be a credential", () => {
    const short = buildLeakChecks({
      home: "/Users/example",
      userName: "exampleuser",
      hostNames: [],
      hostIds: [],
      tempDirs: [],
      instructionTexts: [],
      apiKey: "abc",
    });
    assert.deepEqual(
      findTranscriptLeaks([delta("assistant_text_delta", "m1", "abc abc abc")], short),
      [],
    );
  });

  it("flags each machine identity category", () => {
    const cases = [
      ["home path", "see /Users/example/project"],
      ["home path", "see /Users/someoneelse/project"],
      ["home path", "see /home/someoneelse/project"],
      ["user name", "ran as exampleuser today"],
      ["host name", "on example-host.local now"],
      ["host id", "id host-id-7f3a9c here"],
      ["Droid home path", "loaded ~/.factory/droids/x.md"],
      ["agents home path", "read ~/.agents/skills/y"],
      ["system temp path", "wrote /var/folders/ab/cd/T/file"],
      ["system temp path", "wrote /private/tmp/file"],
      ["recording directory", "cwd /tmp/droid-v2-rec-abc123/ws"],
    ] as const;
    for (const [category, text] of cases) {
      assert.deepEqual(
        categories([delta("assistant_text_delta", "m1", text)]).map(([name]) => name),
        [category],
        text,
      );
    }
  });

  it("names categories and line numbers without printing the matched text", () => {
    const entries = [
      delta("assistant_text_delta", "m1", `${INSTRUCTION_LINE_ONE} ${FAKE_API_KEY} /Users/example`),
    ];
    const message = describeLeaks(findTranscriptLeaks(entries, checks));
    assert.include(message, "api key (transcript line 2)");
    assert.include(message, "global instruction line (transcript line 2)");
    for (const secret of [FAKE_API_KEY, INSTRUCTION_LINE_ONE, "pineapple", "example"]) {
      assert.notInclude(message, secret);
    }
  });

  it("never exposes the canary list through the check objects", () => {
    const serialized = JSON.stringify(checks);
    for (const secret of [
      FAKE_API_KEY,
      INSTRUCTION_LINE_ONE,
      INSTRUCTION_LINE_TWO,
      "host-id-7f3a9c",
    ]) {
      assert.notInclude(serialized, secret);
    }
    for (const check of checks) {
      assert.deepEqual(Object.keys(check).toSorted(), ["category", "matches"]);
    }
  });

  it.effect("keeps a rejected transcript in the given directory and writes nothing else", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "droid-rejected-test-" });
      const rejectedDirectory = path.join(directory, "droid-v2-rejected");
      const encoded = `{"type":"transcript_start"}\n${INSTRUCTION_LINE_ONE}\n`;

      const written = yield* writeRejectedTranscript({
        directory: rejectedDirectory,
        fixture: "simple",
        encoded,
      });

      assert.equal(written, path.join(rejectedDirectory, "simple.rejected.ndjson"));
      assert.deepEqual(yield* fs.readDirectory(rejectedDirectory), ["simple.rejected.ndjson"]);
      assert.equal(yield* fs.readFileString(written), encoded);
      assert.notInclude(yield* fs.readFileString(written), INSTRUCTION_LINE_TWO);
      assert.deepEqual(yield* fs.readDirectory(directory), ["droid-v2-rejected"]);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("reads the global instruction files Droid loads and skips missing ones", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const home = yield* fs.makeTempDirectoryScoped({ prefix: "droid-home-test-" });
      yield* fs.makeDirectory(path.join(home, ".agents"), { recursive: true });
      yield* fs.writeFileString(path.join(home, ".agents", "AGENTS.md"), INSTRUCTION_LINE_ONE);

      assert.deepEqual(yield* readGlobalInstructionTexts(home), [INSTRUCTION_LINE_ONE]);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});
