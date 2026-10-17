import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import { EnvironmentId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";

import {
  PRIME_T3_MCP_EXTENSION_FILENAME,
  T3_MCP_BEARER_ENV,
  T3_MCP_URL_ENV,
  T3_PRIME_RUNTIME_MODE_ENV,
  T3_PRIME_MCP_EXTENSION_PATH_ENV,
} from "./mcpExtensionSource.ts";
import {
  buildPrimeRpcLaunch,
  materializePrimeT3McpExtension,
  resolvePrimeLaunchArgs,
} from "./mcpInjection.ts";

const threadId = ThreadId.make("thread-prime-t3-mcp");

const mcpSession = {
  environmentId: EnvironmentId.make("environment-prime-t3-mcp"),
  threadId,
  providerSessionId: "mcp-session-prime",
  providerInstanceId: ProviderInstanceId.make("prime-agent"),
  endpoint: "http://127.0.0.1:43123/mcp",
  authorizationHeader: "Bearer secret-prime-token",
  browserToolsAvailable: true,
};

describe("prime-agent T3 MCP injection", () => {
  it("always adds the permission bridge and configures MCP when available", () => {
    const resolvedArgs = resolvePrimeLaunchArgs(
      "--extension=/home/user/.prime/agent/extensions/demo.ts --session-dir=/tmp/prime-sessions --provider=anthropic --model=claude-sonnet --tools='' --name=-review --extension-flag=kept",
    );
    assert.isTrue(resolvedArgs.ok);
    if (!resolvedArgs.ok) return;
    const launch = buildPrimeRpcLaunch({
      launchArgs: resolvedArgs.args,
      environment: { PATH: "/usr/bin" },
      mcpSession,
      extensionPath: "/tmp/cache/prime-t3-mcp-extension.ts",
      runtimeMode: "approval-required",
    });
    assert.deepEqual(launch.args, [
      "--mode",
      "rpc",
      "--extension",
      "/home/user/.prime/agent/extensions/demo.ts",
      "--session-dir",
      "/tmp/prime-sessions",
      "--provider",
      "anthropic",
      "--model",
      "claude-sonnet",
      "--tools",
      "",
      "--name",
      "-review",
      "--extension-flag=kept",
      "--extension",
      "/tmp/cache/prime-t3-mcp-extension.ts",
    ]);
    assert.notInclude(launch.args, "--no-extensions");
    assert.equal(launch.env[T3_MCP_URL_ENV], "http://127.0.0.1:43123/mcp");
    assert.equal(launch.env[T3_MCP_BEARER_ENV], "secret-prime-token");
    assert.equal(launch.env[T3_PRIME_RUNTIME_MODE_ENV], "approval-required");
    assert.equal(
      launch.env[T3_PRIME_MCP_EXTENSION_PATH_ENV],
      "/tmp/cache/prime-t3-mcp-extension.ts",
    );

    const permissionOnly = buildPrimeRpcLaunch({
      launchArgs: [],
      environment: {
        [T3_MCP_URL_ENV]: "http://127.0.0.1:9999/stale",
        [T3_MCP_BEARER_ENV]: "stale-token",
        [T3_PRIME_MCP_EXTENSION_PATH_ENV]: "/stale/extension.ts",
      },
      mcpSession: undefined,
      extensionPath: "/tmp/cache/prime-t3-mcp-extension.ts",
      runtimeMode: "auto-accept-edits",
    });
    assert.deepEqual(permissionOnly.args, [
      "--mode",
      "rpc",
      "--extension",
      "/tmp/cache/prime-t3-mcp-extension.ts",
    ]);
    assert.isFalse(permissionOnly.hasT3Mcp);
    assert.isUndefined(permissionOnly.env[T3_MCP_URL_ENV]);
    assert.isUndefined(permissionOnly.env[T3_MCP_BEARER_ENV]);
    assert.equal(permissionOnly.env[T3_PRIME_RUNTIME_MODE_ENV], "auto-accept-edits");
    assert.equal(
      permissionOnly.env[T3_PRIME_MCP_EXTENSION_PATH_ENV],
      "/tmp/cache/prime-t3-mcp-extension.ts",
    );
  });

  it("falls back to Prime's first supported mode for legacy auto threads", () => {
    const launch = buildPrimeRpcLaunch({
      launchArgs: [],
      environment: {},
      mcpSession: undefined,
      extensionPath: "/tmp/cache/prime-t3-mcp-extension.ts",
      runtimeMode: "auto",
    });

    assert.equal(launch.env[T3_PRIME_RUNTIME_MODE_ENV], "approval-required");
  });

  it("forces tools and user extensions off for unattended text generation", () => {
    const launch = buildPrimeRpcLaunch({
      launchArgs: [
        "--tools",
        "read,write",
        "--extension",
        "/home/user/.prime/agent/extensions/demo.ts",
        "--extension=./second.ts",
        "--provider",
        "anthropic",
      ],
      environment: {},
      mcpSession,
      extensionPath: "/tmp/cache/prime-t3-mcp-extension.ts",
      ephemeral: true,
      disableExtensions: true,
      disableTools: true,
    });
    assert.deepEqual(launch.args, [
      "--mode",
      "rpc",
      "--no-session",
      "--provider",
      "anthropic",
      "--no-extensions",
      "--no-tools",
    ]);
    assert.isFalse(launch.hasT3Mcp);
    assert.deepInclude(resolvePrimeLaunchArgs("--mode text"), {
      ok: false,
      message: "Prime launch argument '--mode' is controlled by T3 Code and cannot be overridden.",
    });
    assert.deepInclude(resolvePrimeLaunchArgs("--session old.jsonl"), { ok: false });
    assert.deepInclude(resolvePrimeLaunchArgs("prompt prime-agent immediately"), { ok: false });
    assert.deepInclude(resolvePrimeLaunchArgs("--plan @instructions.md"), { ok: false });
  });

  it("rejects --provider without --model, which Prime refuses at startup", () => {
    const rejection = {
      ok: false,
      message: "Prime launch argument '--provider' requires '--model'.",
    };
    assert.deepInclude(resolvePrimeLaunchArgs("--provider openrouter"), rejection);
    assert.deepInclude(resolvePrimeLaunchArgs("--provider=openrouter --models gpt-6"), rejection);
    assert.isTrue(resolvePrimeLaunchArgs("--provider openrouter --model=deepseek/v4").ok);
    assert.isTrue(resolvePrimeLaunchArgs("--model deepseek/v4").ok);
  });

  it.effect("materializes the MCP bridge with namespaced tool registration", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const cacheDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-prime-extensions-" });
      const mcpDest = yield* materializePrimeT3McpExtension(cacheDir);
      assert.isTrue(mcpDest.endsWith(PRIME_T3_MCP_EXTENSION_FILENAME));
      const mcpSource = yield* fs.readFileString(mcpDest);
      assert.include(mcpSource, "export default async function t3McpExtension");
      assert.include(mcpSource, "before_agent_start");
      assert.include(mcpSource, 'pi.on("tool_call"');
      assert.include(mcpSource, "Allow ${event.toolName}?");
      assert.include(mcpSource, '"mcp-protocol-version"');
      assert.include(mcpSource, '"tools/call"');
      assert.include(mcpSource, "mcp__t3-code__");
      assert.include(mcpSource, "mcp__t3_code__");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});
