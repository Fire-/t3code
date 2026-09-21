import type { DeviceToolVersions as ToolVersions } from "@t3tools/contracts";
import {
  deviceToolVersionLabels,
  deviceToolUpdatePolicy,
} from "@t3tools/client-runtime/state/device";

export function DeviceToolVersions({
  tools,
  error,
}: {
  tools: ToolVersions | undefined;
  error?: string | undefined;
}) {
  return (
    <div className="space-y-1 text-xs text-muted-foreground">
      {deviceToolVersionLabels(tools).map((label) => (
        <p key={label}>{label}</p>
      ))}
      <p>{deviceToolUpdatePolicy(tools)}</p>
      {error ? (
        <p role="status" className="text-destructive">
          {error}
        </p>
      ) : null}
    </div>
  );
}
