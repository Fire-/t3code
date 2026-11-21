import { scopeThreadRef, scopedThreadKey } from "@t3tools/client-runtime/environment";
import type { ScopedThreadRef } from "@t3tools/contracts";
import { useGT } from "gt-react";
import {
  CircleDashedIcon,
  CircleDotDashedIcon,
  MessageCircleQuestionIcon,
  ShieldQuestionIcon,
} from "lucide-react";
import { memo, type KeyboardEvent as ReactKeyboardEvent } from "react";

import { cn } from "~/lib/utils";
import { textDirection } from "~/textDirection";
import type { SidebarThreadSummary } from "../../types";
import { ProviderInstanceIcon } from "../chat/ProviderInstanceIcon";
import {
  resolveSidebarRowAccessibility,
  resolveSidebarThreadStatus,
  resolveWorkingStartedAt,
} from "../Sidebar.logic";
import { WorkingDuration, type SidebarProviderEntry } from "./SidebarThreadRow";

const childRowInsetClassName = (inFolder: boolean) => (inFolder ? "ps-12" : "ps-7");

/** A running subagent, side chat or delegated task, one line under its parent's row. */
export const SidebarChildThreadRow = memo(function SidebarChildThreadRow(props: {
  thread: SidebarThreadSummary;
  parentRowKey: string;
  inFolder: boolean;
  isActive: boolean;
  hidden: boolean;
  providerEntry: SidebarProviderEntry | null;
  onActivate: (threadRef: ScopedThreadRef) => void;
}) {
  const { thread, onActivate } = props;
  const gt = useGT();
  const threadRef = scopeThreadRef(thread.environmentId, thread.id);
  const status = resolveSidebarThreadStatus(thread);
  const indicator =
    status === "approval"
      ? {
          icon: ShieldQuestionIcon,
          label: gt("Approval", { $context: "thread status: needs approval" }),
          className: "text-warning-foreground",
        }
      : status === "input"
        ? {
            icon: MessageCircleQuestionIcon,
            label: gt("Input", { $context: "thread status: needs user input" }),
            className: "text-indigo-600 dark:text-indigo-300",
          }
        : status === "waiting"
          ? {
              icon: CircleDotDashedIcon,
              label: gt("Waiting", { $context: "thread status" }),
              className: "text-muted-foreground",
            }
          : {
              icon: CircleDashedIcon,
              label:
                status === "preparing"
                  ? gt("Setting up")
                  : gt("Working", { $context: "thread status" }),
              className: "text-info-foreground",
            };
  const StatusIcon = indicator.icon;
  const accessibility = resolveSidebarRowAccessibility({
    title: thread.title,
    statusLabel: indicator.label,
    projectDisplayName: null,
    isActive: props.isActive,
  });
  const handleKeyDown = (event: ReactKeyboardEvent) => {
    if (event.key !== "Enter" && event.key !== " ") return;
    event.preventDefault();
    onActivate(threadRef);
  };
  return (
    <li className={cn("list-none", props.hidden && "invisible")}>
      <div
        role="button"
        tabIndex={0}
        data-sidebar-row={scopedThreadKey(threadRef)}
        data-sidebar-parent={props.parentRowKey}
        data-testid="sidebar-child-row"
        aria-label={accessibility.label}
        aria-current={accessibility.current}
        className={cn(
          "group/sidebar-row flex h-7 w-full min-w-0 cursor-pointer items-center gap-1.5 rounded-md pe-2.5 text-left text-xs outline-none select-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring",
          childRowInsetClassName(props.inFolder),
          props.isActive
            ? "bg-sidebar-row-active text-sidebar-foreground"
            : "text-sidebar-muted-foreground hover:bg-sidebar-row-hover hover:text-sidebar-foreground",
        )}
        onClick={() => onActivate(threadRef)}
        onKeyDown={handleKeyDown}
      >
        {props.providerEntry ? (
          <span className="inline-flex shrink-0 items-center" aria-hidden>
            <ProviderInstanceIcon
              driverKind={props.providerEntry.driverKind}
              displayName={props.providerEntry.displayName}
              accentColor={props.providerEntry.accentColor}
              acpRegistryAgentId={props.providerEntry.acpRegistryAgentId}
              acpRegistryIconUrl={props.providerEntry.acpRegistryIconUrl}
              size="compact"
              iconClassName="size-3 opacity-60"
            />
          </span>
        ) : null}
        <span className="min-w-0 flex-1 truncate" dir={textDirection(thread.title)}>
          {thread.title}
        </span>
        <span
          aria-hidden
          className={cn(
            "ml-auto inline-flex shrink-0 items-center gap-1 tabular-nums",
            indicator.className,
          )}
        >
          <StatusIcon className="size-3 shrink-0" />
          <WorkingDuration
            environmentId={thread.environmentId}
            startedAt={resolveWorkingStartedAt(thread)}
          />
        </span>
      </div>
    </li>
  );
});

/** Counts the running children past the ones listed; it opens nothing. */
export function SidebarChildThreadOverflow(props: {
  count: number;
  inFolder: boolean;
  hidden: boolean;
}) {
  const gt = useGT();
  return (
    <li
      data-testid="sidebar-child-overflow"
      className={cn(
        "flex h-6 list-none items-center pe-2.5 text-xs text-sidebar-muted-foreground",
        childRowInsetClassName(props.inFolder),
        props.hidden && "invisible",
      )}
    >
      {gt("{count, plural, one {+{count} more running} other {+{count} more running}}", {
        count: props.count,
      })}
    </li>
  );
}
