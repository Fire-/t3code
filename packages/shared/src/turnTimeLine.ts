// `+` is the gap marker stamps used before Δ; providers keep those in their own session files.
const STAMP_LINE =
  /^\[\d{2}-\d{2} \d{2}:\d{2} [^\s\]]+(?: [Δ+]\d+(?:s|m|h(?:\d+m)?|d(?:\d+h)?))?\]\n/u;

const formatGap = (milliseconds: number): string => {
  const seconds = Math.max(0, Math.floor(milliseconds / 1000));
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return minutes % 60 === 0 ? `${hours}h` : `${hours}h${minutes % 60}m`;
  const days = Math.floor(hours / 24);
  return hours % 24 === 0 ? `${days}d` : `${days}d${hours % 24}h`;
};

/** `[MM-DD HH:mm ZONE Δgap]` for when a message was sent; times are epoch milliseconds. */
export function formatTurnTimeLine(input: {
  readonly sentAt: number;
  readonly previousAt?: number | undefined;
  readonly timeZone: string;
}): string {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: input.timeZone,
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hourCycle: "h23",
    timeZoneName: "short",
  }).formatToParts(input.sentAt);
  const part = (type: Intl.DateTimeFormatPartTypes) =>
    parts.find((candidate) => candidate.type === type)?.value ?? "";
  const gap =
    input.previousAt === undefined ? "" : ` Δ${formatGap(input.sentAt - input.previousAt)}`;
  return `[${part("month")}-${part("day")} ${part("hour")}:${part("minute")} ${part("timeZoneName")}${gap}]`;
}

export const prefixTurnTimeLine = (text: string, line: string): string => `${line}\n${text}`;

export const stripTurnTimeLine = (text: string): string => text.replace(STAMP_LINE, "");
