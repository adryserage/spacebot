import type { ConnectionState } from "@/hooks/useEventSource";

export function ConnectionBanner({ state }: { state: ConnectionState }) {
	if (state === "connected") return null;

	const config: Record<Exclude<ConnectionState, "connected">, { label: string; color: string }> = {
		connecting: { label: "Connecting...", color: "bg-blue-500/10 text-blue-400 border-blue-500/20" },
		reconnecting: { label: "Reconnecting... Dashboard may show stale data.", color: "bg-amber-500/10 text-amber-400 border-amber-500/20" },
		disconnected: { label: "Disconnected from server.", color: "bg-red-500/10 text-red-400 border-red-500/20" },
	};

	const { label, color } = config[state];

	return (
		<div className={`border-b px-4 py-2 text-sm ${color}`}>
			<div className="flex items-center gap-2">
				<div className="h-1.5 w-1.5 animate-pulse rounded-full bg-current" />
				{label}
			</div>
		</div>
	);
}
