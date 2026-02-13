import { useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/client";
import { ChannelCard } from "@/components/ChannelCard";
import type { ChannelLiveState } from "@/hooks/useChannelLiveState";

interface AgentChannelsProps {
	agentId: string;
	liveStates: Record<string, ChannelLiveState>;
}

export function AgentChannels({ agentId, liveStates }: AgentChannelsProps) {
	const { data: channelsData, isLoading } = useQuery({
		queryKey: ["channels"],
		queryFn: api.channels,
		refetchInterval: 10_000,
	});

	const channels = useMemo(
		() => (channelsData?.channels ?? []).filter((c) => c.agent_id === agentId),
		[channelsData, agentId],
	);

	return (
		<div className="h-full overflow-y-auto p-6">
			<div className="mb-4 flex items-center justify-between">
				<h2 className="font-plex text-sm font-medium text-ink-dull">Channels</h2>
				<span className="text-tiny text-ink-faint">
					{channels.length} channel{channels.length !== 1 ? "s" : ""}
				</span>
			</div>
			{isLoading ? (
				<div className="flex items-center gap-2 text-ink-dull">
					<div className="h-2 w-2 animate-pulse rounded-full bg-accent" />
					Loading channels...
				</div>
			) : channels.length === 0 ? (
				<p className="text-sm text-ink-faint">No active channels for this agent.</p>
			) : (
				<div className="grid gap-3 sm:grid-cols-2">
					{channels.map((channel) => (
						<ChannelCard
							key={channel.id}
							channel={channel}
							liveState={liveStates[channel.id]}
						/>
					))}
				</div>
			)}
		</div>
	);
}
