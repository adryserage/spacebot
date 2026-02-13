import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/client";
import type { ChannelLiveState } from "@/hooks/useChannelLiveState";

interface AgentDetailProps {
	agentId: string;
	liveStates: Record<string, ChannelLiveState>;
}

export function AgentDetail({ agentId, liveStates }: AgentDetailProps) {
	const { data: agentsData } = useQuery({
		queryKey: ["agents"],
		queryFn: api.agents,
		refetchInterval: 30_000,
	});

	const agent = agentsData?.agents.find((a) => a.id === agentId);

	if (!agent) {
		return (
			<div className="flex h-full items-center justify-center">
				<p className="text-sm text-ink-faint">Agent not found: {agentId}</p>
			</div>
		);
	}

	return (
		<div className="h-full overflow-y-auto p-6">
			<div className="mb-3">
				<h2 className="font-plex text-sm font-medium text-ink-dull">Configuration</h2>
			</div>
			<div className="grid grid-cols-2 gap-3 sm:grid-cols-3">
				<ConfigItem label="Workspace" value={agent.workspace} />
				<ConfigItem label="Context Window" value={agent.context_window.toLocaleString()} />
				<ConfigItem label="Max Turns" value={String(agent.max_turns)} />
				<ConfigItem label="Max Branches" value={String(agent.max_concurrent_branches)} />
			</div>
		</div>
	);
}

function ConfigItem({ label, value }: { label: string; value: string }) {
	return (
		<div className="rounded-md bg-app-darkBox px-3 py-2">
			<span className="text-tiny text-ink-faint">{label}</span>
			<p className="mt-0.5 truncate text-sm text-ink-dull" title={value}>{value}</p>
		</div>
	);
}
