import { useMemo } from "react";
import { Link, useMatchRoute } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";
import { api, type ChannelInfo } from "@/api/client";
import type { ChannelLiveState } from "@/hooks/useChannelLiveState";

interface SidebarProps {
	liveStates: Record<string, ChannelLiveState>;
}

export function Sidebar({ liveStates }: SidebarProps) {
	const { data: agentsData } = useQuery({
		queryKey: ["agents"],
		queryFn: api.agents,
		refetchInterval: 30_000,
	});

	const { data: channelsData } = useQuery({
		queryKey: ["channels"],
		queryFn: api.channels,
		refetchInterval: 10_000,
	});

	const agents = agentsData?.agents ?? [];
	const channels = channelsData?.channels ?? [];

	const matchRoute = useMatchRoute();
	const isOverview = matchRoute({ to: "/" });

	// Count active workers/branches per agent
	const agentActivity = useMemo(() => {
		const byAgent: Record<string, { workers: number; branches: number }> = {};
		for (const channel of channels) {
			const live = liveStates[channel.id];
			if (!live) continue;
			if (!byAgent[channel.agent_id]) byAgent[channel.agent_id] = { workers: 0, branches: 0 };
			byAgent[channel.agent_id].workers += Object.keys(live.workers).length;
			byAgent[channel.agent_id].branches += Object.keys(live.branches).length;
		}
		return byAgent;
	}, [channels, liveStates]);

	return (
		<nav className="flex h-full w-56 flex-col border-r border-sidebar-line bg-sidebar">
			{/* Logo */}
			<div className="flex h-12 items-center gap-2 border-b border-sidebar-line px-3">
				<Link to="/" className="flex items-center gap-2">
					<img src="/ball.png" alt="" className="h-5 w-5" draggable={false} />
					<span className="font-plex text-sm font-semibold text-sidebar-ink">
						Spacebot
					</span>
				</Link>
			</div>

			{/* Top-level nav */}
			<div className="flex flex-col gap-0.5 pt-2">
				<Link
					to="/"
					className={`mx-2 flex items-center gap-2 rounded-md px-2 py-1.5 text-sm ${
						isOverview
							? "bg-sidebar-selected text-sidebar-ink"
							: "text-sidebar-inkDull hover:bg-sidebar-selected/50"
					}`}
				>
					Dashboard
				</Link>
				<Link
					to="/logs"
					className="mx-2 flex items-center gap-2 rounded-md px-2 py-1.5 text-sm text-sidebar-inkDull hover:bg-sidebar-selected/50 [&.active]:bg-sidebar-selected [&.active]:text-sidebar-ink"
					activeProps={{ className: "active" }}
				>
					Logs
				</Link>
			</div>

			{/* Agents */}
			<div className="flex flex-1 flex-col overflow-y-auto pt-3">
				<span className="px-3 pb-1 text-tiny font-medium uppercase tracking-wider text-sidebar-inkFaint">
					Agents
				</span>
				{agents.length === 0 ? (
					<span className="px-3 py-2 text-tiny text-sidebar-inkFaint">
						No agents configured
					</span>
				) : (
					<div className="flex flex-col gap-0.5">
						{agents.map((agent) => {
							const activity = agentActivity[agent.id];
							const isActive = matchRoute({ to: "/agents/$agentId", params: { agentId: agent.id }, fuzzy: true });

							return (
								<Link
									key={agent.id}
									to="/agents/$agentId"
									params={{ agentId: agent.id }}
									className={`mx-2 flex items-center gap-2 rounded-md px-2 py-1.5 text-sm ${
										isActive
											? "bg-sidebar-selected text-sidebar-ink"
											: "text-sidebar-inkDull hover:bg-sidebar-selected/50"
									}`}
								>
									<span className="flex-1 truncate">{agent.id}</span>
									{activity && (activity.workers > 0 || activity.branches > 0) && (
										<div className="flex items-center gap-1">
											{activity.workers > 0 && (
												<span className="rounded bg-amber-500/15 px-1 py-0.5 text-tiny text-amber-400">
													{activity.workers}w
												</span>
											)}
											{activity.branches > 0 && (
												<span className="rounded bg-violet-500/15 px-1 py-0.5 text-tiny text-violet-400">
													{activity.branches}b
												</span>
											)}
										</div>
									)}
								</Link>
							);
						})}
					</div>
				)}
				<button className="mx-2 mt-1 flex items-center justify-center rounded-md border border-dashed border-sidebar-line px-2 py-1.5 text-sm text-sidebar-inkFaint hover:border-sidebar-inkFaint hover:text-sidebar-inkDull">
					+ New Agent
				</button>
			</div>
		</nav>
	);
}
