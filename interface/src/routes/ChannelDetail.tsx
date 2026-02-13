import { useEffect, useRef } from "react";
import { Link } from "@tanstack/react-router";
import type { ChannelInfo } from "@/api/client";
import type { ChannelLiveState, ActiveWorker, ActiveBranch } from "@/hooks/useChannelLiveState";
import { LiveDuration } from "@/components/LiveDuration";
import { Markdown } from "@/components/Markdown";
import { formatTimestamp, platformIcon, platformColor } from "@/lib/format";

interface ChannelDetailProps {
	agentId: string;
	channelId: string;
	channel: ChannelInfo | undefined;
	liveState: ChannelLiveState | undefined;
}

function WorkerDetail({ worker }: { worker: ActiveWorker }) {
	return (
		<div className="flex items-center gap-2 rounded-md bg-amber-500/10 px-3 py-2 text-sm">
			<div className="h-2 w-2 animate-pulse rounded-full bg-amber-400" />
			<div className="min-w-0 flex-1">
				<div className="flex items-center gap-2">
					<span className="font-medium text-amber-300">Worker</span>
					<span className="text-ink-dull">{worker.task}</span>
				</div>
				<div className="mt-1 flex items-center gap-3 text-tiny text-ink-faint">
					<span>{worker.status}</span>
					{worker.currentTool && (
						<span className="text-amber-400/70">{worker.currentTool}</span>
					)}
					{worker.toolCalls > 0 && (
						<span>{worker.toolCalls} tool calls</span>
					)}
				</div>
			</div>
		</div>
	);
}

function BranchDetail({ branch }: { branch: ActiveBranch }) {
	const displayTool = branch.currentTool ?? branch.lastTool;
	return (
		<div className="flex items-center gap-2 rounded-md bg-violet-500/10 px-3 py-2 text-sm">
			<div className="h-2 w-2 animate-pulse rounded-full bg-violet-400" />
			<div className="min-w-0 flex-1">
				<div className="flex items-center gap-2">
					<span className="font-medium text-violet-300">Branch</span>
					<span className="text-ink-dull">{branch.description}</span>
				</div>
				<div className="mt-1 flex items-center gap-3 text-tiny text-ink-faint">
					<LiveDuration startMs={branch.startedAt} />
					{displayTool && (
						<span className={branch.currentTool ? "text-violet-400/70" : "text-violet-400/40"}>{displayTool}</span>
					)}
					{branch.toolCalls > 0 && (
						<span>{branch.toolCalls} tool calls</span>
					)}
				</div>
			</div>
		</div>
	);
}

export function ChannelDetail({ agentId, channelId, channel, liveState }: ChannelDetailProps) {
	const messages = liveState?.messages ?? [];
	const isTyping = liveState?.isTyping ?? false;
	const workers = Object.values(liveState?.workers ?? {});
	const branches = Object.values(liveState?.branches ?? {});

	const messagesEndRef = useRef<HTMLDivElement>(null);
	const prevMessageCount = useRef(messages.length);

	// Auto-scroll when new messages arrive
	useEffect(() => {
		if (messages.length > prevMessageCount.current) {
			messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
		}
		prevMessageCount.current = messages.length;
	}, [messages.length]);

	return (
		<div className="flex h-full flex-col">
			{/* Channel sub-header */}
			<div className="flex h-12 items-center gap-3 border-b border-app-line/50 bg-app-darkBox/20 px-6">
				<Link
					to="/agents/$agentId/channels"
					params={{ agentId }}
					className="text-tiny text-ink-faint hover:text-ink-dull"
				>
					Channels
				</Link>
				<span className="text-ink-faint/50">/</span>
				<span className="text-sm font-medium text-ink">
					{channel?.display_name ?? channelId}
				</span>
				{channel && (
					<span className={`inline-flex items-center rounded-md px-1.5 py-0.5 text-tiny font-medium ${platformColor(channel.platform)}`}>
						{platformIcon(channel.platform)}
					</span>
				)}
				{isTyping && (
					<div className="flex items-center gap-1 ml-auto">
						<span className="inline-block h-1.5 w-1.5 animate-pulse rounded-full bg-accent" />
						<span className="inline-block h-1.5 w-1.5 animate-pulse rounded-full bg-accent [animation-delay:0.2s]" />
						<span className="inline-block h-1.5 w-1.5 animate-pulse rounded-full bg-accent [animation-delay:0.4s]" />
						<span className="ml-1 text-tiny text-ink-faint">typing</span>
					</div>
				)}
			</div>

			{/* Activity bar */}
			{(workers.length > 0 || branches.length > 0) && (
				<div className="flex flex-col gap-2 border-b border-app-line/50 bg-app-darkBox/30 px-6 py-3">
					{workers.map((worker) => (
						<WorkerDetail key={worker.id} worker={worker} />
					))}
					{branches.map((branch) => (
						<BranchDetail key={branch.id} branch={branch} />
					))}
				</div>
			)}

			{/* Messages */}
			<div className="flex-1 overflow-y-auto">
				<div className="flex flex-col gap-1 p-6">
					{messages.length === 0 ? (
						<p className="text-sm text-ink-faint">No messages yet</p>
					) : (
						messages.map((message) => (
							<div
								key={message.id}
								className={`flex gap-3 rounded-md px-3 py-2 ${
									message.sender === "user"
										? "bg-app-darkBox/30"
										: ""
								}`}
							>
								<span className="flex-shrink-0 pt-0.5 text-tiny text-ink-faint">
									{formatTimestamp(message.timestamp)}
								</span>
								<div className="min-w-0 flex-1">
									<span className={`text-sm font-medium ${
										message.sender === "user" ? "text-accent-faint" : "text-green-400"
									}`}>
										{message.sender === "user" ? (message.senderName ?? "user") : "bot"}
									</span>
									<div className="mt-0.5 text-sm text-ink-dull">
										<Markdown>{message.text}</Markdown>
									</div>
								</div>
							</div>
						))
					)}
					{isTyping && (
						<div className="flex gap-3 px-3 py-2">
							<span className="flex-shrink-0 pt-0.5 text-tiny text-ink-faint">
								{formatTimestamp(Date.now())}
							</span>
							<div className="flex items-center gap-1.5">
								<span className="text-sm font-medium text-green-400">bot</span>
								<span className="inline-block h-1.5 w-1.5 animate-pulse rounded-full bg-ink-faint" />
								<span className="inline-block h-1.5 w-1.5 animate-pulse rounded-full bg-ink-faint [animation-delay:0.2s]" />
								<span className="inline-block h-1.5 w-1.5 animate-pulse rounded-full bg-ink-faint [animation-delay:0.4s]" />
							</div>
						</div>
					)}
					<div ref={messagesEndRef} />
				</div>
			</div>
		</div>
	);
}
