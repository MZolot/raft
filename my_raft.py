from node_connector import *
from my_timer import Timer
from colors import *
from request_types import *

from enum import Enum, auto


class Role(Enum):
    CANDIDATE = auto()
    FOLLOWER = auto()
    LEADER = auto()


class Raft:
    def __init__(self, self_node: Node, nodes: list[Node]):
        self.node = self_node
        self.connector = NodeConnector(self_node, nodes)
        self.connector.on_message_received_callback = self.react_to_message

        self.role = Role.FOLLOWER
        self.timer = Timer(self.become_candidate)

        self.current_term = 0
        self.votes_gained = 0
        self.voted_for = None
        self.leader = None

        self.log: list[LogEntry] = [LogEntry(0, None, 0)]

        self.commit_index = 0
        self.last_applied = 0

        self.next_index = {}
        self.match_index = {}

    # =================================================================================================================

    async def start(self):
        await self.connector.start()
        self.become_follower()

    # =================================================================================================================

    def become_follower(self):
        self.role = Role.FOLLOWER
        colorful_print("I'm a follower!", "role")
        self.voted_for = None
        self.timer.reset()

    def become_candidate(self):
        self.role = Role.CANDIDATE
        self.current_term += 1
        colorful_print("\nI'm a Candidate!", "role")
        colorful_print(f'Starting election... term = {self.current_term} ', "vote")
        self.votes_gained = 1
        self.voted_for = self.node

        self.timer.reset()

        request_vote = VoteRequest(
            self.current_term,
            len(self.log) - 1,
            self.log[-1].term
        )

        asyncio.create_task(self.connector.send_message_to_everyone(request_vote))
        # for node in self.connector.nodes:
        #     asyncio.create_task(self.connector.send_message(node, request_vote))

    def become_leader(self):
        self.role = Role.LEADER
        colorful_print("I'm a LEADER!", "role")
        self.timer.stop()
        self.leader = self.node
        self.voted_for = None

        self.next_index = {node: len(self.log) for node in self.connector.nodes}
        self.match_index = {node: 0 for node in self.connector.nodes}

        for node in self.connector.nodes:
            asyncio.create_task(self.send_heartbeat(node))

        # request = AppendEntriesRequest(self.current_term, None)
        # asyncio.create_task(self.connector.send_message_to_everyone(request))

    # =================================================================================================================

    async def react_to_message(self, node: Node, message):
        if isinstance(message, VoteRequest):
            await self.respond_to_vote_request(node, message)
        elif isinstance(message, VoteReply):
            await self.respond_to_vote_reply(node, message)
        elif isinstance(message, AppendEntriesRequest):
            await self.respond_to_append_entries_request(node, message)
        elif isinstance(message, RequestFromClient):
            await self.respond_to_request_from_client(node, message)
        elif isinstance(message, AppendEntriesReply):
            await self.respond_to_append_entries_reply(node, message)
        else:
            colorful_print(
                "Unknown message received from " + str(node) + ": " + str(message) + " -- " + str(type(message)),
                "error"
            )

    async def respond_to_vote_request(self, candidate_node: Node, request: VoteRequest):
        colorful_print("Vote request from " + str(candidate_node), "vote")

        if request.term < self.current_term:
            # Reply false if term < currentTerm
            vote_reply = VoteReply(self.current_term, False)
            colorful_print("-- wrong term", "warning")

            colorful_print("Replying to " + str(candidate_node) + ": " + str(vote_reply.granted), "vote")
            print()
            asyncio.create_task(self.connector.send_message(candidate_node, vote_reply))

            return

        self.update_term(request.term)

        if (not self.voted_for) or (self.voted_for == candidate_node):
            if (request.last_log_term > self.log[-1].term) or (
                    (request.last_log_term == self.log[-1].term) and (request.last_log_index >= len(self.log) - 1)):
                # If votedFor is null or candidateId,
                # and candidate’s log is at least as up-to-date as receiver’s log, grant vote
                vote_reply = VoteReply(self.current_term, True)
                self.voted_for = candidate_node
            else:
                colorful_print("-- log problem", "warning")
                vote_reply = VoteReply(self.current_term, False)
        else:
            colorful_print("-- already voted for someone else", "warning")
            vote_reply = VoteReply(self.current_term, False)

        colorful_print("Replying to " + str(candidate_node) + ": " + str(vote_reply.granted), "vote")
        print()
        asyncio.create_task(self.connector.send_message(candidate_node, vote_reply))

    async def respond_to_vote_reply(self, candidate_node: Node, reply: VoteReply):
        colorful_print("Vote reply from " + str(candidate_node) + ": " + str(reply), "vote")
        self.update_term(reply.term)

        if reply.granted:
            self.votes_gained += 1

        votes_required = (len(self.connector.nodes) + 1) / 2
        colorful_print("Gained " + str(self.votes_gained) + " out of " + str(votes_required), "vote")
        if self.votes_gained > votes_required:
            self.become_leader()

    def update_term(self, new_term):
        if new_term > self.current_term:
            print("Updating term " + str(self.current_term) + " -> " + str(new_term))
            self.current_term = new_term
            self.become_follower()

    # =================================================================================================================

    async def send_heartbeat(self, node: Node):
        print("Heart starts beating for " + str(node))
        while self.role == Role.LEADER:
            request = AppendEntriesRequest(
                term=self.current_term,
                entries=None,
                prev_log_index=self.next_index[node] - 1,
                prev_log_term=self.log[self.next_index[node] - 1].term,
                leader_commit_index=min(self.commit_index, self.next_index[node])
            )
            await self.connector.send_message(node, request)
            # print("bip " + str(node))
            await asyncio.sleep(2)

    async def send_append_entries_request(self):
        if not self.role == Role.LEADER:
            colorful_print("--- send_append_entries_request not by leader!!!", "error")
            return

        for node in self.connector.nodes:
            if len(self.log) - 1 >= self.next_index[node]:
                request = AppendEntriesRequest(
                    term=self.current_term,
                    entries=self.log[self.next_index[node]],
                    prev_log_index=self.next_index[node] - 1,
                    prev_log_term=self.log[self.next_index[node] - 1].term,
                    leader_commit_index=min(self.commit_index, self.next_index[node])
                )
                asyncio.create_task(self.connector.send_message(node, request))

    # Reacting to leader's command to append entries
    async def respond_to_append_entries_request(self, node: Node, request: AppendEntriesRequest):
        # Reply false if term < currentTerm
        if request.term < self.current_term:
            colorful_print("Refusing append entries request: have higher term than leader", "warning")
            await self.connector.send_message(node,
                                              AppendEntriesReply(self.current_term, False, request.entries is None))
            return
        self.update_term(request.term)
        self.timer.reset()

        if self.role == Role.CANDIDATE:
            self.become_follower()
        self.leader = node

        if request.entries is not None:
            colorful_print("Append entries request from " + str(node) + ": " + str(request), "append")

        # Reply false if log does not contain an entry at prevLogIndex whose term matches prevLogTerm

        if request.prev_log_index != len(self.log) - 1:
            colorful_print("Refusing append entries request: log not up-to-date", "warning")
            colorful_print(f" Must have {request.prev_log_index}, have {len(self.log) - 1}", "warning")
            await self.connector.send_message(node, AppendEntriesReply(self.current_term, False,
                                                                       request.entries is None))
            return

        if request.prev_log_term != self.log[request.prev_log_index].term:
            colorful_print("Refusing append entries request: log conflict", "warning")
            await self.connector.send_message(node,
                                              AppendEntriesReply(self.current_term, False, request.entries is None))
            return

        # If an existing entry conflicts with a new one (same index but different terms),
        # delete the existing entry and all that follow it
        self.log = self.log[:request.prev_log_index + 1]

        # Append any new entries not already in the log
        if request.entries:
            self.log.append(request.entries)

        # If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
        if request.leader_commit_index > self.commit_index:
            self.commit_index = min(request.leader_commit_index, len(self.log) - 1)

        # TODO: If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine

        if request.entries is not None:
            colorful_print(f"Append entries request {str(len(self.log) - 1)} successful", "append")
        await self.connector.send_message(node, AppendEntriesReply(self.current_term, True, request.entries is None))

    # =================================================================================================================

    # Reacting to client's reply to leader's command to append entries
    async def respond_to_append_entries_reply(self, node: Node, reply: AppendEntriesReply):
        if reply.success:
            self.next_index[node] = min(self.next_index[node] + 1, len(self.log))
            self.match_index[node] = min(self.match_index[node] + 1, len(self.log) - 1)
            if not reply.request_empty:
                colorful_print(f"Append request to {str(node)} successful", "append")

                votes_required = (len(self.connector.nodes) + 1) / 2
                votes_gained = 1
                for index in self.next_index.values():
                    # print(index)
                    if (index == len(self.log)) or (index <= self.commit_index):
                        votes_gained += 1

                colorful_print(f"{str(votes_gained)} out of {str(votes_required)} applied", "vote")
                if votes_gained > votes_required:
                    colorful_print(f"Committing", "append")
                    # TODO: apply commit, including
                    self.commit_index = len(self.log) - 1
            return
        else:
            colorful_print(f"Append request to {str(node)} unsuccessful", "warning")
            if reply.term > self.current_term:
                colorful_print("reply term", "warning")
                self.update_term(reply.term)
                return

            self.match_index[node] = 0
            self.next_index[node] -= 1

            await self.send_append_entries_request()

    # =================================================================================================================

    async def send_request_from_client(self, message):
        if not self.role == Role.FOLLOWER:
            colorful_print("--- send_request_from_client not by client!!!", "error")
            return
        await self.connector.send_message(self.leader, RequestFromClient(self.current_term, message))

    # Reacting to client's command with new entry
    async def respond_to_request_from_client(self, node: Node, request: RequestFromClient):
        if not self.role == Role.LEADER:
            colorful_print("--- respond_to_request_from_client not by leader!!!", "error")
            return

        # append entry to local log
        colorful_print("Received from " + str(node) + " : " + str(request.data), "append")

        new_entry = LogEntry(self.current_term, request.data, len(self.log))
        self.log.append(new_entry)

        await self.send_append_entries_request()

    # =================================================================================================================

    def print_log(self):
        print("LOG:")
        for i in range(1, len(self.log)):
            print(f"-- {i} {str(self.log[i])} (committed: {i <= self.commit_index})")
