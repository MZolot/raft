from node_connector import *
from my_timer import Timer

from enum import Enum, auto


class Role(Enum):
    CANDIDATE = auto()
    FOLLOWER = auto()
    LEADER = auto()


class VoteRequest:
    def __init__(self,
                 term: int,
                 last_log_index: int,
                 last_log_term: int
                 ):
        self.term = term
        self.last_log_index = last_log_index
        self.last_log_term = last_log_term

    def __str__(self):
        return "VoteRequest, term " + str(self.term)


class VoteReply:
    def __init__(self, term: int, granted: bool):
        self.term = term
        self.granted = granted

    def __str__(self):
        return "VoteReply, term " + str(self.term) + ": " + str(self.granted)


class RequestFromClient:
    def __init__(self, term: int, data):
        self.term = term
        self.data = data

    def __str__(self):
        return "RequestFromClient, term " + str(self.term) + ": " + str(self.data)


class LogEntry:
    def __init__(self, term: int, data, index: int):
        self.term = term
        self.data = data
        self.index = index

    def __str__(self):
        return "LogEntry, term " + str(self.term) + ": " + str(self.data)


class AppendEntriesRequest:
    def __init__(self,
                 term: int,
                 entries: LogEntry | None,
                 prev_log_index: int,
                 prev_log_term: int,
                 leader_commit_index: int
                 ):
        self.term = term
        self.entries = entries
        self.prev_log_index = prev_log_index
        self.prev_log_term = prev_log_term
        self.leader_commit_index = leader_commit_index

    def __str__(self):
        return ("AppendEntriesRequest, term " +
                str(self.term) +
                ", log state: [" +
                str(self.prev_log_index) + ", " +
                str(self.prev_log_term) + ", " +
                str(self.leader_commit_index) + "]" +
                ": " + str(self.entries))


class AppendEntriesReply:
    def __init__(self, term: int, success: bool):
        self.term = term
        self.success = success

    def __str__(self):
        return "AppendEntriesRequest, term " + str(self.term) + ": " + str(self.success)


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

    async def start(self):
        await self.connector.start()
        self.become_follower()

    def become_follower(self):
        self.role = Role.FOLLOWER
        print("I'm a follower!")
        self.voted_for = None
        self.timer.reset()

    def become_candidate(self):
        self.role = Role.CANDIDATE
        self.current_term += 1
        print(f'\nStarting election... term = {self.current_term} ')
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
        print("I'm a LEADER!")
        self.timer.stop()
        self.leader = self.node
        self.voted_for = None

        self.next_index = {node: len(self.log) for node in self.connector.nodes}
        self.match_index = {node: -1 for node in self.connector.nodes}

        for node in self.connector.nodes:
            asyncio.create_task(self.send_heartbeat(node))

        # request = AppendEntriesRequest(self.current_term, None)
        # asyncio.create_task(self.connector.send_message_to_everyone(request))

    async def react_to_message(self, node: Node, message):
        if isinstance(message, VoteRequest):
            await self.respond_to_vote_request(node, message)
        elif isinstance(message, VoteReply):
            await self.respond_to_vote_reply(node, message)
        elif isinstance(message, AppendEntriesRequest):
            await self.respond_to_append_entries_request(node, message)
        elif isinstance(message, RequestFromClient):
            await self.respond_to_request_from_client(node, message)
        elif isinstance(message, bool):
            pass
        else:
            print("Unknown message received from " + str(node) + ": " + str(message) + " -- " + str(type(message)))

    async def respond_to_vote_request(self, candidate_node: Node, request: VoteRequest):
        print("Vote request from " + str(candidate_node))

        if request.term < self.current_term:
            # Reply false if term < currentTerm
            vote_reply = VoteReply(self.current_term, False)
            print("-- wrong term")

            print("Replying to " + str(candidate_node) + ": " + str(vote_reply.granted))
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
                print("-- log problem")
                print(str(request.last_log_term > self.log[-1].term))
                print(str(request.last_log_term == self.log[-1].term))
                print(str(request.last_log_index > len(self.log)))
                vote_reply = VoteReply(self.current_term, False)
        else:
            print("-- already voted for someone else")
            vote_reply = VoteReply(self.current_term, False)

        print("Replying to " + str(candidate_node) + ": " + str(vote_reply.granted))
        print()
        asyncio.create_task(self.connector.send_message(candidate_node, vote_reply))

    async def respond_to_vote_reply(self, candidate_node: Node, reply: VoteReply):
        print("Vote reply from " + str(candidate_node) + ": " + str(reply))
        self.update_term(reply.term)

        if reply.granted:
            self.votes_gained += 1

        votes_required = (len(self.connector.nodes) + 1) / 2
        print("Gained " + str(self.votes_gained) + " out of " + str(votes_required))
        if self.votes_gained > votes_required:
            self.become_leader()

    def update_term(self, new_term):
        if new_term > self.current_term:
            print("Updating term " + str(self.current_term) + " -> " + str(new_term))
            self.current_term = new_term
            self.become_follower()

    async def send_heartbeat(self, node: Node):
        print("Heart starts beating for " + str(node))
        while self.role == Role.LEADER:
            request = AppendEntriesRequest(
                self.current_term,
                None,
                self.log[-1].term,
                len(self.log) - 1,
                self.commit_index
            )
            await self.connector.send_message(node, request)
            # print("bip " + str(node))
            await asyncio.sleep(2)

    async def send_append_entries_request(self, entries):
        if not self.role == Role.LEADER:
            print("--- send_append_entries_request not by leader!!!")
            return

        request = AppendEntriesRequest(
            self.current_term,
            entries,
            self.log[-1].term,
            len(self.log) - 1,
            self.commit_index
        )
        await self.connector.send_message_to_everyone(request)

    # Reacting to leader's command to append entries
    async def respond_to_append_entries_request(self, node: Node, request: AppendEntriesRequest):
        # Reply false if term < currentTerm
        if request.term < self.current_term:
            await self.connector.send_message(node, False)
            return
        self.update_term(request.term)
        self.timer.reset()

        if self.role == Role.CANDIDATE:
            self.become_follower()
        self.leader = node

        print("Append entries request from " + str(node) + ": " + str(request))

        # TODO: If commitIndex > lastApplied: increment lastApplied, apply
        #  log[lastApplied] to state machine

        # Reply to heartbeat
        if not request.entries:
            await self.connector.send_message(node, True)
            return

        # Reply false if log does not contain an entry at prevLogIndex
        #  whose term matches prevLogTerm
        if request.prev_log_term != self.log[request.prev_log_index].term:
            await self.connector.send_message(node, False)
            return

        # TODO: If an existing entry conflicts with a new one (same index but different terms),
        #  delete the existing entry and all that follow it

        # Append any new entries not already in the log
        self.log.append(request.entries)

        # If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
        if request.leader_commit_index > self.commit_index:
            self.commit_index = min(request.leader_commit_index, len(self.log) - 1)

    async def send_request_from_client(self, message):
        if not self.role == Role.FOLLOWER:
            print("--- send_request_from_client not by client!!!")
            return
        await self.connector.send_message(self.leader, RequestFromClient(self.current_term, message))

    # Reacting to client's command with new entry
    async def respond_to_request_from_client(self, node: Node, request: RequestFromClient):
        if not self.role == Role.LEADER:
            print("--- respond_to_request_from_client not by leader!!!")
            return

        # append entry to local log
        new_entry = LogEntry(self.current_term, request.data, len(self.log))
        self.log.append(new_entry)

        # applying entry to state machine
        # asyncio.create_task(self.send_append_entries_request(new_entry))
