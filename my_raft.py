from node_connector import *
from my_timer import Timer

from enum import Enum, auto


class Role(Enum):
    CANDIDATE = auto()
    FOLLOWER = auto()
    LEADER = auto()


# class RequestVote:
#     def __init__(self, candidate_node: Node, term: int):
#         self.candidate_node = candidate_node
#         self.term = term
#
#     def __str__(self):
#         return "RequestVote from " + str(self.candidate_node) + ", term " + str(self.term)
#         return "RequestVote, term " + str(self.term)


class VoteRequest:
    def __init__(self, term: int):
        self.term = term

    def __str__(self):
        return "VoteRequest, term " + str(self.term)


class VoteReply:
    def __init__(self, term: int, granted: bool):
        self.term = term
        self.granted = granted

    def __str__(self):
        return "VoteReply, term " + str(self.term) + ": " + str(self.granted)


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

    async def start(self):
        await self.connector.start()
        self.become_follower()

    def become_follower(self):
        self.role = Role.FOLLOWER
        print("I'm a follower!")
        self.timer.start()

    def become_candidate(self):
        self.role = Role.CANDIDATE
        self.current_term += 1
        print(f'\nStarting election... term = {self.current_term} ')
        self.votes_gained = 1

        # request_vote = RequestVote(self.node, self.current_term)
        request_vote = VoteRequest(self.current_term)

        asyncio.create_task(self.connector.send_message_to_everyone(request_vote))

        self.timer.start()

    def become_leader(self):
        self.role = Role.LEADER
        print("I'm a LEADER!")
        self.timer.stop()

    async def react_to_message(self, node: Node, message):
        if isinstance(message, VoteRequest):
            await self.respond_to_vote_request(node, message)
        if isinstance(message, VoteReply):
            await self.respond_to_vote_reply(node, message)
        else:
            print("Message received from " + str(node) + ": " + str(message))

    async def respond_to_vote_request(self, candidate_node: Node, request: VoteRequest):
        print("Vote request from " + str(candidate_node))

        if (request.term < self.current_term) or (self.voted_for and self.voted_for != candidate_node):
            vote_reply = VoteReply(self.current_term, False)
        else:
            if request.term > self.current_term:
                self.update_term(request.term)
            vote_reply = VoteReply(self.current_term, True)
            self.voted_for = candidate_node

        print("Replying to " + str(candidate_node) + ": " + str(vote_reply.granted))
        asyncio.create_task(self.connector.send_message(candidate_node, vote_reply))

    async def respond_to_vote_reply(self, candidate_node: Node, reply: VoteReply):
        print("Vote reply from " + str(candidate_node) + ": " + str(reply))

        if reply.granted:
            self.votes_gained += 1

        if self.votes_gained >= (len(self.connector.nodes) + 1) / 2:
            self.become_leader()

    def update_term(self, new_term):
        print("Updating term " + str(self.current_term) + " -> " + str(new_term))
        self.current_term = new_term
        self.become_follower()
