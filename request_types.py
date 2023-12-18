class LogEntry:
    def __init__(self, term: int, data, index: int):
        self.term = term
        self.data = data
        self.index = index

    def __str__(self):
        return "LogEntry, term " + str(self.term) + ": " + str(self.data)


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
