import sys
from aioconsole import ainput
from my_raft import *
from node_connector import Node


async def main():
    ports = list(map(int, sys.argv[1:]))
    print(ports)
    my_node = Node('127.0.0.1', ports[0])
    nodes = [Node('127.0.0.1', port) for port in ports[1:]]

    raft = Raft(my_node, nodes)
    await raft.start()

    while True:
        message = await ainput()
        if message == "log":
            raft.print_log()
        elif raft.role == Role.FOLLOWER:
            await raft.send_request_from_client(message)
        elif raft.role == Role.LEADER:
            new_entry = LogEntry(raft.current_term, message, len(raft.log))
            raft.log.append(new_entry)
            await raft.send_append_entries_request()
        await asyncio.sleep(1)


if __name__ == '__main__':
    asyncio.run(main())
