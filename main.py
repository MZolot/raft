import asyncio
import sys
from aioconsole import ainput

from raft import RaftServer
from my_raft import Raft
from node_connector import Node, NodeConnector


async def main():
    ports = list(map(int, sys.argv[1:]))
    # ports = [1234, 4321, 1477, 1447, 1147]
    print(ports)
    my_node = Node('127.0.0.1', ports[0])
    nodes = [Node('127.0.0.1', port) for port in ports[1:]]

    # node_connector = NodeConnector(my_node, nodes)
    # await node_connector.start()
    raft = Raft(my_node, nodes)
    await raft.start()
    # print('Transport started')
    # await raft.done_running.wait()

    while True:
        # index, message = (await ainput()).split()
        # await raft.send_message(nodes[int(index)], message)
        # await raft.connector.send_message(nodes[int(index)], message)
        await raft.connector.send_message_to_everyone("hiii :3")
        print('Message sent!')
        await asyncio.sleep(10)


if __name__ == '__main__':
    asyncio.run(main())
