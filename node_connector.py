import asyncio
import pickle
import traceback
from asyncio import StreamReader, StreamWriter, Task
from dataclasses import dataclass
from colors import *


@dataclass(frozen=True, eq=True, order=True)
class Node:
    ip: str
    port: int


@dataclass
class Connection:
    reader: StreamReader
    writer: StreamWriter


async def on_connected(node: Node):
    colorful_print(f'Connected to {node}', "connect")


async def on_disconnected(node: Node):
    colorful_print(f'Disconnected from {node}', "connect")


async def on_message(node: Node, message):
    colorful_print(f'Got message from {node}: {message}', "connect")


class NodeConnector:
    server: asyncio.Server

    def __init__(self, self_node: Node, nodes: list[Node]):
        self.nodes = nodes
        self.on_node_disconnected_callback = on_disconnected
        self.on_node_connected_callback = on_connected
        self.on_message_received_callback = on_message
        self.self_node = self_node
        self.connections: dict[Node, Connection | None] = {node: None for node in self.nodes}
        self.running = True
        self.connection_keeper_tasks: list[Task] = []

    async def on_client_connected(self, reader: StreamReader, writer: StreamWriter):
        conn = Connection(reader, writer)
        await self.send_to_connection(conn, self.self_node)
        connected_node = await self.receive_from_connection(conn)
        if connected_node not in self.connections.keys():
            raise Exception('Подключился неизвестный узел!')
        self.connections[connected_node] = conn
        await self.on_node_connected_callback(connected_node)
        await self.connection_handler(connected_node, conn)

    async def connection_handler(self, node: Node, conn: Connection):
        while self.running:
            try:
                message = await self.receive_from_connection(conn)
                await self.on_message_received_callback(node, message)
            except Exception as e:
                print("============================")
                traceback.print_exception(e)
                print("============================\n")
                await self.handle_disconnect(node)
                break

    async def handle_disconnect(self, node: Node):
        conn = self.connections[node]
        conn.writer.close()
        self.connections[node] = None
        await self.on_node_disconnected_callback(node)

    async def send_message(self, node: Node, message):
        if self.connections[node] is None:
            # print('Trying to send message to disconnected node')
            return
        try:
            await self.send_to_connection(self.connections[node], message)
        except Exception:
            # traceback.print_exception(e)
            await self.handle_disconnect(node)

    async def send_message_to_everyone(self, message):
        for node in self.nodes:
            await self.send_message(node, message)

    def should_connect_to(self, node: Node):
        return node < self.self_node

    async def node_connection_keeper(self, node: Node):
        while self.running:
            if self.connections[node] is None:
                # print(f'Trying to connect to {node.port}')
                try:
                    reader, writer = await asyncio.open_connection(node.ip, node.port)
                except ConnectionRefusedError:
                    await asyncio.sleep(1)
                    continue
                conn = Connection(reader, writer)
                await self.send_to_connection(conn, self.self_node)
                connected_node = await self.receive_from_connection(conn)
                if node != connected_node:
                    raise Exception('Подключился неизвестный узел!')
                self.connections[node] = conn
                await self.on_node_connected_callback(connected_node)
                asyncio.ensure_future(self.connection_handler(node, conn))
            await asyncio.sleep(1)

    async def start(self):
        self.server = await asyncio.start_server(self.on_client_connected, self.self_node.ip, self.self_node.port)
        for node in self.nodes:
            if self.should_connect_to(node):
                task = asyncio.create_task(self.node_connection_keeper(node))
                self.connection_keeper_tasks.append(task)

    async def send_to_connection(self, conn: Connection, message):
        pickled = pickle.dumps(message)
        size = len(pickled).to_bytes(8, byteorder='little', signed=False)
        conn.writer.write(size + pickled)
        await conn.writer.drain()

    async def receive_from_connection(self, conn: Connection):
        message_len = int.from_bytes(await conn.reader.readexactly(8), byteorder='little', signed=False)
        pickled = await conn.reader.readexactly(message_len)
        return pickle.loads(pickled)

    def shutdown(self):
        for task in self.connection_keeper_tasks:
            task.cancel()
        self.server.close()
