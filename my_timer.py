import asyncio
import random


class Timer:
    def __init__(self, callback):
        self.callback = callback
        self.task = None

    async def go(self):
        timeout = 1 + random.random() * 10
        await asyncio.sleep(timeout)
        self.callback()

    def stop(self):
        self.task.cancel()

    def reset(self):
        self.stop()
        self.start()

    def start(self):
        self.task = asyncio.create_task(self.go())
