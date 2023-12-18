import asyncio
import random


class Timer:
    def __init__(self, callback):
        self.callback = callback
        self.task = None

    async def go(self):
        timeout = 2 + random.random() * 10
        # print("Timer for: " + str(timeout))
        await asyncio.sleep(timeout)
        print("Timer ran out!")
        self.callback()

    def stop(self):
        if self.task:
            self.task.cancel()

    def start(self):
        self.task = asyncio.create_task(self.go())

    def reset(self):
        self.stop()
        self.start()
