import asyncio


class AsyncQueue(asyncio.Queue):
    """An asyncio queue that supports putting items at the front."""
    async def put_front(self, item):
        """Put an item into the front of the queue.

        If the queue is full, wait until a free slot is available.
        """
        while self.full():
            putter = self._get_loop().create_future()
            self._putters.append(putter)
            try:
                await putter
            except Exception:
                putter.cancel()
                try:
                    self._putters.remove(putter)
                except ValueError:
                    pass
                if not self.full() and not putter.cancelled():
                    self._wakeup_next(self._putters)
                raise

        return self.put_front_nowait(item)


    def put_front_nowait(self, item):
        """Put an item into the front of the queue without blocking.

        Raise QueueFull if no free slot is available.
        """
        if self.full():
            raise asyncio.QueueFull
        self._queue.appendleft(item)
        self._wakeup_next(self._getters)