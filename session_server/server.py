import asyncio
import logging
import signal
from contextlib import suppress

from .storage import Storage
from .storage_mongodb import StorageMongoDB
from .webserver import WebServer
from .wsserver import WSServer

class Server:
    WEB_PORT = 8080
    WS_PORT = 8089

    def __init__(self, files_dir):
        self._running = False
        self._storage = Storage(StorageMongoDB(files_dir))
        self._ws_server = WSServer(self, self.WS_PORT)
        self._web_server = WebServer(self, self.WEB_PORT)
        # Enable to add testing data to storage
        # self._storage.add_testing()

    def start(self):
        self._running = True
        loop = asyncio.get_event_loop()
        for signame in ('SIGINT', 'SIGTERM'):
            loop.add_signal_handler(getattr(signal, signame), self.stop)
        try:
            self._start_server()
            loop.run_forever()
            pending = asyncio.Task.all_tasks()
            for task in pending:
                task.cancel()
                # Await task to execute its cancellation
                with suppress(asyncio.CancelledError):
                    loop.run_until_complete(task)
        finally:
            loop.close()

    def stop(self):
        self._web_server.stop()
        self._ws_server.stop()
        self._running = False
        asyncio.get_event_loop().stop()

    @property
    def storage(self):
        return self._storage

    @property
    def web_server(self):
        return self._web_server

    @property
    def ws_server(self):
        return self._ws_server

    def _start_server(self):
        self._web_server.start()
        self._ws_server.start()
