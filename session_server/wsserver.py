import asyncio
import json
import logging
import websockets

class WSServer:
    HOST = '0.0.0.0'

    def __init__(self, server, port, loop=None):
        self._server = server
        self._storage = server.storage
        self._port = port
        self._loop = loop or asyncio.get_event_loop()
        self._ws_server = None
        self._clients = []

    def start(self):
        logging.info('Starting WebSockets server on %s:%d', self.HOST, self._port)
        serve = websockets.serve(self._handler, self.HOST, self._port, loop=self._loop)
        # Wait for server to start
        self._ws_server = self._loop.run_until_complete(serve)

    def stop(self):
        if self._ws_server is not None:
            self._ws_server.close()
            self._ws_server = None
            logging.info('WebSockets server stopped')

    @property
    def port(self):
        return self._port

    async def broadcast_item_added(self, data, exclude=None):
        await self.broadcast_event('ITEM_ADDED', data, exclude)

    async def broadcast_item_moved(self, data, exclude=None):
        await self.broadcast_event('ITEM_MOVED', data, exclude)

    async def broadcast_item_removed(self, uid, exclude=None):
        await self.broadcast_event('ITEM_REMOVED', {'Uid': uid}, exclude)

    async def broadcast_session_added(self, data, exclude=None):
        await self.broadcast_event('SESSION_ADDED', data, exclude)

    async def broadcast_session_removed(self, uid, exclude=None):
        await self.broadcast_event('SESSION_REMOVED', {'Uid': uid}, exclude)

    async def broadcast_event(self, event, data, exclude=None):
        if not self._clients or len(self._clients) == 1 and self._clients[0] == exclude:
            return
        futures = []
        for client in self._clients:
            if client != exclude:
                message = json.dumps({'Event': event, 'Seq': client.msg_seq, **data})
                futures.append(client.send(message))
                client.msg_seq += 1
        if futures:
            await asyncio.wait(futures)

    async def broadcast_message(self, message, exclude=None):
        if not self._clients or len(self._clients) == 1 and self._clients[0] == exclude:
            return
        futures = []
        for client in self._clients:
            if client != exclude:
                message['Seq'] = client.msg_seq
                futures.append(client.send(json.dumps(message)))
                client.msg_seq += 1
        if futures:
            await asyncio.wait(futures)


    async def _handler(self, websocket, path):
        host, port = websocket.remote_address
        logging.debug(f'WS connection from {host}:{port}')
        websocket.msg_seq = 1
        self._clients.append(websocket)
        while True:
            try:
                # Try to receive a message from the client, this throws when
                # the client has disconnected
                message = await websocket.recv()
            except:
                logging.exception('WebSocket recv()')
                break
            try:
                message = json.loads(message)
            except:
                logging.debug(f'Failed to decode: {message}')
                message = None
            if message is not None:
                if self._process_message(message, websocket):
                    # Broadcast to other clients
                    await self.broadcast_message(message, websocket)
        logging.debug(f'WS client {host}:{port} disconnected')
        self._clients.remove(websocket)
        moves = self._storage.deselect_all_ident_objects(websocket)
        for uid, move in moves.items():
            await self.broadcast_item_moved({
                'Uid': uid,
                'Position': move[0],
                'Scale': move[1],
                'Rotation': move[2]
            })

    def _process_message(self, data, websocket):
        if 'Event' not in data or 'Uid' not in data:
            return False

        event, uid = data['Event'], data['Uid']
        if event == 'ITEM_ADDED':
            # Adding objects through WebSockets only works for non-file
            # objects, in any case clients can add using HTTP requests
            return self._storage.add_object(data) is not None
        elif event == 'ITEM_MOVED':
            position, scale, rotation = None, None, None
            if 'Position' in data:
                position = data['Position']
            if 'Scale' in data:
                scale = data['Scale']
            if 'Rotation' in data:
                rotation = data['Rotation']
            return self._storage.move_object(uid, websocket, position, scale, rotation)
        elif event == 'ITEM_REMOVED':
            return self._storage.remove_object(data['Uid'])
        elif event == 'ITEM_SELECTION_CHANGED':
            if 'IsSelected' not in data:
                return False
            if data['IsSelected']:
                return self._storage.select_object(uid, websocket)
            else:
                return self._storage.deselect_object(uid, websocket)
        elif event == 'SESSION_ADDED':
            return self._storage.add_session(data) is not None
        elif event == 'SESSION_REMOVED':
            return self._storage.remove_session(data['Uid'])
        else:
            # Unknown message
            return False
