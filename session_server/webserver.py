import asyncio
import json
import logging
import os
import pathlib
import tempfile

from aiohttp import web

class WebServer:
    HOST = '0.0.0.0'

    _ROUTES_GET = (
        {'url': '/item/all/{session}', 'handler': 'handle_item_all'},
        {'url': '/item/download/{uid}', 'handler': 'handle_item_download'},
        {'url': '/item/{uid}', 'handler': 'handle_item'},
        {'url': '/session/all', 'handler': 'handle_session_all'},
        {'url': '/session/{uid}', 'handler': 'handle_session'})
    _ROUTES_POST = (
        {'url': '/item/add', 'handler': 'handle_item_add'},
        {'url': '/session/add', 'handler': 'handle_session_add'})
    _ROUTES_DELETE = (
        {'url': '/item/all/{session}', 'handler': 'handle_item_all'},
        {'url': '/item/{uid}', 'handler': 'handle_item'},
        {'url': '/session/{uid}', 'handler': 'handle_session'})

    def __init__(self, server, port, loop=None):
        self._server = server
        self._port = port
        self._loop = loop or asyncio.get_event_loop()
        self._web_app = web.Application()
        self._web_server = None
        self._get_handler = WebServerGETHandler(server)
        self._post_handler = WebServerPOSTHandler(server)
        self._delete_handler = WebServerDELETEHandler(server)
        self._setup_routes()

    @property
    def port(self):
        return self._port

    def start(self):
        logging.info('Starting HTTP server on %s:%d', self.HOST, self._port)
        handler = self._web_app.make_handler()
        self._web_server = self._loop.create_server(handler, self.HOST, self._port)
        self._loop.create_task(self._web_server)

    def stop(self):
        if self._web_server is not None:
            self._web_server.close()
            self._web_server = None
            logging.info('HTTP server stopped')

    def _setup_routes(self):
        router = self._web_app.router
        for route in self._ROUTES_GET:
            router.add_get(route['url'],
                           getattr(self._get_handler, route['handler']))
        for route in self._ROUTES_POST:
            router.add_post(route['url'],
                            getattr(self._post_handler, route['handler']))
        for route in self._ROUTES_DELETE:
            router.add_delete(route['url'],
                              getattr(self._delete_handler, route['handler']))

class WebServerGETHandler:
    def __init__(self, server):
        self._server = server
        self._storage = server.storage

    async def handle_item(self, req):
        data = self._storage.get_object(req.match_info['uid'])
        if data is not None:
            return web.json_response({'data': data})
        else:
            return web.HTTPNotFound(text='Object not found')

    async def handle_item_download(self, req):
        file_path = self._storage.get_object_file(req.match_info['uid'])
        if file_path is not None:
            return web.FileResponse(pathlib.Path(file_path))
        else:
            return web.HTTPNotFound(text='Object not found')

    async def handle_item_all(self, req):
        session = req.match_info['session']
        data = self._storage.get_all_objects(session)
        return web.json_response({'data': data})

    async def handle_session(self, req):
        data = self._storage.get_session(req.match_info['uid'])
        if data is not None:
            return web.json_response({'data': data})
        else:
            return web.HTTPNotFound(text='Object not found')

    async def handle_session_all(self, req):
        data = self._storage.get_all_sessions()
        return web.json_response({'data': data})

class WebServerPOSTHandler:
    TEXT_FIELDS = ('Uid', 'Session', 'ObjectType', 'FileName', 'Url', 'Text')
    JSON_FIELDS = ('Position', 'Scale', 'Rotation')
    FILE_TYPES  = ('File')

    def __init__(self, server):
        self._server = server
        self._storage = server.storage
        self._ws_server = server.ws_server

    async def handle_item_add(self, req):
        if req.has_body and req.content_type == 'multipart/form-data':
            reader = await req.multipart()
            data = {}
            file_name, temp_path = None, None

            while True:
                field = await reader.next()
                if field is None:
                    break
                name = field.name
                if name in self.JSON_FIELDS:
                    try:
                        data[name] = json.loads(await field.text())
                    except json.JSONDecodeError:
                        if temp_path is not None:
                            os.unlink(temp_path)
                        return web.HTTPBadRequest(text='Invalid field: ' + name)
                elif name in self.TEXT_FIELDS:
                    data[name] = await field.text()
                    if name == 'FileName':
                        file_name = data[name]
                elif data['ObjectType'] in self.FILE_TYPES and name == 'FileContent':
                    # Read the file content, if file name is not yet known,
                    # we take it from the request
                    if file_name is None:
                        file_name = field.filename
                    if file_name is None:
                        return web.HTTPBadRequest(text='File name unknown')
                    # https://docs.aiohttp.org/en/stable/web_quickstart.html#file-uploads
                    try:
                        temp_fd, temp_path = tempfile.mkstemp()
                        temp_fp = os.fdopen(temp_fd, mode='wb')
                        while True:
                            chunk = await field.read_chunk()  # 8192 bytes by default.
                            if not chunk:
                                break
                            temp_fp.write(chunk)
                        temp_fp.close()
                    except:
                        if temp_path is None:
                            logging.exception('Failed to create a temporary file')
                        else:
                            os.unlink(temp_path)
                            logging.exception(f'Failed to write file content to {temp_path}')
                        return web.HTTPServerError()
                else:
                    if temp_path is not None:
                        os.unlink(temp_path)
                    return web.HTTPBadRequest(text='Invalid field: ' + name)

            data = self._storage.add_object(data, temp_path)
            if data is not None:
                print(data)
                await self._ws_server.broadcast_item_added(data)
                return web.HTTPNoContent()
            else:
                if temp_path is not None:
                    os.unlink(temp_path)
                return web.HTTPBadRequest(text='Invalid object data')
        else:
            return web.HTTPBadRequest(text='Form data required')

    SESSION_TEXT_FIELDS = ('Uid', 'Name')
    SESSION_JSON_FIELDS = ()

    async def handle_session_add(self, req):
        if req.has_body and req.content_type == 'multipart/form-data':
            reader = await req.multipart()
            data = {}

            while True:
                field = await reader.next()
                if field is None:
                    break
                name = field.name
                if name in self.SESSION_JSON_FIELDS:
                    try:
                        data[name] = json.loads(await field.text())
                    except json.JSONDecodeError:
                        return web.HTTPBadRequest(text='Invalid field: ' + name)
                elif name in self.SESSION_TEXT_FIELDS:
                    data[name] = await field.text()
                else:
                    return web.HTTPBadRequest(text='Invalid field: ' + name)

            data = self._storage.add_session(data)
            if data is not None:
                await self._ws_server.broadcast_session_added(data)
                return web.HTTPNoContent()
            else:
                return web.HTTPBadRequest(text='Invalid object data')
        else:
            return web.HTTPBadRequest(text='Form data required')

class WebServerDELETEHandler:
    def __init__(self, server):
        self._server = server
        self._storage = server.storage
        self._ws_server = server.ws_server

    async def handle_item(self, req):
        uid = req.match_info['uid']
        if self._storage.remove_object(uid):
            await self._ws_server.broadcast_item_removed(uid)
            return web.HTTPNoContent()
        else:
            return web.HTTPNotFound(text='Object not found')

    async def handle_item_all(self, req):
        session = req.match_info['session']
        uids = self._storage.get_all_objects_uid_list(session)
        if self._storage.clear(session):
            for uid in uids:
                await self._ws_server.broadcast_item_removed(uid)
        return web.HTTPNoContent()

    async def handle_session(self, req):
        uid = req.match_info['uid']
        if self._storage.can_remove_session(uid):
            if self._storage.remove_session(uid):
                await self._ws_server.broadcast_session_removed(uid)
                return web.HTTPNoContent()
            else:
                return web.HTTPNotFound(text='Object not found')
        else:
            return web.HTTPForbidden(text='Session cannot be deleted')
