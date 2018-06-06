# shared-session-server

Server part for shared workspace session. It allows connected clients to store and retrive shared objects and automatically notifies other connected clients about changes.

The server/client interaction happens through a WebSockets connection, while regular HTTP is used for data-heavy transfers.

This program is written in Python 3.6 and uses MongoDB database.
