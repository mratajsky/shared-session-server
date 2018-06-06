import logging
import os

class Storage:
    def __init__(self, engine):
        self._engine = engine
        # Object selection is handled here as it doesn't need to be stored
        # in the real storage
        self._selection = {}
        self._pending_move = {}

    ### Session API

    def add_session(self, uid, name):
        logging.debug(f'Adding session: {uid} : {name}')
        data = {'Uid': uid, 'Name': name}
        if self._engine.add_session(data):
            return data
        return None

    def get_session(self, uid):
        return self._engine.get_session(uid)

    def get_all_sessions(self, only_removable=False):
        if only_removable:
            return self._engine.get_all_sessions(['default'])
        else:
            return self._engine.get_all_sessions()

    def get_all_sessions_uid_list(self, only_removable=False):
        if only_removable:
            return self._engine.get_all_sessions_uid_list(['default'])
        else:
            return self._engine.get_all_sessions_uid_list()

    def can_remove_session(self, uid):
        return uid != 'default'

    def remove_session(self, uid):
        if uid == 'default':
            return False
        logging.debug(f'Removing session: {uid}')
        return self._engine.remove_session(uid)

    ### Object API

    TYPES = ('File', 'Link', 'Text')
    FILE_TYPES = ('File')
    BASIC_FIELDS = ('Uid', 'Session', 'ObjectType', 'Position', 'Scale', 'Rotation')
    EXTRA_FIELDS = {
        'File': ('FileName',),
        'Link': ('Url',),
        'Text': ('Text',),
    }
    ARRAY_FIELDS = {
        'Position': 3,
        'Scale': 3,
        'Rotation': 4
    }

    # Add object to the storage
    # Return the potentially modified data dictionary or None when failed
    def add_object(self, data, temp_file=None):
        logging.debug(f'Adding object: {data}')
        # Verify mandatory fields
        for field in self.BASIC_FIELDS:
            if field not in data:
                logging.info(f'Skipping object without basic field {field}')
                return None

        uid = data['Uid']
        if self.get_object(uid) is not None:
            logging.info(f'Skipping object {uid} which already exists')
            return None
        session = data['Session']
        if self.get_session(session) is None:
            logging.info(f'Skipping object {uid} with invalid session {session}')
            return None
        object_type = data['ObjectType']
        if object_type not in self.TYPES:
            logging.info(f'Skipping object of unknown type {object_type}')
            return None
        if object_type in self.FILE_TYPES and temp_file is None:
            logging.info(f'Skipping object without file content')
            return None
        for field, length in self.ARRAY_FIELDS.items():
            if field not in data:
                continue
            if not isinstance(data[field], list) or len(data[field]) != length:
                logging.info(f'Skipping object with invalid field {field}')
                return None

        # Cleanup object data
        data = {key:val for (key,val) in data.items() if
                    key in self.BASIC_FIELDS or
                    key in self.EXTRA_FIELDS[object_type]}

        if temp_file is not None:
            if object_type in self.FILE_TYPES:
                if logging.getLogger().isEnabledFor(logging.DEBUG):
                    name = data['FileName']
                    size = os.stat(temp_file).st_size
                    logging.debug(f'File name: {name}, size: {size}')
            else:
                logging.info(f'Skipping object {uid} with extraneous file')
                return None

        result = self._engine.add_object(data, temp_file)
        logging.debug(f'Result: {result}')
        if result:
            return data
        return None

    # Add testing data to the storage
    def add_testing(self):
        obj = {
            'Uid': '7e10441e-c88e-4d8c-9e6b-60cf96bbadc6',
            'Session': 'default',
            'ObjectType': 'Link',
            'Position': [0.0, 0.0, 0.0],
            'Scale': [1.0, 1.0, 1.0],
            'Rotation': [0.0, 0.0, 0.0, 0.0],
            'Url': 'http://localhost'}
        self.add_object(obj)
        obj = {
            'Uid': 'e87ecfcc-5bd2-4ff3-a4e9-179f52063471',
            'Session': 'default',
            'ObjectType': 'Text',
            'Position': [0.0, 0.0, 0.0],
            'Scale': [1.0, 1.0, 1.0],
            'Rotation': [0.0, 0.0, 0.0, 0.0],
            'Text': 'I hate C#'}
        self.add_object(obj)

    def get_object(self, uid):
        return self._engine.get_object(uid)

    def get_object_file(self, uid):
        return self._engine.get_object_file(uid)

    def get_all_objects(self, session):
        return self._engine.get_all_objects(session)

    def get_all_objects_uid_list(self, session):
        return self._engine.get_all_objects_uid_list(session)

    def clear(self, session):
        logging.debug(f'Removing all objects in session {session}')
        return self._engine.clear(session)

    def is_object_selected(self, uid, ident=None):
        if uid not in self._selection:
            return False
        if ident is not None:
            # Here only check if selected by the given ident
            return ident in self._selection[uid]
        else:
            return len(self._selection[uid]) > 0

    def move_object(self, uid, ident, position=None, scale=None, rotation=None):
        logging.debug(f'Moving object: {uid}, position={position}, scale={scale}, rotation={rotation}')
        if position is not None:
            if not isinstance(position, list) or len(position) != 3:
                logging.info('Not moving object with invalid position')
                return False
        if scale is not None:
            if not isinstance(scale, list) or len(scale) != 3:
                logging.info('Not moving object with invalid scale')
                return False
        if rotation is not None:
            if not isinstance(rotation, list) or len(rotation) != 4:
                logging.info('Not moving object with invalid rotation')
                return False
        if self.is_object_selected(uid):
            # Do not store the move in the engine if the object is selected,
            # wait until the last user deselects it
            if uid not in self._pending_move:
                self._pending_move[uid] = {}
            if ident not in self._pending_move[uid]:
                self._pending_move[uid][ident] = [None, None, None]
            logging.debug('Postponing move')
            if position is not None:
                self._pending_move[uid][ident][0] = position
            if scale is not None:
                self._pending_move[uid][ident][1] = scale
            if rotation is not None:
                self._pending_move[uid][ident][2] = rotation
            return True
        else:
            return self._engine.move_object(uid, position, scale, rotation)

    def remove_object(self, uid):
        logging.debug(f'Removing object: {uid}')
        result = self._engine.remove_object(uid)
        logging.debug(f'Result: {result}')
        return result

    def select_object(self, uid, ident):
        if uid not in self._selection:
            self._selection[uid] = set()
        self._selection[uid].add(ident)
        return True

    def deselect_object(self, uid, ident):
        try:
            if uid in self._selection:
                self._selection[uid].remove(ident)
                if not self._selection[uid]:
                    del self._selection[uid]
                if uid in self._pending_move and ident in self._pending_move[uid]:
                    # Move the object if this is the last deselecting client who
                    # moved the object
                    move = self._pending_move[uid].pop(ident)
                    if not self._pending_move[uid]:
                        del self._pending_move[uid]
                        logging.debug(f'Completing move: {uid} -> {move}')
                        result = self._engine.move_object(uid, *move)
                        if result:
                            return True, move
                return True, None
        except KeyError:
            return False, None

    def deselect_all_ident_objects(self, ident):
        moves = {}
        for uid in list(self._selection.keys()):
            result, move = self.deselect_object(uid, ident)
            if result and move is not None:
                moves[uid] = move
        # Return the moves done as a result of deselection
        return moves
