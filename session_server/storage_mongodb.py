import logging
import os
import pathlib
import shutil

import pymongo

class StorageMongoDB:
    def __init__(self, files_dir):
        self._files_dir = files_dir
        self._client = pymongo.MongoClient()
        self._session = self._client.database.session
        self._object = self._client.database.object
        try:
            self._session.create_index('Uid', unique=True, background=True)
            self._object.create_index('Uid', unique=True, background=True)
            self._object.create_index('Session', background=True)
        except:
            logging.exception('MongoDB setup failed')
        try:
            self._session.insert_one({'Uid': 'default', 'Name': 'default'})
        except pymongo.errors.DuplicateKeyError:
            pass
        except:
            logging.exception('MongoDB setup failed')

    ### Session API

    def add_session(self, data):
        try:
            self._session.insert_one(data)
            # The _id field is added automatically by MongoDB, but we don't
            # want it in the local data
            del data['_id']
            return True
        except:
            logging.exception('MongoDB error')
            return False

    def get_session(self, uid):
        try:
            return self._session.find_one({'Uid': uid}, {'_id': 0})
        except:
            logging.exception('MongoDB error')
            return None

    def get_all_sessions(self, skip=None):
        try:
            cond = {}
            if skip:
                cond['Uid'] = {'$nin': skip}
            return list(self._session.find(cond, {'_id': 0}))
        except:
            logging.exception('MongoDB error')
            return []

    def get_all_sessions_uid_list(self, skip=None):
        uids = []
        try:
            cond = {}
            if skip:
                cond['Uid'] = {'$nin': skip}
            for item in self._session.find(cond, {'_id': 0, 'Uid': 1}):
                uids.append(item['Uid'])
        except:
            logging.exception('MongoDB error')
        return uids

    def remove_session(self, uid):
        uid = os.path.basename(uid)
        try:
            result = self._session.delete_one({'Uid': uid})
            if result.deleted_count == 0:
                return False
            self._object.delete_many({'Session': uid})
        except:
            logging.exception('MongoDB error')
            return False
        try:
            # Delete files of the session
            path = pathlib.PurePath(self._files_dir) / uid
            if os.path.exists(path):
                logging.debug(f'Deleting {path}')
                shutil.rmtree(path)
            return True
        except:
            logging.exception(f'Delete error')
            return False

    ### Object API

    def add_object(self, data, temp_file=None):
        try:
            self._object.insert_one(data)
            # The _id field is added automatically by MongoDB, but we don't
            # want it in the local data
            del data['_id']
        except:
            logging.exception('MongoDB error')
            return False
        if temp_file is not None:
            # Move the temp file
            path = pathlib.PurePath(self._files_dir, data['Session'], data['Uid'])
            try:
                os.makedirs(path, exist_ok=True)
                shutil.move(temp_file, path / os.path.basename(data['FileName']))
            except:
                logging.exception(f'Failed to move {temp_file} to {path}')
                return False
        return True

    def get_object(self, uid):
        try:
            return self._object.find_one({'Uid': uid}, {'_id': 0})
        except:
            logging.exception('MongoDB error')
            return None

    def get_object_file(self, uid):
        data = self.get_object(uid)
        if not data or 'FileName' not in data:
            return None
        file_path = pathlib.PurePath(self._files_dir,
            data['Session'],
            data['Uid'],
            data['FileName'])
        if not os.path.exists(file_path):
            return None
        return file_path

    def get_all_objects(self, session):
        try:
            return list(self._object.find({'Session': session}, {'_id': 0}))
        except:
            logging.exception('MongoDB error')
            return []

    def get_all_objects_uid_list(self, session):
        uids = []
        try:
            for item in self._object.find({'Session': session}, {'_id': 0, 'Uid': 1}):
                uids.append(item['Uid'])
        except:
            logging.exception('MongoDB error')
        return uids

    def clear(self, session):
        try:
            self._object.delete_many({'Session': session})
        except:
            logging.exception('MongoDB error')
            return False
        try:
            # Delete files of the session
            path = pathlib.PurePath(self._files_dir) / session
            if os.path.exists(path):
                logging.debug(f'Deleting {path}')
                shutil.rmtree(path)
            return True
        except:
            logging.exception(f'Delete error')
            return False

    def move_object(self, uid, position=None, scale=None, rotation=None):
        update = {}
        if position is not None:
            update['Position'] = position
        if scale is not None:
            update['Scale'] = scale
        if rotation is not None:
            update['Rotation'] = rotation
        try:
            self._object.update_one({'Uid': uid}, {'$set': update})
            return True
        except:
            logging.exception('MongoDB error')
            return False

    def remove_object(self, uid):
        try:
            data = self._object.find_one_and_delete({'Uid': uid}, {'_id': 0})
        except:
            logging.exception('MongoDB error')
            return False
        if data is None:
            return False
        if 'FileName' in data:
            file_path = pathlib.PurePath(self._files_dir,
                data['Session'],
                data['Uid'],
                data['FileName'])
            try:
                logging.debug(f'Deleting {file_path}')
                os.unlink(file_path)
                os.removedirs(file_path.parent)
            except:
                logging.exception('Remove error')
        return True
