import logging
import pathlib

# Path to the file storage directory
FILES_DIR = pathlib.PurePath(__file__).parent.parent / 'files'

from .server import Server

def run():
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(message)s')
    server = Server(str(FILES_DIR))
    server.start()

if __name__ == '__main__':
    run()
