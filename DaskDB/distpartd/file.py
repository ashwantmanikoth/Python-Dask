from __future__ import absolute_import

import atexit
import os
import shutil
import string
import tempfile

from .core import Interface
import locket
from .utils import ignoring


class File(Interface):
    def __init__(self, path=None, dir=None, hdfs_node=None, hdfs_port=None):
        if not path:
            path = tempfile.mkdtemp(suffix='.partd', dir=dir)
            cleanup_files.append(path)
            self._explicitly_given_path = False
        else:
            self._explicitly_given_path = True
        # if not path.startswith('hdfs://'):
        #     raise ValueError('HDFS path must start with hdfs://')
        self.is_hdfs_path = True
        self.path = path[7:]

        from hdfs3 import HDFileSystem
        hdfs=HDFileSystem(host=hdfs_node,port=hdfs_port)
        self.hdfs = hdfs
        if self.hdfs.exists(self.path) :
            self.hdfs.rm(self.path, recursive=True)
        self.hdfs.mkdir(self.path)
                
#        self.lock = locket.lock_file(self.filename('.lock'))
        Interface.__init__(self)

    def __getstate__(self):
        return {'path': self.path}

    def __setstate__(self, state):
        Interface.__setstate__(self, state)
        File.__init__(self, state['path'])

    def append(self, data, lock=True, fsync=False, **kwargs):
        #if lock: self.lock.acquire()
        for k, v in data.items():
            fn = self.filename(k)
            if not self.hdfs.exists(os.path.dirname(fn)):
                self.hdfs.mkdir(os.path.dirname(fn))
            with self.hdfs.open(fn, 'ab') as f:
                f.write(v)
        #if lock: self.lock.release()            

    def _get(self, keys, lock=True, **kwargs):
        assert isinstance(keys, (list, tuple, set))
#       if lock:
#           self.lock.acquire()
        result = []
        for key in keys:
            with self.hdfs.open(self.filename(key), 'rb') as f:
                result.append(f.read())
#       if lock:
#           self.lock.release()
                        
        return result

    def _iset(self, key, value, lock=True):
        """ Idempotent set """
        fn = self.filename(key)
        if not self.hdfs.exists(os.path.dirname(fn)) :
            self.hdfs.mkdir(os.path.dirname(fn))
#         if lock:
#             self.lock.acquire()

        with self.hdfs.open(self.filename(key), 'wb') as f:
                f.write(value)
#         if lock:
#             self.lock.release()
                
                    
    def _delete(self, keys, lock=True):
        if lock:
            self.lock.acquire()
        try:
            for key in keys:
                path = filename(self.path, key)
                if os.path.exists(path):
                    os.remove(path)
        finally:
            if lock:
                self.lock.release()

    def drop(self):
        if os.path.exists(self.path):
            shutil.rmtree(self.path)
        self._iset_seen.clear()
        os.mkdir(self.path)

    def filename(self, key):
        return filename(self.path, key)

    def __exit__(self, *args):
        self.drop()
        os.rmdir(self.path)

    def __del__(self):
        if not self._explicitly_given_path:
            self.drop()
            os.rmdir(self.path)


def filename(path, key):
    return os.path.join(path, escape_filename(token(key)))


# http://stackoverflow.com/questions/295135/turn-a-string-into-a-valid-filename-in-python
valid_chars = "-_.() " + string.ascii_letters + string.digits + os.path.sep


def escape_filename(fn):
    """ Escape text so that it is a valid filename

    >>> escape_filename('Foo!bar?')
    'Foobar'

    """
    return ''.join(filter(valid_chars.__contains__, fn))



def token(key):
    """

    >>> token('hello')
    'hello'
    >>> token(('hello', 'world'))  # doctest: +SKIP
    'hello/world'
    """
    if isinstance(key, str):
        return key
    elif isinstance(key, tuple):
        return os.path.join(*map(token, key))
    else:
        return str(key)


cleanup_files = list()

@atexit.register
def cleanup():
    for fn in cleanup_files:
        if os.path.exists(fn):
            shutil.rmtree(fn)
