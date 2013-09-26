import cache
from . import Storage

from cocaine.services import Service
from cocaine.exceptions import ServiceError, ChokeEvent

class CocaineStorage(Storage):
    def __init__(self, config):
        self._config = config
        self._storage = Service('storage')
        self.timeout = 120.0

    @cache.get
    def get_content(self, path):
        collection, separator, key = path.partition('/')
	print "============get_content", collection, key
	try:
            return self._storage.read(collection, key).get(timeout=self.timeout)
	except ServiceError as err:
	    raise IOError(err)

    @cache.put
    def put_content(self, path, content):
        collection, separator, key = path.partition('/')
        print "============put_content", collection, key
	try:
            self._storage.write(collection, key, content, ('docker',)).get(timeout=self.timeout)            
	except ChokeEvent:
	    pass	
        return path

    def stream_read(self, path):
        yield self.get_content(path)

    def stream_write(self, path, fp):
        chunks = []
        while True:
            try:
                buf = fp.read(self.buffer_size)
                if not buf:
                    break
                chunks += buf
            except IOError:
                break
        self.put_content(path, ''.join(chunks))

    def list_directory(self, path=None):
        if path is None:
            collection = 'images'
        else:
            collection, separator, key = path.partition('/')

        print "=================list", collection, path
        items = self._storage.find(collection, ('docker',)).get(timeout=self.timeout)
        if not items:
            raise OSError('No such directory: \'{0}\''.format(path))

        for item in items:
            yield item

    def exists(self, path):
        collection, separator, key = path.partition('/')
        print "=====================exists", collection, key
        return len(self._storage.find(collection, ('docker',)).get(timeout=self.timeout)) > 0

    @cache.remove
    def remove(self, path):
        collection, separator, key = path.partition('/')
        print "==========================remove", collection, key
        try:
	    self._storage.remove(collection, key).get(timeout=self.timeout)
	except ChokeEvent:
	    pass
	except ServiceError:
	    pass

    def get_size(self, path):
        return len(self.get_content(path))
