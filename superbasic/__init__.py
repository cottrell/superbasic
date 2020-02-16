import os
import json
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import joblib
import joblib.memory
from joblib._store_backends import FileSystemStoreBackend, StoreBackendBase, mkdirp

def write_parquet(df, dirname):
    print('writing {}'.format(dirname))
    table = pa.Table.from_pandas(df)
    # TODO consider swap to use dataset but then need the threadsafe to handle directories
    pq.write_to_dataset(table, root_path=dirname, partition_cols=None)
    # pq.write_table(table, filename)

def write_json(obj, dirname):
    # from . import hash
    # filename = os.path.join(dirname, hash(obj)) + '.json'
    filename = os.path.join(dirname, 'data') + '.json'
    print('writing {}'.format(filename))
    json.dump(obj, open(filename, 'w'))

def write_object(d, dirname):
    # handle all the cases, if dict, split out the pandas pieces
    # TODO: use zarr or bcolz for numpy arrays
    if isinstance(d, dict):
        pd_obj = {k: v for k, v in d.items() if isinstance(v, pd.DataFrame)}
        # TODO: consider numpy and bcolz or whatever you want for that
        json_obj = {k: v for k, v in d.items() if k not in pd_obj}
        json_obj['_pandas_placeholder_object'] = sorted(list(pd_obj.keys()))
        write_json(json_obj, dirname)
        for k, v in pd_obj.items():
            f = os.path.join(dirname, str(k))
            write_parquet(v, f)
    elif isinstance(d, pd.DataFrame):
        write_parquet(d, dirname)
    else:
        raise Exception('nip')

def read_object(dirname):
    filename = os.path.join(dirname, 'data') + '.json'
    if os.path.exists(filename):
        json_obj = json.load(open(filename))
        pd_obj = json_obj.pop('_pandas_placeholder_object', [])
        for k in pd_obj:
            json_obj[k] = pd.read_parquet(k)
        item = json_obj
    else:
        item = pd.read_parquet(filename)
    return item

class FileSystemStoreBackend2(FileSystemStoreBackend):
    def dump_item(self, path, item, verbose=1):
        """Dump an item in the store at the path given as a list of
           strings."""
        try:
            item_path = os.path.join(self.location, *path)
            if not self._item_exists(item_path):
                self.create_location(item_path)
            filename = os.path.join(item_path, 'output.pkl')
            if verbose > 10:
                print('Persisting in %s' % item_path)

            def write_func(to_write, dest_filename):
                os.makedirs(dest_filename) # actually a dir
                # with self._open_item(dest_filename, "wb") as f:
                #     # numpy_pickle.dump(to_write, f, compress=self.compress)
                write_object(to_write, dest_filename)

            self._concurrency_safe_write(item, filename, write_func)
        except Exception as e:  # noqa: E722
            " Race condition in the creation of the directory "
            raise e
    # def _concurrency_safe_write(self, to_write, filename, write_func):
    #     """Writes an object into a file in a concurrency-safe way."""
    #     temporary_filename = concurrency_safe_write(to_write,
    #                                                 filename, write_func)
    #     self._move_item(temporary_filename, filename)
    def load_item(self, path, verbose=1, msg=None):
        """Load an item from the store given its path as a list of
           strings."""
        full_path = os.path.join(self.location, *path)

        if verbose > 1:
            if verbose < 10:
                print('{0}...'.format(msg))
            else:
                print('{0} from {1}'.format(msg, full_path))

        mmap_mode = (None if not hasattr(self, 'mmap_mode')
                     else self.mmap_mode)

        filename = os.path.join(full_path, 'output.pkl')
        if not self._item_exists(filename):
            raise KeyError("Non-existing item (may have been "
                           "cleared).\nFile %s does not exist" % filename)

        # # file-like object cannot be used when mmap_mode is set
        # if mmap_mode is None:
        #     with self._open_item(filename, "rb") as f:
        #         item = numpy_pickle.load(f)
        # else:
        # item = numpy_pickle.load(filename, mmap_mode=mmap_mode)
        return read_object(filename)

joblib.memory.register_store_backend('superbasic', FileSystemStoreBackend2)

class SingleWriteMultiReadMemory():
    def __init__(self, location=None, readonly_locations=None, backend='superbasic', **kwargs):
        self.writeable_memory = joblib.memory.Memory(location=location, backend=backend, **kwargs)
        self.readonly_memory = list()
        for k in readonly_locations:
            self.readonly_memory.append(joblib.memory.Memory(location=k, backend=backend, **kwargs))
    def cache(self, func=None, ignore=None, verbose=None, mmap_mode=False, strategy='any'):
        return SingleWriteMultiReadMemorizedFunc(func, self.writeable_memory, readonly_memory=self.readonly_memory)

class SingleWriteMultiReadMemorizedFunc():
    def __init__(self, func, writeable_memory, readonly_memory=None):
        self.writeable_cache = writeable_memory.cache(func)
        readonly_memory = readonly_memory if readonly_memory is not None else []
        self.readonly_caches = [x.cache(func) for x in readonly_memory]
        self.func = func
        self.clear = self.writeable_cache.clear
    def __call__(self, *args, **kwargs):
        return self.call(*args, **kwargs)
    def check_call_in_cache(self, *args, **kwargs):
        for x in [self.writeable_cache] + self.readonly_caches:
            if x.check_call_in_cache(*args, **kwargs):
                return True
        return False
    def call(self, *args, **kwargs):
        for x in [self.writeable_cache] + self.readonly_caches:
            if x.check_call_in_cache(*args, **kwargs):
                return x.call(*args, **kwargs)
        return self.writeable_cache.call(*args, **kwargs)
    def call_and_shelve(self, *args, **kwargs):
        for x in [self.writeable_cache] + self.readonly_caches:
            if x.check_call_in_cache(*args, **kwargs):
                return x.call_and_shelve(*args, **kwargs)
        return self.writeable_cache.call_and_shelve(*args, **kwargs)
