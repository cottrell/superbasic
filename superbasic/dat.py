#!/usr/bin/env python
"""
Experimental.

See datproject
"""
import os
import multiprocessing
import json
import subprocess
from .._utils import run_command_get_output, squish

_has_dat = None

def has_dat(force=False):
    global _has_dat
    if _has_dat is None or force:
        _has_dat = False
        res = run_command_get_output('type dat')
        if res['status'] == 0:
            _has_dat = True
    return _has_dat

_ = has_dat()

def _init_dat(title, description, dirname=_joblib_cache_base):
    if not os.path.exists(dirname):
        os.makedirs(dirname)
    cmd = "dat create --dir {}".format(dirname)
    print("initializing {} via cmd={}".format(dirname, cmd))
    # TODO: I think you need some sort of async to use multiple pipes at once so just don't bother for now
    # p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE, shell=True)
    p = subprocess.Popen(cmd, stdin=subprocess.PIPE, shell=True)
    p.stdin.write('{}\r{}\r'.format(title, description).encode())
    out, err = p.communicate()
    status = p.returncode
    res = dict(status=status, out=out, err=err)
    if status != 0:
        raise Exception('Error initializing dat {}'.format(res))
    return res

if _has_dat:
    if not os.path.exists(os.path.join(_joblib_cache_base, '.dat')):
        _init_dat('pandas_datareader joblib cache dat store',
                  'Experimental use of dat + joblib within pandas_datareader to share caches among users.')
else:
    print('dat is not installed or not in PATH. Consider installed datproject: npm install dat -g')

def dat_share(dirname=_joblib_cache):
    # fire and forget
    cmd = 'dat share {} &'.format(dirname)
    print('RUNNING: {}'.format(cmd))
    res = run_command_get_output(cmd)


# https://github.com/datproject/dat/issues/1054
_n_hash_truncation = 12
def _get_hash_to_dirnames():
    hashes = json.load(open(os.path.join(_mydir, 'dats.json')))['shared_dats']
    filename = os.path.expanduser('~/.pandas_datareader/dats.json')
    if os.path.exists(filename):
        hashes.extend(json.load(open(filename))['shared_dats'])
    hashes = list(squish(hashes))
    return {k: os.path.join(_dats, k[:_n_hash_truncation]) for k in hashes}

def dat_clone():
    return map_command_across_dat_dirs('dat clone {hash} {dirname}', only_if_not_exist=True)

def dat_pull():
    """ I think there is a bug in the cli and pull actually just syncs (hangs) ... put in background for now"""
    return map_command_across_dat_dirs('dat pull {dirname} &')

def dat_sync():
    """ run as daemons in the background, todo clean this up and offer kill method """
    return map_command_across_dat_dirs('dat sync {dirname}')

def map_command_across_dat_dirs(basecmd, only_if_not_exist=False):
    """
    Specify a cmd with hash and dirname string replacement
    """
    cmds = list()
    hashes = _get_hash_to_dirnames()
    for k, dirname in hashes.items():
        if os.path.exists(dirname) and only_if_not_exist:
            print('skipping {}'.format(k))
        else:
            cmd = basecmd.format(hash=k, dirname=dirname)
            print(cmd)
            cmds.append(cmd)
    pool = multiprocessing.pool.ThreadPool()
    results = list()
    results = [pool.apply_async(run_command_get_output, args=(cmd,)) for cmd in cmds]
    pool.close()
    pool.join()
    results = [x.get() for x in results]
    return results

if __name__ == '__main__':
    import argh
    argh.dispatch_commands([dat_clone, day_sync, dat_pull])
