"""Functions for interacting with the HDFS."""

import pyarrow as pa  # interact with the HDFS
import fnmatch  # for filtering
import subprocess  # execute shell commands


def run_cmd(args_list, shell=False):
    """
    Run shell commands.

    Inputs
    ------
    args_list : list of arguments to be run.
    shell : to pass additional arguments in one string instead of list.

    Output
    ------
    Triplet of returned code, output and errors.
    """
    # info
    args_string = format(' '.join(args_list))
    print('Running system command: '+args_string)
    # transform into string for shell
    if shell:
        args_list = args_string
    # execute
    proc = subprocess.Popen(args_list, shell=shell, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    s_output, s_err = proc.communicate()
    s_return = proc.returncode
    return s_return, s_output, s_err


def flatten(container):
    """
    Helper function for flattening list of arbitrary nesting.

    Inputs
    ------
    container : Nested list.

    Output
    ------
    Unnested/Flattened list.
    """
    for i in container:
        if isinstance(i, (list, tuple)):
            for j in flatten(i):
                yield j
        else:
            yield i


def r_search(fs, flist):
    """
    Helper function for recursively searching the HDFS.

    Inputs
    ------
    fs : Filesytem options created by pyarrow package.
    flist : List of potential files.

    Output
    ------
    List of files.
    """
    # flatten if input is nested list
    flist = list(flatten(flist))

    # get type of first element and if there are more than 1 element
    itype = fs.info(flist[0])['kind']
    remainder = len(flist) > 1

    # if element is file, append and r_search the rest if remainder
    if itype == 'file':
        if remainder:
            return list(flatten([flist[0], r_search(fs, flist[1:])]))
        else:
            return flist[0]

    # if directory, get files in that directory and append to r_search list
    elif itype == 'directory':
        leveldown = fs.ls(flist[0])
        if remainder:
            return list(flatten(r_search(fs, [leveldown, flist[1:]])))
        elif len(leveldown) > 1:
            return r_search(fs, leveldown)
        else:
            return []

    # if neither, ignore. And if that was the last element, return empty list
    else:
        if remainder:
            return r_search(fs, flist[1:])
        else:
            return []


def ls_hdfs(path, pattern='*', recursive=False):
    """
    List files (or directories) in given path inside HDFS.

    Inputs
    ------
    path : Path inside HDFS.
    entry_type : Type of entry to filter for in the end, e.g. '.csv'.
    recursive : Set to True to search in subfolders as well.

    Output
    ------
    List of files/directories.
    """
    # connect to HDFS
    fs = pa.hdfs.connect()
    # go recursively into each subfolder to extract files
    if recursive:
        file_list = list(flatten(r_search(fs, [path])))
    # list files in given path
    else:
        file_list = fs.ls(path)
    # filter for given file_type
    return fnmatch.filter(file_list, pattern)
