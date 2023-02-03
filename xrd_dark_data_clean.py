#!/usr/bin/python3
# vim: set fileencoding=utf-8 :
# Version 1.1.1
from os import path, readlink, remove, walk
from sys import argv, exit, stdout
from re import sub, escape, match
from subprocess import call

textwidth = 120


def cdnf(cdir):
    """This check if parameter is existing dir or it fails script"""
    if not path.isdir(cdir):
        exit(cdir + ' should be directory but it is not!')
    return


def clean_empty_dirs(directory_to_be_cleaned):
    """Finds (depth first) all empty dirs and deletes them"""
    call(['/bin/find', directory_to_be_cleaned, '-mindepth', '1', '-type', 'd', '-empty', '-delete'])


if __name__ == "__main__":
    old_args = ''
    if len(argv) < 3:
        old_args = '\nYou gave:\n   ' + ' '.join(argv)
        argv[1:] = ['-h']

    if ('-h' in argv) or ('--help' in argv):
        print('This script is to be used in this way:')
        print(argv[0] + ' /name/space/path /data/path/1 [/data/path/2] ... [/data/path/n]' + old_args)
        exit(0)

    ns = argv[1]
    data = argv[2:]
    ns_links = set()
    illegals = set()
    i = 1
    # Check if name space and source are dirs
    cdnf(ns)
    for d in data:
        cdnf(d)

    print('Collecting all link targets in ' + ns)

    # Find all link targets in ns
    for root, dirs, files in walk(ns):
        for filename in files:
            # Create file name
            fpath = path.join(root, filename)
            if (i % 1000) == 0:
                stdout.write(('Processing NS entry %d: %s' % (i, fpath)).ljust(textwidth) + '\r')
                stdout.flush()
            # Check if it is link
            if path.islink(fpath):
                # Get link target
                target = readlink(fpath)
                # Check if link address is absolute
                if not path.isabs(target):
                    # Create absolute link address
                    target = path.abspath(path.join(path.dirname(target), target))
                if path.isfile(target):
                    ns_links.add(target)
                    i += 1
                else:
                    illegals.add(fpath)
            else:
                illegals.add(fpath)
    print('\r' + ('Found total of %d links.' % i).ljust(textwidth))

    i = 1
    for d in data:
        for root, dirs, files in walk(d):
            for filename in files:
                # Create file name
                fpath = path.join(root, filename)
                if (i % 100) == 0:
                    stdout.write(('Processing data entry %d: %s' % (i, fpath)).ljust(textwidth) + '\r')
                    stdout.flush()
                # Check if file is in NS entries
                if not (fpath in ns_links):
                    print('\r' + ('Removing dark data file: ' + fpath).ljust(textwidth) + '\n')
                    remove(fpath)
                i += 1

    # Count all illegals
    icount = len(illegals)
    if icount > 0:
        d = ''
        # Ask what to do about all illegal files
        # Ignore it with 'q'
        while d != 'q' or d != 'Q':
            print('Found %d illegal (not links) entries in namespace.\nWhat would you like to do about it?\n'
                  '\nBeware if you have some file systems unmounted!!!\n' % icount)
            d = input('(D)elete entires\n(L)ist entires\n(Q)uit and do nothing about it\n')
            # Delete illegals
            if d == 'D' or d == 'd':
                for f in illegals:
                    remove(f)
                print('Illegal entries were deleted.')
                break
            # List all illegal files
            elif d == 'L' or d == 'l':
                print(illegals)
            elif d == 'Q' or d == 'q':
                break
            else:
                print('Unknown choice "' + d + '"!')

    # Clean all empty dirs in src and s_ns
    print('Cleaning empty directories in data dirs')
    for d in data:
        clean_empty_dirs(d)
    exit(0)
