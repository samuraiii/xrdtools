#!/usr/bin/python3
# vim: set fileencoding=utf-8 :
# Version 1.1.0
from os import path, remove, readlink, walk
from sys import argv, exit
from subprocess import call

if __name__ == "__main__":
    old_args = ''
    if not(len(argv) == 2):
        old_args = '\nYou gave:\n   ' + ' '.join(argv)
        argv[1:] = ['-h']

    if ('-h' in argv) or ('--help' in argv):
        print('All xrootd related daemons should be stopped before running this script!!!')
        print('This script is to be used in this way:')
        print(argv[0] + ' /name/space/path' + old_args)
        exit(0)

    ns = argv[1]
    illegals = set()
    filelist = {}

    if not path.isdir(ns):
        exit(ns + ' should be directory but it is not!')

    if (not call(['/usr/bin/pgrep', 'xrootd'])) and (not call(['/usr/bin/pgrep', 'cmsd'])):
        x = input('All xrootd related deamons should be stopped by now!\n'
                      'Are you sure you want to continue?\n (Y)es = Possible data loss\n (N)o = Exit\n')
        if (not x == 'y') and (not x == 'Y'):
            exit(0)
    # Find all valid links
    for root, dirs, files in walk(ns):
        for filename in files:
            # Create file name
            fpath = path.join(root, filename)
            # Check if it is link
            if path.islink(fpath):
                # Get link target
                target = readlink(fpath)
                # Check if link address is absolute
                if not path.isabs(target):
                    # Create absolute link address
                    target = path.abspath(path.join(path.dirname(fpath), target))
                if not path.exists(target):
                    # Delete all matching dead links
                    print('Removing dead link: ' + fpath)
                    remove(fpath)
                else:
                    filelist.setdefault(target, set()).add(fpath)
            else:
                # Add to illegal files if file is not a link
                illegals.add(fpath)

    # remove double linked files
    for k in filelist.keys():
        if not(len(filelist[k]) == 1):
            for li in filelist[k]:
                remove(li)
            remove(k)
        del filelist[k]

    # Count all illegals
    icount = len(illegals)
    if icount > 0:
        d = ''
        # Ask what to do about all illegal files
        # Ignore it with 'q'
        while d != 'q' or d != 'Q':
            print('Found %d illegal (not links) entries in namespace.\nWhat would you like to do about it?' % icount)
            d = input('(D)elete entires\n(L)ist entires\n(Q)uit and do nothing about it\n')
            # Delete illegals
            if d == 'D' or d == 'd':
                for f in illegals:
                    remove(f)
                print('Illegal entries were deleted.')
                d = 'Q'
            # List all illegal files
            elif d == 'L' or d == 'l':
                print(illegals)
            elif d == 'Q' or d == 'q':
                break
            else:
                print('Unknown choice "' + d + '"!')

    # Clean all empty dirs in ns
    print('Cleaning empty directories in ' + ns)
    call(['/bin/find', ns, '-mindepth', '1', '-type', 'd', '-empty', '-delete'])
    exit(0)
