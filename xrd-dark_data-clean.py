#!/usr/bin/python2
import os
import sys
from re import sub, escape, match
from subprocess import call

def cdnf( cdir ):
    'This check if parameter is existing dir or it fails script'
    if not os.path.isdir(cdir):
        sys.exit(cdir + ' should be directory but it is not!')
    return

def clean_empty_dirs( d ):
    'Finds (depth first) all empty dirs and deletes them'
    call(['/bin/find', d, '-mindepth', '1', '-type', 'd', '-empty', '-delete' ])

if __name__ == "__main__":
    old_args = ''
    if len(sys.argv) < 3:
        old_args = '\nYou gave:\n   ' + ' '.join(sys.argv)
        sys.argv[1:] = ['-h']

    if ( '-h' in sys.argv ) or ( '--help' in sys.argv ):
        print('This script is to be used in this way:')
        print(sys.argv[0] + ' /name/space/path /data/path/1 [/data/path/2] ... [/data/path/n]' + old_args)
        sys.exit(0)

    ns = sys.argv[1]
    data = sys.argv[2:]
    ns_links = []
    i = 1
    # Check if name space and source are dirs
    cdnf(ns)
    for d in data:
        cdnf(d)

    print('Collecting all link targets in ' + ns)

    # Find all link targets in ns
    for root, dirs, files in os.walk(ns):
        for filename in files:
            # Create file name
            path = os.path.join(root, filename)
            if (i % 1000) == 0 :
                sys.stdout.write('Processing NS entry {0}: {1}\r'.format(i, path))
                sys.stdout.flush()
            # Check if it is link
            if os.path.islink(path):
                # Get link target
                target = os.readlink(path)
                # Check if link address is absolute
                if not os.path.isabs(target):
                    # Create absolute link address
                    target = os.path.join(os.path.dirname(path), target)
                ns_links.append(target)
                i += 1

    i = 1
    for d in data:
        for root, dirs, files in os.walk(d):
            for filename in files:
                # Create file name
                path = os.path.join(root, filename)
                if (i % 1000) == 0 :
                    sys.stdout.write('Processing data entry {0}: {1}\r'.format(i, path))
                    sys.stdout.flush()
                # Check if file is in NS entries
                if not (path in ns_links):
                    print('\rRemoving dark data file: ' + path + '\n')
                    os.remove(path)
                i += 1


    # Clean all empty dirs in src and s_ns
    print('Cleaning empty directories in data dirs')
    for d in data:
        clean_empty_dirs(d)
    sys.exit(0)
