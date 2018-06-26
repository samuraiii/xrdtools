#!/usr/bin/python2
import os
import sys
from subprocess import call

old_args = ''
if not(len(sys.argv) == 2):
    old_args = '\nYou gave:\n   ' + ' '.join(sys.argv)
    sys.argv[1:] = ['-h']

if ( '-h' in sys.argv ) or ( '--help' in sys.argv ):
    print('All xrootd related deamons should be stopped before runninf this script!!!')
    print('This script is to be used in this way:')
    print(sys.argv[0] + ' /name/space/path' + old_args)
    sys.exit(0)

ns = sys.argv[1]
illegals = []
filelist = {}

if not os.path.isdir(ns):
    sys.exit(ns + ' should be directory but it is not!')

if call(['/usr/bin/pgrep', 'xrootd']) or call(['/usr/bin/pgrep', 'cmsd']):
    x = raw_input('All xrootd related deamons should be stopped by now!\nAre you sure you want to continue?\n (Y)es = Possible data loss\n (N)o = Exit')
    if (not x == 'y') or (not x == 'Y') :
        sys.exit(0)

# Find all valid links
for root, dirs, files in os.walk(ns):
    for filename in files:
        # Create file name
        path = os.path.join(root, filename)
        # Check if it is link
        if os.path.islink(path):
            # Get link target
            target = os.readlink(path)
            # Check if link address is absolute
            if not os.path.isabs(target):
                # Create absolute link address
                target = os.path.join(os.path.dirname(path), target)
            if not os.path.exists(target):
                # Delete all matching dead links
                print('Removing dead link: ' + path)
                os.remove(path)
            else:
                filelist.setdefault(target, []).append(path)
        else:
            # Add to illegal files if file is not a link
            illegals.append(path)

# remove double linked files
for k in filelist.keys():
    if not(len(filelist[k]) == 1):
        for li in filelist[k]:
            os.remove(li)
        os.remove(k)
    del link_list[k]

#Count all illegals
icount = len(illegals)
if icount > 0:
    d = ''
    # Ask what to do about all illegal files
    # Ignore it with 'q'
    while d != 'q' or d != 'Q':
        print('Found {0} illegal (not links) entries in namespace.\nWhat would you like to do about it?'.format(icount))
        d = raw_input('(D)elete entires\n(L)ist entires\n(Q)uit and do nothing about it\n')
        # Delete illegals
        if d == 'D' or d == 'd':
            for f in illegals:
                os.remove(f)
            print('Illegal entries were deleted.')
            d = 'Q'
        # List all illegal files
        elif d == 'L' or d == 'l':
            print(illegals)
        else:
            print('Unknown choice "' + d +'"!')

# Clean all empty dirs in ns
print('Cleaning empty directories in ' + ns)
call(['/bin/find', ns, '-mindepth', '1', '-type', 'd', '-empty', '-delete' ])
sys.exit(0)
