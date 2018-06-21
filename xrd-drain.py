#!/usr/bin/python
import os
import sys
from re import sub, escape, match
from subprocess import call

old_args = ""
if len(sys.argv) < 6:
    old_args = "\nYou gave:\n   " + ' '.join(sys.argv)
    sys.argv[1:] = ["-h"]

if ( "-h" in sys.argv ) or ( "--help" in sys.argv ):
    print("This script is to be used in this way:")
    print(sys.argv[0] + " /source/name/space/path /source/path [user@]destination.server[:port] /destination/name/space/path /destination/path" + old_args)
    sys.exit(0)

s_ns = sys.argv[1]
src = sys.argv[2]
d_server = sys.argv[3]
d_ns = sys.argv[4]
dest = sys.argv[5]
dtransfers = 1
illegals = []
sync_dirs_only = [ '--include=*/', '--exclude=*' ]

if ":" in list(d_server):
    d_server, port = d_server.split(':')
else:
    port = "22"

if "@" in list(d_server):
    user, d_server = d_server.split('@')
else:
    user = "root"

def cdnf( cdir ):
    "This check if parameter is existing dir or it fails script"
    if not os.path.isdir(cdir):
        sys.exit(cdir + " should be directory but it is not!")
    return

def flatten(S):
    "Flattens list recursively"
    if S == []:
        return S
    if isinstance(S[0], list):
        return flatten(S[0]) + flatten(S[1:])
    return S[:1] + flatten(S[1:])

def rds ( string ):
    "Removes // from string"
    return sub('//', '/', string )

def rsync( cmd ):
    "Call rsync with given arguments (Pass only options to this function)"
    # Use lighter (arc4) encryption and no Compression to speed transfers up
    cmd = flatten([ '/usr/bin/rsync', '-a', '-e', '/usr/bin/ssh -T -c arcfour -o Compression=no -x -p ' + port + ' -l ' + user, cmd ])
    return call(cmd)

def migrate( lin, fil ):
    "This migrates data and link to new destination"
    # Create destination "addresses"
    d_file = rds(sub(escape(src), dest + '/', fil))
    d_link = rds(sub(escape(s_ns), d_ns + '/', lin))
    cmd = [ rds(fil), rds(d_server + ':/' + d_file)]
    # Rsync data file
    if rsync(cmd) == 0:
        # Create link on destination
        if call([ '/usr/bin/ssh', '-p', port, user + '@' + d_server, '/bin/ln -sf ' + d_file + ' ' + d_link]) == 0:
            # Remove source data
            os.remove(lin)
            os.remove(fil)
        else:
            # Link failure
            print("Failed to create link " + d_server + ":" + d_link)
    else :
        # Data failure
        print("Failed to copy file: " + fil + " to: " + d_server + ":" + d_file)

def clean_empty_dirs( d ):
    "Finds (depth first) all empty dirs and deletes them"
    call(['/bin/find', d, '-mindepth', '1', '-type', 'd', '-empty', '-delete' ])

sync_ns_dirs = [ sync_dirs_only, rds(s_ns + '/'), rds(d_server + ':/' + d_ns) ]
sync_storage_dirs = [ sync_dirs_only, rds(src + '/'), rds(d_server + ':/' + dest) ]

# Check if name space and source are dirs
cdnf(s_ns)
cdnf(src)

# Prepare directory structure on destination
try:
    rsync(sync_ns_dirs)
except:
    sys.exit("Initial sync of name space dirs failed")

try:
    rsync(sync_storage_dirs)
except:
    sys.exit("Initial sync of data dirs failed")

# Find all valid liks and corresponding files
for root, dirs, files in os.walk(s_ns):
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
            # Select only likns belonging to src
            if match(escape(src), target):
                if not os.path.exists(target):
                    # Delete all matching dead links
                    os.remove(path)
                else:
                    # Migrate all data
                    sys.stdout.write("Migrating file {0}: {1}\n".format(dtransfers, path))
                    migrate(path, target)
                    dtransfers += 1
        else:
            # Add to illegal files if file is not a link
            illegals.append(path)

#Count all illegals
icount = len(illegals)
if icount > 0:
    d = ""
    # Ask what to do about all illegal files
    # Ignore it with "q"
    while d != "q" or d != "Q":
        print("Found {0} illegal (not links) entries in namespace.\nWhat would you like to do about it?".format(icount))
        d = raw_input("(D)elete entires\n(L)ist entires\n(Q)uit and do nothing about it\n")
        # Delete illegals
        if d == "D" or d == "d":
            for f in illegals:
                os.remove(f)
            print("Illegal entries were deleted.")
            d = "Q"
        # List all illegal files
        elif d == "L" or d == "l":
            print(illegals)
        else:
            print('Unknown choice "' + d +'"!')

# Clean all empty dirs in src and s_ns
print('Cleaning empty directories in ' + src + ' and ' + s_ns)
clean_empty_dirs(src)
clean_empty_dirs(s_ns)
sys.exit(0)
