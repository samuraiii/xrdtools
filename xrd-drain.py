#!/usr/bin/python2
from os import path, readlink, remove, walk
from re import escape, match, sub
from subprocess import call
from sys import argv, exit, stdout
from uuid import uuid4 as uuid
from hashlib import md5

old_args = ''
if not(len(argv) == 7):
    old_args = '\nYou gave:\n   ' + ' '.join(argv)
    argv[1:] = ['-h']

if ( '-h' in argv ) or ( '--help' in argv ):
    print('This script is to be used in this way:')
    print(argv[0] + ' /source/name/space/path /source/path [user@]destination.server[:port] /destination/name/space/path /destination/path user:group' + old_args)
    exit(0)

s_ns = argv[1]
src = argv[2]
src_id = md5(src + str(uuid())).hexdigest()
d_server = argv[3]
d_ns = argv[4]
dest = argv[5]
fileog = argv[6]
dtransfers = 1
illegals = []
sync_dirs_only = [ '--include=*/', '--exclude=*' ]
usermatch = '[a-z][a-z0-9\\\-]+'

if not match(usermatch + ':' + usermatch, fileog):
    exit(fileog + ' is not a valid user:group definition.')

if ':' in list(d_server):
    d_server, port = d_server.split(':')
else:
    port = '22'

if '@' in list(d_server):
    user, d_server = d_server.split('@')
else:
    user = 'root'

ssh = [
        '/usr/bin/ssh',
        '-o', 'ControlMaster=auto',
        '-o', 'ControlPath=/dev/shm/.xrd-drain-' + src_id + '.socket',
        '-o', 'ControlPersist=1200',
        '-o', 'Compression=no',
        '-x',
        '-T',
        '-p', port,
        '-l', user
    ]
sshf = ' ' + ' '.join(ssh) + ' '

def cdnf( cdir ):
    'This check if parameter is existing dir or it fails script'
    if not path.isdir(cdir):
        exit(cdir + ' should be directory but it is not!')
    return

def flatten(S):
    'Flattens list recursively'
    if S == []:
        return S
    if isinstance(S[0], list):
        return flatten(S[0]) + flatten(S[1:])
    return S[:1] + flatten(S[1:])


def explode( filestring, start='/'):
    'Returns all dirs on a way from "start" to last dir (like in dirname)'
    members = []
    while not match(start + '/*$', filestring + '/'):
        members.append(filestring)
        filestring = path.dirname(filestring)
    return members


def rds ( string ):
    'Removes // from string'
    return sub('//', '/', string )

def rsync( cmd ):
    'Call rsync with given arguments (Pass only options to this function)'
    # Use lighter (arc4) encryption and no Compression to speed transfers up
    cmd = flatten([ '/usr/bin/rsync', '-a', '-e', sshf, cmd ])
    return call(cmd)

def migrate( lin, fil ):
    'This migrates data and link to new destination'
    # Create destination 'addresses'
    d_file = rds(sub(escape(src), dest + '/', fil))
    d_link = rds(sub(escape(s_ns), d_ns + '/', lin))
    cmd = [ rds(fil), rds(d_server + ':/' + d_file)]
    d_filedir = path.dirname(d_file)
    d_linkdir = path.dirname(d_link)
    # get all dest dirs up to d_ns and dest
    d_linkdir_members = ' '.join( explode(d_linkdir, d_ns))
    d_filedir_members = ' '.join( explode(d_filedir, dest))
    # create directory structure on destination
    if call(flatten([ ssh, d_server, '/bin/mkdir -p ' + d_filedir + ' ' + d_linkdir ])) == 0:
        # Rsync data file
        if rsync(cmd) == 0:
            # Create link on destination and set owner:group
            if call(flatten([ ssh, d_server, '/bin/ln -sf ' + d_file + ' ' + d_link + ' && /bin/chown -h ' + fileog + ' ' + d_link + ' ' + d_filedir_members + ' ' + d_linkdir_members])) == 0:
                # Remove source data
                remove(lin)
                remove(fil)
            else:
                # Link failure
                print('Failed to create link ' + d_server + ':' + d_link + ' or to set permissions!')
        else :
            # Data failure
            print('Failed to copy file: ' + fil + ' to: ' + d_server + ':' + d_file)
    else :
        print('Failed to create directories: ' + d_filedir + ' and/or ' + d_linkdir )


def clean_empty_dirs( d ):
    'Finds (depth first) all empty dirs and deletes them'
    call(['/bin/find', d, '-mindepth', '1', '-type', 'd', '-empty', '-delete' ])


# Check if name space and source are dirs
cdnf(s_ns)
cdnf(src)

#sync_ns_dirs = [ sync_dirs_only, rds(s_ns + '/'), rds(d_server + ':/' + d_ns) ]
#sync_storage_dirs = [ sync_dirs_only, rds(src + '/'), rds(d_server + ':/' + dest) ]
# Prepare directory structure on destination
#try:
#    rsync(sync_ns_dirs)
#except:
#    exit('Initial sync of name space dirs failed (Is ssh connection to ' + d_server + ' possible?')

#try:
#    rsync(sync_storage_dirs)
#except:
#    exit('Initial sync of data dirs failed')

testfile = '/.xrd-drain-testfile_55c4e792761ddeb2dca627ffadca546f82359' + src_id
testfile = [ d_ns + testfile, dest + testfile ]
try:
    for f in testfile:
        call(flatten([ ssh, d_server, '/bin/touch ' + f + ' && /bin/chown ' + fileog + ' ' + f + ' && /bin/rm -f ' + f ]))
except:
    exit('Writing of testfiles to ' + d_ns + ' and ' + dest + ' failed!\n Is ' + fileog + ' defined on ' + d_server + '?')

# Find all valid links and corresponding files
for root, dirs, files in walk(s_ns, topdown=False):
    for filename in files:
        # Create file name
        filepath = path.join(root, filename)
        # Check if it is link
        if path.islink(filepath):
            # Get link target
            target = readlink(filepath)
            # Check if link address is absolute
            if not path.isabs(target):
                # Create absolute link address
                target = path.abspath(path.join(path.dirname(filepath), target))
            # Select only likns belonging to src
            if match(escape(src), target):
                if not path.exists(target):
                    # Delete all matching dead links
                    remove(filepath)
                else:
                    # Migrate all data
                    stdout.write('Migrating file %s: %s\n' % (str(dtransfers), filepath))
                    migrate(filepath, target)
                    dtransfers += 1
        else:
            # Add to illegal files if file is not a link
            illegals.append(filepath)

#Count all illegals
icount = len(illegals)
if icount > 0:
    d = ''
    # Ask what to do about all illegal files
    # Ignore it with 'q'
    while d != 'q' or d != 'Q':
        print('Found %d illegal (not links) entries in namespace.\nWhat would you like to do about it?' % icount)
        d = raw_input('(D)elete entires\n(L)ist entires\n(Q)uit and do nothing about it\n')
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
            print('Unknown choice "' + d +'"!')

# Clean all empty dirs in src and s_ns
print('Cleaning empty directories in ' + src + ' and ' + s_ns)
clean_empty_dirs(src)
clean_empty_dirs(s_ns)
exit(0)
