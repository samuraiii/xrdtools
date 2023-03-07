#!/usr/bin/python3
# vim: set fileencoding=utf-8 :
'''
Version 1.5.0
Migrates XRootD storage to different location
Conforms to the ALICE exepriment storage layout
Tested on CentOS 7.
'''
from hashlib import md5
from multiprocessing import Lock, Pool, Queue, cpu_count, current_process
from os import path, readlink, remove, scandir
from random import random
from re import match, sub
from subprocess import call
from sys import argv, exit, stderr  # pylint: disable=redefined-builtin
from threading import Condition, Thread, Event
from threading import Lock as tLock
from time import sleep

OLD_ARGS: str = ''
if len(argv) not in {7, 8}:
    OLD_ARGS = f"You gave:\n\r{' '.join(argv)}"
    argv[1:] = ['-h']

if ('-h' in argv) or ('--help' in argv):
    print('This script is to be used in this way:')
    print(f'{argv[0]} /source/name/space/path /source/path [user@]destination.server[:port] '\
         '/destination/name/space/path /destination/path user:group [number of threads]')
    print('\tScript will perform the local move when the destination host is "localhost".')
    print('\tIf you prepend /source/path with "r" it will treat path as python regexp,')
    print('\tallowing you to drain multiple filesystems at once.')

    print(OLD_ARGS)
    exit(0)

SOURCE_NAME_SPACE: str = argv[1]
SOURCE_DATA: str = argv[2]
DESTINATION_SERVER: str = argv[3]
DESTINATION_NAME_SPACE: str = argv[4]
DESTINATION_DATA: str = argv[5]
FILE_OWNER_AND_GROUP: str = argv[6]
SOURCE_ID: str = md5(
    f'{SOURCE_DATA}{str(random())}8ZS8s6tDDLOz+dDZVFTKaZ4mjIH'.encode('utf-8')
    ).hexdigest()
TRANSFER_COUNT: int = 1
ILLEGAL_ENTRIES_IN_SOURCE_NAME_SPACE: set = set()
SNYC_DIRECTORIES_ONLY: list = ['--include=*/', '--exclude=*']

MULTIPROCESS_THREADS: int = cpu_count()*2
if len(argv) == 8 and match(r'^\d+$', str(argv[7])):
    MULTIPROCESS_THREADS = int(argv[7])

if not match(r'[a-z][a-z0-9\\\-]+:[a-z][a-z0-9\\\-]+', FILE_OWNER_AND_GROUP):
    exit(f'{FILE_OWNER_AND_GROUP} is not a valid user:group definition.')


DESTINATION_PORT: str = '22'
if ':' in DESTINATION_SERVER:
    DESTINATION_SERVER, DESTINATION_PORT = DESTINATION_SERVER.split(':')

DESTINATION_SERVER_USER: str = 'root'
if '@' in DESTINATION_SERVER:
    DESTINATION_SERVER_USER, DESTINATION_SERVER = DESTINATION_SERVER.split('@')

LOCAL_MOVE: bool = bool(DESTINATION_SERVER == 'localhost')


def ssh_connection(socket_id: str=SOURCE_ID) -> list:
    '''
    ssh connection details
    '''
    return [
        '/usr/bin/ssh',
        '-o', 'ControlMaster=auto',
        '-o', f'ControlPath=/dev/shm/.xrd-drain-{socket_id}.socket',
        '-o', 'ControlPersist=1200',
        '-o', 'Compression=no',
        '-x',
        '-T',
        '-p', DESTINATION_PORT,
        '-l', DESTINATION_SERVER_USER
    ]


def ssh_connection_formated(socket_id_f: str=SOURCE_ID) -> str:
    '''
    Formated ssh connection
    '''
    return f"{' '.join(ssh_connection(socket_id_f))} "


def check_if_directory_exists(check_directory: str) -> None:
    '''
    This checks if parameter is existing dir, or it fails script.
    '''
    if not path.isdir(check_directory):
        exit(f'{check_directory} should be directory but it is not!')


def flatten(list_to_flatten: list) -> list:
    '''
    Flattens list recursively
    '''
    if list_to_flatten == []:
        return list_to_flatten
    if isinstance(list_to_flatten[0], list):
        return flatten(list_to_flatten[0]) + flatten(list_to_flatten[1:])
    return list_to_flatten[:1] + flatten(list_to_flatten[1:])


def explode_path(filestring: str, start: str='/') -> list:
    '''
    Returns all dirs on a way from "start" to last dir (like in dirname)
    '''
    members: list = []
    while not match(f'{start}/*$', f'{filestring}/'):
        members.append(filestring)
        filestring: str = path.dirname(filestring)
    return members


def remove_double_slashes(string: str) -> str:
    '''
    Removes "//" from string
    '''
    return sub('//', '/', string)


def rsync(command: list, rsocket_name: str=SOURCE_ID) -> int:
    '''
    Call rsync with given arguments (Pass only options to this function)
    '''
    if LOCAL_MOVE:
        command: list = flatten(['/usr/bin/rsync', '-a', command])
    else:
        command: list = flatten(['/usr/bin/rsync', '-a', '-e', \
            ssh_connection_formated(rsocket_name), command])
    return call(command)


def migrate(   # pylint: disable=too-many-branches,too-many-locals
    source_link : str,
    source_file: str,
    multi_thread_tranfers: int,
    migration_iolock: Lock,
    worker_id: str) -> None:
    '''
    This migrates data and link to new destination
    '''
    # Create destination 'addresses'
    destination_file: str = remove_double_slashes(
        sub(SOURCE_DATA, f'{DESTINATION_DATA}/', source_file))
    destination_link: str = remove_double_slashes(
        sub(SOURCE_NAME_SPACE, f'{DESTINATION_NAME_SPACE}/', source_link))
    rsync_command: list
    if LOCAL_MOVE:
        rsync_command = [remove_double_slashes(source_file), \
            remove_double_slashes(f'/{destination_file}')]
    else:
        rsync_command = [remove_double_slashes(source_file), \
            remove_double_slashes(f'{DESTINATION_SERVER}:/{destination_file}')]
    destination_file_directory: str = path.dirname(destination_file)
    destination_link_directory: str = path.dirname(destination_link)
    # get all dest dirs up to d_ns and dest
    destination_link_directory_members:str = ' '.join(
        explode_path(destination_link_directory, DESTINATION_NAME_SPACE))
    destination_file_directory_members: str = ' '.join(
        explode_path(destination_file_directory, DESTINATION_DATA))
    socket_name: str
    # separate ssh socket for each worker
    socket_name = f'{SOURCE_ID}-{worker_id}'
    # Sleep during first 110% of mp_threads transfers up
    # to ~10 seconds not to overwhelm the destinations sshd
    if multi_thread_tranfers \
        < (MULTIPROCESS_THREADS + max((round(MULTIPROCESS_THREADS/10)), 1)):
        sleep(multi_thread_tranfers/(round(MULTIPROCESS_THREADS/10) + 1))
    # create directory structure on destination
    create_dir: str
    if LOCAL_MOVE:
        create_dir = ['/bin/mkdir', '-p', 'd_filedir']
    else:
        create_dir = flatten([ssh_connection(socket_name), DESTINATION_SERVER, \
            f'/bin/mkdir -p {destination_file_directory} {destination_link_directory}'])
    with migration_iolock:
        print(f'Start migrating file {multi_thread_tranfers:>10_}: {source_link}')
    if call(create_dir) == 0:
        # Rsync data file
        if rsync(rsync_command, socket_name) == 0:
            # Create link on destination and set owner:group
            link_cmd: str = f'/bin/ln -sf {destination_file} {destination_link} && '\
                f'/bin/chown -h {FILE_OWNER_AND_GROUP} {destination_link} '\
                    f'{destination_file_directory_members}  {destination_link_directory_members}'
            create_link: list
            if LOCAL_MOVE:
                create_link = ['/bin/sh', '-c', link_cmd]
            else:
                create_link = flatten([ssh_connection(socket_name), DESTINATION_SERVER, \
                    f'/bin/ln -sf {destination_file} {destination_link} '\
                    f'&& /bin/chown -h {FILE_OWNER_AND_GROUP} {destination_link} '\
                    f'{destination_file_directory_members} {destination_link_directory_members}'])
            if call(create_link) == 0:
                # Remove source data
                if not LOCAL_MOVE:
                    remove(source_link)
                remove(source_file)
                with migration_iolock:
                    print(f'Done migrating file  {multi_thread_tranfers:>10_}: {source_link}')
            else:
                with migration_iolock:
                    # Link failure
                    print(f'Failed to create link {DESTINATION_SERVER}:{destination_link} or '\
                        f'to set permissions! File: {multi_thread_tranfers:>10_}: {source_link}')
        else:
            with migration_iolock:
                # Data failure
                print(f'Failed to copy file: {source_file} to: '\
                    f'{DESTINATION_SERVER}:{destination_file}. File: '\
                    f'{multi_thread_tranfers:>10_}: {source_link}')
    else:
        with migration_iolock:
            print(f'Failed to create directories: {destination_file_directory} '\
                f'and/or {destination_link_directory}. '\
                f'File: {multi_thread_tranfers:>10_}: {source_link}')


def multithreaded_processing(multithread_queue: Queue, multithread_iolock: Lock) -> None:
    '''
    Multithreaded processing
    '''
    worker_id: str = f"{int(current_process().name.split('-')[1]):0>4}"
    multithreaded_link: str | None
    multithreaded_file: str | None
    multithreaded_number: int | None
    while True:
        multithreaded_link, multithreaded_file, multithreaded_number = multithread_queue.get()
        if None in [multithreaded_link, multithreaded_file, multithreaded_number]:
            break
        migrate(multithreaded_link, multithreaded_file, \
            multithreaded_number, multithread_iolock, worker_id)


def clean_empty_dirs(directory_to_clean: str) -> None:
    '''
    Finds (depth first) all empty dirs and deletes them
    '''
    call(['/bin/find', directory_to_clean, '-mindepth', '1', '-type', 'd', '-empty', '-delete'])


def walk(top: str,
    threads: int = MULTIPROCESS_THREADS,
    separate_links: bool = True,
    full_paths: bool = True):
    '''
    Multi-threaded version of os.walk() with some improvements
    '''
    if not path.isdir(top):
        return
    lock: Lock = tLock()
    on_input: Condition = Condition(lock)
    on_output: Condition = Condition(lock)
    state: dict = {'tasks': 1}
    paths: set = {top}
    output: list = []

    def worker() -> None:
        while True:
            with lock:
                while True:
                    if not state['tasks']:
                        output.append(None)
                        on_output.notify()
                        return
                    if not paths:
                        on_input.wait()
                        continue
                    fs_path: str = paths.pop()
                    break
            try:
                walk_dirs: set = set()
                walk_files: set = set()
                walk_links: set = set()
                for item in scandir(fs_path):
                    item_name: str = item.name
                    if full_paths:
                        item_name = item.path
                    if item.is_symlink():
                        if separate_links:
                            walk_links.add(item_name)
                        else:
                            walk_files.add(item_name)
                    elif item.is_file():
                        walk_files.add(item_name)
                    else:
                        walk_dirs.add(item_name)
                        with lock:
                            state['tasks'] += 1
                            paths.add(item.path)
                            on_input.notify()
                with lock:
                    if separate_links:
                        output.append((fs_path, walk_dirs, walk_files, walk_links))
                    else:
                        output.append((fs_path, walk_dirs, walk_files))
                    on_output.notify()
            except OSError as exception:
                print(exception, file=stderr)
            finally:
                with lock:
                    state['tasks'] -= 1
                    if not state['tasks']:
                        on_input.notify_all()

    workers: list = [Thread(target=worker, name=f'fastio.walk {i} {top}') for i in range(threads)]
    for start_worker in workers:
        start_worker.start()
    while threads or output:
        with lock:
            while not output:
                on_output.wait()
            item: tuple = output.pop()
        if item:
            yield item
        else:
            threads -= 1



def start_migration(name_space: str, data_dir: str) -> set:
    '''
    Does the MT migration
    '''
    migrate_lock: tLock = tLock()
    migrate_on_input: Condition = Condition(migrate_lock)
    migrate_on_output: Condition = Condition(migrate_lock)
    migrate_event: Event = Event()
    migrate_state: dict = {
        'iterator': 1,
        'illegals': set(),
        'links_to_process': set(),
    }
    # Find all valid links and corresponding files
    for _, _, files, links in walk(name_space):
        for filename in files:
            migrate_state['illegals'].add(filename)
        for ns_link in links:
            migrate_state['links_to_process'].add(ns_link)
    # Set up the multiprocess pool and queue
    mt_queue: Queue = Queue()
    io_lock: Lock = Lock()
    pool: Pool = Pool(MULTIPROCESS_THREADS, initializer=multithreaded_processing, \
        initargs=(mt_queue, io_lock))  # pylint: disable=consider-using-with

    def link_worker() -> None:
        while True:
            with migrate_lock:
                while True:
                    if len(migrate_state['links_to_process']) == 0:
                        migrate_on_output.notify()
                        return
                    migrate_path: str = migrate_state['links_to_process'].pop()
                    break
            try:
                migrate_target: str = readlink(migrate_path)
                if not path.isabs(migrate_target):
                    # Create absolute link address
                    migrate_target = path.abspath(
                        path.join(path.dirname(migrate_path), migrate_target))
                if match(data_dir, migrate_target):
                    with migrate_lock:
                        if not path.exists(migrate_target):
                            migrate_state['illegals'].add(migrate_path)
                        else:
                            mt_queue.put((migrate_path, migrate_target, migrate_state['iterator']))
                            migrate_state['iterator'] += 1
            except FileNotFoundError:
                migrate_state['illegals'].add(migrate_path)

            if len(migrate_state['links_to_process']) == 0:
                migrate_on_input.notify_all()
            if migrate_event.is_set():
                migrate_on_input.notify_all()
                break


    migrate_workers: list = [Thread(target=link_worker, \
        name=f'link_worker {i} {data_dir}') for i in range(MULTIPROCESS_THREADS)]
    try:
        for migrate_start_worker in migrate_workers:
            migrate_start_worker.start()
    except KeyboardInterrupt:
        migrate_event.set()
        for migrate_start_worker in migrate_workers:
            migrate_start_worker.join()
        while not mt_queue.empty():
            mt_queue.get()
    else:
        for migrate_start_worker in migrate_workers:
            migrate_start_worker.join()
    for _ in range(MULTIPROCESS_THREADS):
        mt_queue.put((None, None, None))
    pool.close()
    pool.join()

    return migrate_state['illegals']


if __name__ == '__main__':
    # Check if name space and source are dirs
    check_if_directory_exists(SOURCE_NAME_SPACE)
    REGEX_DATA: bool = False
    if SOURCE_DATA.startswith('r'):
        REGEX_DATA = True
        print('Detected regex as source data directory')
        print('The check of the data directory existence is skipped!')
        SOURCE_DATA = SOURCE_DATA[1:]
    else:
        check_if_directory_exists(SOURCE_DATA)

    TEST_FILE: str = f'/.xrd-drain-testfile_55c4e792761ddeb2dc{SOURCE_ID}'
    TEST_FILE = [f'{DESTINATION_NAME_SPACE}{TEST_FILE}', f'{DESTINATION_DATA}{TEST_FILE}']
    for test_destination in TEST_FILE:
        check_command: str = f'/bin/touch {test_destination} && /bin/chown {FILE_OWNER_AND_GROUP} '\
            f'{test_destination} && /bin/rm -f {test_destination}'
        if LOCAL_MOVE:
            check: list = ['/bin/sh', '-c', check_command]
        else:
            check = flatten([ssh_connection(), DESTINATION_SERVER, check_command])
        RETURN_CODE: int = call(check)
        if RETURN_CODE != 0:
            exit(f'Writing of test files to {DESTINATION_NAME_SPACE} and {DESTINATION_DATA} '\
                f'failed!\n Is {FILE_OWNER_AND_GROUP} defined on {DESTINATION_SERVER}?')

    ILLEGAL_ENTRIES_IN_SOURCE_NAME_SPACE = start_migration(SOURCE_NAME_SPACE, SOURCE_DATA)
    print('Data migration done')

    ILLEGALS_COUNT: int = len(ILLEGAL_ENTRIES_IN_SOURCE_NAME_SPACE)
    if ILLEGALS_COUNT > 0:
        USER_ACTION: str = ''
        while USER_ACTION not in {'q', 'Q', 'd', 'D'}:
            print(f'Found {ILLEGALS_COUNT:_} illegal (not links) entries in namespace.')
            print('What would you like to do about it?')
            USER_ACTION = input('(D)elete entries\n(L)ist entries'\
                '\n(Q)uit and do nothing about it\n')
            if USER_ACTION in {'D', 'd'}:
                for test_destination in ILLEGAL_ENTRIES_IN_SOURCE_NAME_SPACE:
                    try:
                        remove(test_destination)
                    except FileNotFoundError:
                        pass
                print('Illegal entries were deleted.')
            elif USER_ACTION in {'L', 'l'}:
                print(ILLEGAL_ENTRIES_IN_SOURCE_NAME_SPACE)
            elif USER_ACTION in {'q', 'Q'}:
                pass
            else:
                print(f'Unknown choice "{USER_ACTION}"!')

    # Clean all empty dirs in src and s_ns
    if REGEX_DATA:
        print('Detected regexp source data, skipping empty directory cleanup.')
    else:
        print(f'Cleaning empty directories in {SOURCE_DATA}')
        clean_empty_dirs(SOURCE_DATA)
    print(f'Cleaning empty directories in {SOURCE_NAME_SPACE}')
    clean_empty_dirs(SOURCE_NAME_SPACE)
    print('Migration finished')
    exit(0)
