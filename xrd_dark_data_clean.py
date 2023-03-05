#!/usr/bin/python3
# vim: set fileencoding=utf-8 :
# Version 2.0.2
'''
Scans all entries in XRootD namespace
and than scans all data files,
deleting all data files not found in namespace.
It also finds all illegal namespace entires (not links)
and allows them to be deleted.
'''
from os import path, readlink, remove, cpu_count, scandir
from sys import argv, exit, stdout, stderr # pylint: disable=redefined-builtin
from subprocess import call
from threading import Lock, Condition, Thread, Event
from typing import Union, Generator, TextIO

TEXT_WIDTH: int = 150
THREADS: int = cpu_count()*2

def status_print(print_line: str, keep: bool = True, error: bool = False) -> None:
    '''
    Prints line TEXT_WIDTH long (left padded),
    either permanently (keep == True) or to be overwritten.
    '''
    if error:
        outfile: TextIO = stderr
    else:
        outfile: TextIO = stdout
    formated_line: str = print_line.ljust(TEXT_WIDTH)
    if keep:
        print(formated_line, file = outfile)
    else:
        stdout.write(f'{formated_line}\r')
        stdout.flush()


def walk(top: str,
    threads: int = THREADS,
    separate_links: bool = True,
    full_paths: bool = True) -> Generator:
    '''
    Multi-threaded version of os.walk() with some improvements
    '''
    if not path.isdir(top):
        return
    lock: Lock = Lock()
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
                dirs: set = set()
                files: set = set()
                links: set = set()
                for item in scandir(fs_path):
                    item_name: str = item.name
                    if full_paths:
                        item_name = item.path
                    if item.is_symlink():
                        if separate_links:
                            links.add(item_name)
                        else:
                            files.add(item_name)
                    elif item.is_file():
                        files.add(item_name)
                    else:
                        dirs.add(item_name)
                        with lock:
                            state['tasks'] += 1
                            paths.add(item.path)
                            on_input.notify()
                with lock:
                    if separate_links:
                        output.append((fs_path, dirs, files, links))
                    else:
                        output.append((fs_path, dirs, files))
                    on_output.notify()
            except OSError as exception:
                status_print(exception, error=True)
            finally:
                with lock:
                    state['tasks'] -= 1
                    if not state['tasks']:
                        on_input.notify_all()

    workers: list = [Thread(target=worker, name=f'fastio.walk {i} {top}') for i in range(threads)]
    for start_worker in workers:
        start_worker.start()
    while threads or output:  # TODO(jart): Why is 'or output' necessary?
        with lock:
            while not output:
                on_output.wait()
            item: tuple = output.pop()
        if item:
            yield item
        else:
            threads -= 1


def check_if_dir_exists(check_dir: str) -> None:
    '''
    This check if parameter is existing dir or it fails script
    '''
    if not path.isdir(check_dir):
        exit(f'{check_dir} should be directory but it is not!')


def clean_empty_dirs(directory_to_be_cleaned: str) -> None:
    '''
    Finds (depth first) all empty dirs and deletes them
    '''
    call(['/bin/find', directory_to_be_cleaned, '-mindepth', \
        '1', '-type', 'd', '-empty', '-delete'])

def get_name_space_links(name_space_root: str) -> tuple:
    '''
    Process the links in multithreaded way
    '''
    getns_lock: Lock = Lock()
    getns_on_input: Condition = Condition(getns_lock)
    getns_on_output: Condition = Condition(getns_lock)
    getns_event: Event = Event()
    getns_state: dict = {
        'iterator': 0,
        'link_count': 0,
        'links_to_process': set(),
        'illegal_ns_data': set(),
        'valid_links': set(),
    }

    def process_name_space_link(link_to_check: str) -> Union[str, None]:
        '''
        Checks if given path is link to existing file
        '''
        out: Union[str, None] = None
        link_target: str = readlink(link_to_check)
        # Check if link address is absolute
        if not path.isabs(link_target):
            # Create absolute link address
            link_target = path.abspath(path.join(path.dirname(link_to_check), link_target))
        if path.isfile(link_target):
            out = link_target
        return out

    # Find all link targets in ns
    for _, _, name_space_files, name_space_links in walk(name_space_root):
        for getns_link in name_space_links:
            getns_state['links_to_process'].add(getns_link)
            getns_state['link_count'] += 1
            if getns_state['link_count'] % 1000 == 0:
                status_print(f'Found {getns_state["link_count"]:_} entries so far...', keep=False)
        getns_state['illegal_ns_data'] = getns_state['illegal_ns_data'].union(name_space_files)
    status_print(f'Found {getns_state["link_count"]:_} name space entries')

    def getns_worker() -> None:
        while True:
            with getns_lock:
                while True:
                    if len(getns_state['links_to_process']) == 0:
                        getns_on_output.notify()
                        return
                    getns_path: str = getns_state['links_to_process'].pop()
                    getns_state['iterator'] += 1
                    if (getns_state['iterator'] % 1_000) == 0:
                        status_print(f'Processing NS entry {getns_state["iterator"]:_} of '\
                            f'{getns_state["link_count"]:_}: {getns_path}', keep=False)
                    break
            getns_target: Union[str, None] = process_name_space_link(getns_path)
            with getns_lock:
                if getns_target and getns_target not in getns_state['valid_links']:
                    getns_state['valid_links'].add(getns_target)
                else:
                    getns_state['illegal_ns_data'].add(getns_path)
                if len(getns_state['links_to_process']) == 0:
                    getns_on_input.notify_all()
                if getns_event.is_set():
                    getns_on_input.notify_all()
                    break

    getns_workers: list = [Thread(target=getns_worker, \
        name=f'getns_worker {i} {name_space_root}') for i in range(THREADS)]
    try:
        for getns_start_worker in getns_workers:
            getns_start_worker.start()
    except KeyboardInterrupt:
        getns_event.set()
    for getns_start_worker in getns_workers:
        getns_start_worker.join()
    status_print(f'Found {len(getns_state["valid_links"]):_} valid name space entries')
    return (getns_state['valid_links'], getns_state['illegal_ns_data'])


def find_dark_data(data_directories: list, name_space_links: set) -> set:
    '''
    Checks the data dirs for dark data files
    '''
    dark_data: set = set()
    dark_iterator: int = 1
    for data_dir in data_directories:
        for _, _, data_files, data_links in walk(data_dir):
            for data_link in data_links:
                dark_data.add(data_link)
            for data_file in data_files:
                # Create file name
                if (dark_iterator % 1_000) == 0:
                    status_print(f'Processing data entry {dark_iterator:_}: {data_file}', \
                        keep=False)
                # Check if file is in NS entries
                if not data_file in name_space_links:
                    dark_data.add(data_file)
                dark_iterator += 1
    return dark_data


def delete(del_file) -> None:
    '''
    Try to delete file, fail silently if it does not exist
    '''
    try:
        remove(del_file)
    except FileNotFoundError:
        pass


def save_to_file(data: any) -> None:
    '''
    Saves the data to specified file
    '''
    savefile: str = ''
    while True:
        savefile = input('Please write file path for saving the data:\n')
        if not path.isabs(savefile):
            status_print('Please specify save file as absolute path:')
        else:
            savedir: str = path.dirname(savefile)
            if not path.exists(savedir):
                status_print(f'The directory {savedir} does not exist, '\
                    'please select a different file path!')
            else:
                break
    if isinstance(data, (list, set)):
        data_out: str = '\n'.join(data)
    else:
        data_out = data
    with open(savefile, 'w', encoding='utf-8') as savefile_handle:
        savefile_handle.write(data_out)


def handle_data(data_to_handle: set, dark: bool) -> None:
    '''
    Asks user for input and handles the entries
    '''
    data_count: int = len(data_to_handle)
    if data_count > 0:
        user_input: str = ''
        while user_input not in {'q', 'Q', 'd', 'D'}:
            if dark:
                status_print(f'Found {data_count:_} dark data files.')
            else:
                status_print(f'Found {data_count:_} illegal (not links or dangling links) '\
                    'entries in namespace.')
                status_print('\nBeware if you have some file systems unmounted!!!')
            status_print('What would you like to do about it?')
            status_print('(D)elete entries')
            status_print('(L)ist entries')
            status_print('(S)ave the entries into a file')
            user_input = input('(Q)uit and do nothing about it\n')
            if user_input in {'D', 'd'}:
                for entry in data_to_handle:
                    delete(entry)
                status_print('Entries were deleted...')
            elif user_input in {'L', 'l'}:
                for entry in data_to_handle:
                    status_print(entry)
            elif user_input in {'S', 's'}:
                save_to_file(data_to_handle)
            elif user_input in {'q', 'Q'}:
                pass
            else:
                status_print(f'Unknown choice "{user_input}"!')



if __name__ == '__main__':
    OLD_ARGS: str = ''
    if len(argv) < 3:
        OLD_ARGS = f"\nYou gave:\n   {' '.join(argv)}"
        argv[1:] = ['-h']

    if ('-h' in argv) or ('--help' in argv):
        status_print('This script is to be used in this way:')
        status_print(f'{argv[0]} /name/space/path /data/path/1 '\
            '[/data/path/2] ... [/data/path/n] {OLD_ARGS}')
        exit(0)

    NAME_SPACE: str = argv[1]
    DATA_DIRS: list = argv[2:]
    NAME_SPACE_LINKS: set
    ILLEGAL_NAME_SPACE_ENTRIES: set
    check_if_dir_exists(NAME_SPACE)
    for DATA_DIR in DATA_DIRS:
        check_if_dir_exists(DATA_DIR)

    status_print(f'Collecting all link targets in {NAME_SPACE}')

    NAME_SPACE_LINKS, ILLEGAL_NAME_SPACE_ENTRIES = get_name_space_links(NAME_SPACE)

    handle_data(find_dark_data(DATA_DIRS, NAME_SPACE_LINKS), True)

    handle_data(ILLEGAL_NAME_SPACE_ENTRIES, False)

    status_print('Cleaning empty directories in data dirs')
    for DATA_DIR in DATA_DIRS:
        clean_empty_dirs(DATA_DIR)
    exit(0)
