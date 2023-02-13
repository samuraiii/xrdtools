#!/usr/bin/python3
# vim: set fileencoding=utf-8 :
# Version 1.2.1
'''
Scans all entries in XRootD namespace
and than scans all data files,
deleting all data files not found in namespace.
It also finds all illegal namespace entires (not links)
and allows them to be deleted.
'''
from os import path, readlink, remove, walk
from sys import argv, exit, stdout  # pylint: disable=redefined-builtin
from subprocess import call

TEXT_WIDTH: int = 120


def check_if_dir_exists(check_dir: str) -> None:
    '''
    This check if parameter is existing dir or it fails script
    '''
    if not path.isdir(check_dir):
        exit(f'{check_dir} should be directory but it is not!')


def status_print(print_line: str, keep: bool=True) -> None:
    '''
    Prints line TEXT_WIDTH long (left padded),
    either permanently (keep == True) or to be overwritten.
    '''
    formated_line: str = print_line.ljust(TEXT_WIDTH)
    if keep:
        print(formated_line)
    else:
        stdout.write(f'{formated_line}\r')
        stdout.flush()


def clean_empty_dirs(directory_to_be_cleaned: str) -> None:
    '''
    Finds (depth first) all empty dirs and deletes them
    '''
    call(['/bin/find', directory_to_be_cleaned, '-mindepth', \
        '1', '-type', 'd', '-empty', '-delete'])


if __name__ == '__main__':
    OLD_ARGS: str = ''
    if len(argv) < 3:
        OLD_ARGS = f"\nYou gave:\n   {' '.join(argv)}"
        argv[1:] = ['-h']

    if ('-h' in argv) or ('--help' in argv):
        print('This script is to be used in this way:')
        print(f'{argv[0]} /name/space/path /data/path/1 '\
            '[/data/path/2] ... [/data/path/n] {OLD_ARGS}')
        exit(0)

    NAME_SPACE: str = argv[1]
    DATA_DIRS: list = argv[2:]
    NAME_SPACE_LINKS: set = set()
    ILLEGAL_NAME_SPACE_ENTRIES: set = set()
    ITERATOR: int = 1
    # Check if name space and source are dirs
    check_if_dir_exists(NAME_SPACE)
    for DATA_DIR in DATA_DIRS:
        check_if_dir_exists(DATA_DIR)

    print('Collecting all link targets in ' + NAME_SPACE)

    # Find all link targets in ns
    for name_space_root, _, name_space_files in walk(NAME_SPACE):
        for name_space_entry in name_space_files:
            # Create file name
            name_space_entry_path: str = path.join(name_space_root, name_space_entry)
            if (ITERATOR % 1000) == 0:
                status_print(f'Processing NS entry {ITERATOR}: {name_space_entry_path}', keep=False)
            # Check if it is link
            if path.islink(name_space_entry_path):
                # Get link target
                name_space_link_target: str = readlink(name_space_entry_path)
                # Check if link address is absolute
                if not path.isabs(name_space_link_target):
                    # Create absolute link address
                    name_space_link_target = path.abspath(
                        path.join(
                            path.dirname(name_space_link_target),
                            name_space_link_target))
                if path.isfile(name_space_link_target):
                    NAME_SPACE_LINKS.add(name_space_link_target)
                    ITERATOR += 1
                else:
                    ILLEGAL_NAME_SPACE_ENTRIES.add(name_space_entry_path)
            else:
                ILLEGAL_NAME_SPACE_ENTRIES.add(name_space_entry_path)
    status_print(f'Found total of {ITERATOR} links.')

    ITERATOR = 1
    for DATA_DIR in DATA_DIRS:
        for data_root, _, data_files in walk(DATA_DIR):
            for data_file in data_files:
                # Create file name
                data_file_path: str = path.join(data_root, data_file)
                if (ITERATOR % 100) == 0:
                    status_print(f'Processing data entry {ITERATOR}: {data_file_path}', keep=False)
                # Check if file is in NS entries
                if not data_file_path in NAME_SPACE_LINKS:
                    status_print(f'Removing dark data file: {data_file_path}')
                    remove(data_file_path)
                ITERATOR += 1

    # Count all illegals
    ILLEGALS_COUNT: int = len(ILLEGAL_NAME_SPACE_ENTRIES)
    if ILLEGALS_COUNT > 0:
        USER_INPUT: str = ''
        # Ask what to do about all illegal files
        # Ignore it with 'q'
        while USER_INPUT not in {'q', 'Q', 'd', 'D'}:
            print(f'Found {ILLEGALS_COUNT} illegal (not links) entries in namespace.')
            print('What would you like to do about it?')
            print('\nBeware if you have some file systems unmounted!!!')
            USER_INPUT = input('(D)elete entires\n(L)ist entires\n(Q)uit and do nothing about it\n')
            # Delete illegals
            if USER_INPUT in {'D', 'd'}:
                for ILLEGAL_ENTRY in ILLEGAL_NAME_SPACE_ENTRIES:
                    try:
                        remove(ILLEGAL_ENTRY)
                    except FileNotFoundError:
                        pass
                print('Illegal entries were deleted.')
                USER_INPUT = 'Q'
            # List all illegal files
            elif USER_INPUT in {'L', 'l'}:
                print(ILLEGAL_NAME_SPACE_ENTRIES)
            else:
                print(f'Unknown choice "{USER_INPUT}"!')

    # Clean all empty dirs in src and s_ns
    print('Cleaning empty directories in data dirs')
    for DATA_DIR in DATA_DIRS:
        clean_empty_dirs(DATA_DIR)
    exit(0)
