#!/usr/bin/python3
# vim: set fileencoding=utf-8 :
# Version 1.0.0
'''
Collects entries from namespaces of supplied servers
and saves it to one file per server and one file with all entries
This script can be quite memory intensive (~32GB RAM @ ~140M entries)
'''
from os import path, cpu_count, scandir
from sys import argv, exit, stderr # pylint: disable=redefined-builtin
from subprocess import call, Popen, PIPE
from threading import Lock, Condition, Thread
from typing import Generator
from re import sub, escape, match
from datetime import datetime

THREADS: int = cpu_count()*2
NOW: str = datetime.isoformat(datetime.now(),timespec='seconds')
REMOTE_SCRIPT: str = f'/tmp/{NOW}-{path.basename(argv[0])}'

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
                print(exception, error=True)
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


def check_if_dir_exists(check_dir: str, my_name: str) -> None:
    '''
    This check if parameter is existing dir or it fails script
    '''
    if not path.isdir(check_dir):
        exit(f'{my_name}: {check_dir} should be directory but it is not!')


def rds(string) -> str:
    '''
    Removes // from string
    '''
    return sub('//', '/', string)


def parse_server(server_string: str) -> tuple:
    '''
    Parses server string for arguments, deos basic validation
    '''
    server_string_w: str = server_string
    parse_user: str = 'root'
    parse_user_w: str = ''
    parse_port_w: str = ''
    parse_port: int = 22
    parse_namespace: str = ''
    parse_params: dict = {}
    server_string_w, parse_namespace = server_string_w.split('/', 1)
    parse_namespace = rds(f'/{parse_namespace}/')
    if '@' in server_string_w:
        parse_user_w, server_string_w = server_string.split('@', 1)
        if match('^[a-z_]([a-z0-9_-]{0,31}|[a-z0-9_-]{0,30}[$])$', parse_user_w):
            parse_user = parse_user_w
        else:
            exit(f'Parsed user "{parse_user_w}" is not valid for "{server_string}"')
    if ':' in server_string_w:
        server_string_w, parse_port_w = server_string_w.split(':', 1)
        if int(parse_port_w):
            parse_port = int(parse_port_w)
        else:
            exit(f'Parsed port "{parse_port_w}" is not valid for "{server_string}"')
    parse_params = {'port': str(parse_port), 'user': parse_user, 'name_space': parse_namespace}
    return (server_string_w, parse_params)





if __name__ == '__main__':
    if ('-h' in argv) or ('--help' in argv) or len(argv) == 1:
        print('This script is to be used in this way:')
        print(f'{argv[0]} [user1@]server1[port]/name/space/path1 '\
            '[user2@]server2[port]/name/space/path2'\
            '... [userN@]serverN[portN]/name/space/pathN')
        print('You can omit user and port for defualts ("root" and "22").')
        print(f'Example: {argv[0]} alice@xrd1.example.com:2222/xrd/space/ '\
            'bob@xrd2/bobsxrd/space/path')
        exit(0)

    SERVERS: dict = {'to_process': set(), 'servers': set(), 'all_files': set()}
    COLLECT: bool =  bool('COllECt_dATa' == argv[1])

    if COLLECT:
        MY_NAME: str = argv[2]
        NAMESPACE: str = argv[3]
        NAMESPACE_RE: str = escape(NAMESPACE)
        check_if_dir_exists(NAMESPACE, MY_NAME)
        for _, _, _, ns_entries in walk(NAMESPACE):
            for ns_entry in ns_entries:
                print(f"{sub(f'^{NAMESPACE_RE}', '', ns_entry)}", end = '\x00')
    else:
        for SERVER_ARG in argv[1:]:
            server: str = ''
            server_params: dict = {}
            server, server_params = parse_server(SERVER_ARG)
            SERVERS[server] = server_params
            SERVERS['to_process'].add(server)
            SERVERS['servers'].add(server)
            SERVERS[server]['entries']: set = set()

        COLLECT_LOCK: Lock = Lock()

        def get_entries() -> set:
            '''
            Copies the script to server, and collects the entries
            '''
            with COLLECT_LOCK:
                get_server: str = SERVERS['to_process'].pop()
            port: str = SERVERS[get_server]['port']
            user: str = SERVERS[get_server]['user']
            name_space: str = SERVERS[get_server]['name_space']
            if call([
                '/usr/bin/scp', '-q',
                '-P', port,
                f'{argv[0]}',
                f'{user}@{get_server}:{REMOTE_SCRIPT}']) == 0:
                for entry in Popen(['/usr/bin/ssh',
                        '-p', port,
                        '-l', user,
                        get_server,
                        f'/usr/bin/python3 {REMOTE_SCRIPT} COllECt_dATa {get_server} '\
                        f'{name_space}'],
                        stdout=PIPE, stderr=stderr,).communicate()[0].decode('utf-8').split('\x00'):
                    with COLLECT_LOCK:
                        SERVERS['all_files'].add(entry)
                        SERVERS[get_server]['entries'].add(entry)
                call(['/usr/bin/ssh', '-p', port, '-l', user,
                    get_server, f'/usr/bin/rm -f {REMOTE_SCRIPT}'])

        COLLECT_WORKERS: list = [Thread(target=get_entries, \
            name=f'collect_worker {s}') for s in SERVERS['to_process']]
        for collect_worker in COLLECT_WORKERS:
            collect_worker.start()
        for collect_worker in COLLECT_WORKERS:
            collect_worker.join()
        for server in SERVERS['servers']:
            with open(f'/tmp/{server}-{NOW}_file_list.txt', 'w', encoding='utf-8') as server_file:
                server_file.write(f'List from {NOW}')
                for server_file_entry in sorted(SERVERS[server]['entries']):
                    server_file.write(f'{server_file_entry}\n')
        with open(f'/tmp/ALL_SERVERS-{NOW}_file_list.txt', 'a', encoding='utf-8') as all_file:
            all_file.write(f'List from {NOW}\n')
            for all_entry in sorted(SERVERS['all_files']):
                all_file.write(f'{all_entry}\n')
    exit(0)
