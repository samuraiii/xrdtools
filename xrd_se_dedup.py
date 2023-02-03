#!/usr/bin/python3
# vim: set fileencoding=utf-8 :
# Version 1.0.1
import os
import sys
import multiprocessing
from re import sub, escape, match
from subprocess import call, Popen, PIPE

user = 'root'
port = '22'
# For sorting
os.environ['LC_ALL'] = 'C'


def rds(string):
    """Removes // from string"""
    return sub('//', '/', string)


def build_server_dict_element(s_elem):
    user_s = user
    port_s = port
    if '/' in list(s_elem):
        s_server, s_ns = s_elem.split('/', 1)
        s_ns = rds('/' + s_ns + '/')
    else:
        sys.exit('There is no name space defined for ' + s_elem)
    if '@' in list(s_server):
        user_s, s_server = s_server.split('@')
    if ':' in list(s_server):
        s_server, port_s = s_server.split(':')
    if not call(['/usr/bin/ssh', '-p', port_s, '-l', user_s, s_server, '/usr/bin/test -d ' + s_ns]) == 0:
        sys.exit('Testing connection to server ' + s_server + ' failed, or defined name space ('
                 + s_ns + ') is not a directory.')
    return [s_server, user_s, port_s, s_ns]


def get_links_on_server(server_tuple):
    g_server, props = server_tuple
    g_user = props['user']
    g_port = props['port']
    g_ns = props['ns']
    server_out_filename = '/tmp/%s_link.txt' % g_server
    server_log_filename = '/tmp/%s_log.txt' % g_server
    with open(server_out_filename, 'w+') as server_out_file:
        with open(server_log_filename, 'w+') as server_log_file:
            Popen(['/usr/bin/ssh', '-p', g_port, '-l', g_user, g_server,
                   '/bin/find ' + g_ns + ' -type l -print'], stdout=server_out_file,
                  stderr=server_log_file,).communicate()
    server_out_filename_sorted = '/tmp/%s_link_sorted.txt' % g_server
    call(['/bin/sort', '-o', server_out_filename_sorted, server_out_filename])
    return [g_server, server_log_filename, server_out_filename, server_out_filename_sorted]


def return_indexes(r_list, condition):
    result = []
    j = 0
    while j < len(r_list):
        if r_list[j] == condition:
            result.append(j)
        j += 1
    return result


def create_ns_entry(r_server, r_link):
    nspath = escape(servers[r_server]['ns'])
    return sub(nspath, '', r_link)


def rm(rport, ruser, rserver, rlink):
    call(['/usr/bin/ssh', '-p', rport, '-l', ruser, rserver, '/bin/rm -f $(/bin/readlink '
            + rlink + ') ' + rlink])


if __name__ == "__main__":
    link_list = {}
    servers = {}
    deleted = 0
    test = True

    old_args = ''
    if len(sys.argv) < 2:
        old_args = '\nYou gave:\n   ' + ' '.join(sys.argv)
        sys.argv[1:] = ['-h']

    if ('-h' in sys.argv) or ('--help' in sys.argv):
        print('This script is for cleaning xrootd storage of duplicate files.')
        print('This script is to be used in this way:')
        print(sys.argv[0] + ' [user@]se1.server[:port]/name/space/path [user@]se2.server[:port]/name/space/path '
                            '... [user@]seN.server[:port]/name/space/path' + old_args)
        sys.exit(0)

    for st in sys.argv[1:]:
        server, user, port, ns = build_server_dict_element(st)
        servers[server] = {'user': user, 'port': port, 'ns': ns}

    server_list = list(servers.items())
    pool = multiprocessing.Pool(len(server_list))
    results = list(pool.imap(get_links_on_server, server_list))
    pool.terminate()

    for elem in results:
        servers[elem[0]]['logfile'] = elem[1]
        servers[elem[0]]['linkfile'] = elem[2]
        servers[elem[0]]['sortedlinkfile'] = elem[3]

    server_list = list(servers.keys())

    # results je ted seznam serveru
    file_handles = []
    print(servers)
    for s in server_list:
        file_handles.append(open(servers[s]['sortedlinkfile']))

    aktualni_slova = list(map(lambda file_handle: file_handle.readline(), file_handles))
    while len(file_handles) > 1:  # nejsme na konci seznamu:
        aktualni_slova_strip = []
        i = 0
        while i < len(server_list):
            aktualni_slova_strip.append(create_ns_entry(server_list[i], aktualni_slova[i]))

        min_slovo = min(aktualni_slova_strip)
        idx_min = return_indexes(aktualni_slova_strip, min_slovo)
        if len(idx_min) > 1:
            for idx in idx_min:
                link_list.setdefault(min_slovo, []).append(server_list[idx], aktualni_slova[idx])
                dalsi_slovo = file_handles[idx].readline()
                if not dalsi_slovo:
                    file_handles[idx].close()
                    del file_handles[idx]
                    del aktualni_slova[idx]
                    del server_list[idx]
                else:
                    aktualni_slova[idx] = dalsi_slovo
    if test:
        from pprint import pprint
        pprint(link_list)
    else:
        for k in link_list.keys():
            exists = False
            for i in link_list[k]:
                delete = False
                server, link = i
                user = servers[server]['user']
                port = servers[server]['port']
                if call(['/usr/bin/ssh', '-p', port, '-l', user, server, '/usr/bin/test -f $(/bin/readlink '
                                                                         + link + ')']) == 0:
                    if exists:
                        delete = True
                    else:
                        exists = True
                else:
                    delete = True

                if delete:
                    rm(port, user, server, link)
                    deleted += 1

        print('Cleanup removed {0} duplicate entries.'.format(deleted))
