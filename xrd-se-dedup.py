#!/usr/bin/python2
import os
import sys
import multiprocessing
from re import sub, escape, match
from subprocess import call, Popen, PIPE

user = 'root'
port = '22'
# For sorting
os.environ['LC_ALL'] = 'C'

def rds( string ):
    'Removes // from string'
    return sub('//', '/', string)

def build_server_dict_element(s):
    user_s = user
    port_s = port
    if '/' in list(s):
        server, ns = s.split('/', 1)
        ns = rds('/' + ns + '/')
    else:
        sys.exit('There is no name space defined for ' + s)
    if '@' in list(server):
        user_s, server = server.split('@')
    if ':' in list(server):
        server, port_s = server.split(':')
    if not call(['/usr/bin/ssh', '-p', port_s, '-l', user_s, server, '/usr/bin/test -d ' + ns]) == 0:
        sys.exit('Testing connection to server ' + server + ' failed, or defined name space (' + ns + ') is not a directory.')
    return [server, user_s, port_s, ns]


def get_links_on_server(server_tuple):
    server, props = server_tuple
    user = props['user']
    port = props['port']
    ns = props['ns']
    server_out_filename = '/tmp/%s_link.txt' % (server)
    server_log_filename = '/tmp/%s_log.txt' % (server)
    with open(server_out_filename, 'w+') as server_out_file:
        with open(server_log_filename, 'w+') as server_log_file:
            Popen(['/usr/bin/ssh', '-p', port, '-l', user, server, '/bin/find ' + ns + ' -type l -print'], stdout=server_out_file, stderr=server_log_file,).communicate()
    server_out_filename_sorted = '/tmp/%s_link_sorted.txt' % (server)
    call(['/bin/sort', '-o', server_out_filename_sorted, server_out_filename ])
    return [server, server_log_filename, server_out_filename, server_out_filename_sorted]

def return_indexes(list, condition):
    result = []
    i=0
    while i < len(list):
        if list[i] == condition:
            result.append(i)
        i += 1
    return result

def create_ns_entry(server, link):
    nspath = escape(servers[server]['ns'])
    return sub(nspath, '', link)

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
        print(sys.argv[0] + ' [user@]se1.server[:port]/name/space/path [user@]se2.server[:port]/name/space/path ... [user@]seN.server[:port]/name/space/path' + old_args)
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

    #sys.exit(1)
    # results jsou ted seznam serveru
    file_handles = []
    print(servers)
    for s in server_list:
        file_handles.append(open(servers[s]['sortedlinkfile']))

    aktualni_slova = list(map(lambda file_handle: file_handle.readline(), file_handles))
    while len(file_handles) > 1:#nejsme na konci seznamu:
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
                server, link = i
                user = servers[server]['user']
                port = servers[server]['port']
                if call(['/usr/bin/ssh', '-p', port, '-l', user, server, '/usr/bin/test -f $(/bin/readlink ' + link + ')']) == 0:
                    if exists:
                        call(['/usr/bin/ssh', '-p', port, '-l', user, server, '/bin/rm -f $(/bin/readlink ' + link + ') ' + link])
                        deleted += 1
                    else:
                        exists = True
                else:
                    call(['/usr/bin/ssh', '-p', port, '-l', user, server, '/bin/rm -f $(/bin/readlink ' + link + ') ' + link])
                    deleted += 1

        print('Cleanup removed {0} duplicate entries.'.format(deleted))
