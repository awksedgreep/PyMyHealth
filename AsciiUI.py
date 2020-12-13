import PyMyHealth
import mysql.connector
from asciimatics.screen import Screen
from asciimatics.constants import *
from time import sleep
import re
import argparse

parser = argparse.ArgumentParser(description='PyMyHealth is top for MySQL')
parser.add_argument('-d', '--database', nargs='?', help='Database', default='INFORMATION_SCHEMA', type=str)
parser.add_argument('-u', '--user', nargs='?', help='User', default='root', type=str)
parser.add_argument('-p', '--password', nargs='?', help='Password', default='', type=str)
parser.add_argument('-H', '--host', nargs='?', help='Host to connect to', default='localhost', type=str)
parser.add_argument('-P', '--port', nargs='?', help='Port', default='3306', type=int)
parser.add_argument('-S', '--socket', nargs='?', help='Socket', type=str)
parser.add_argument('-r', '--refresh', nargs='?', help='Screen refresh rate', default='2', type=str)
parser.add_argument('-v', '--verbose', help='Verbose/Debug output', default=False)

args = parser.parse_args()

if args.socket:
    conn = mysql.connector.connect(user=args.user, password=args.password,
                                   host=args.host, unix_socket=args.socket,
                                   database=args.database)
else:
    conn = mysql.connector.connect(user=args.user, password=args.password,
                                   host=args.host, database=args.database)

health = PyMyHealth.PyMyHealth(conn, args.verbose)


def bytesize(num, suffix='B'):
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


def pymyhealth(screen):
    running = True
    while running:
        info = health.info()
        qps = health.get_qps()
        qps_delta = health.get_qps_delta()
        uptime = health.pretty_uptime()
        query_dist = health.query_distribution()
        thread_dist = health.thread_distribution()
        key_efficiency = health.key_efficiency()
        network_avg = health.get_network()
        network_delta = health.get_network_delta()
        innodb_io_stats = health.innodb_io_stats()
        innodb_io_stats_delta = health.innodb_io_stats_delta()
        slow_qps = health.get_slow_qps()
        now_qps = health.get_qps_delta()
        processlist = health.get_processlist()
        buff_pool = health.innodb_buffer_pool_status()
        height, width = screen.dimensions
        center = int(width / 2)
        # fill in for color header
        screen.print_at('-' * width, 0, 0, COLOUR_BLACK, 0, 236)
        screen.print_at(' ' * width, 0, 1, 238, 0, 238)
        screen.print_at(' ' * width, 0, 2, 238, 0, 238)
        screen.print_at(' ' * width, 0, 3, 238, 0, 238)
        screen.print_at(' ' * width, 0, 4, 238, 0, 238)
        screen.print_at(' ' * width, 0, 5, 238, 0, 238)
        screen.print_at('-' * width, 0, 6, COLOUR_BLUE, 0, 236)
        # first line
        screen.print_at('Py', 0, 0, COLOUR_BLUE, 0, 236)
        screen.print_at('My', 2, 0, COLOUR_CYAN, 0, 236)
        screen.print_at('SQL ', 4, 0, 166, 0, 236)
        screen.print_at('Health', 8, 0, COLOUR_CYAN, 0, 236)
        # center labels
        screen.print_at('Now : Avg', center - 5, 0, COLOUR_BLUE, 0, 236)
        screen.print_at(':', center - 1, 1, COLOUR_BLUE, 0, 236)
        screen.print_at(':', center - 1, 2, COLOUR_BLUE, 0, 236)
        screen.print_at(':', center - 1, 3, COLOUR_BLUE, 0, 236)
        screen.print_at(':', center - 1, 4, COLOUR_BLUE, 0, 236)
        screen.print_at(':', center - 1, 5, COLOUR_BLUE, 0, 236)
        screen.print_at(info['version_comment'], center - 37, 0, COLOUR_CYAN, 0, 236)
        screen.print_at(info['version'], center - 44, 0, COLOUR_CYAN, 0, 236)
        screen.print_at(info['user'] + ' on ' + info['hostname'] + ':' + str(info['port']),
                        center + 8, 0, COLOUR_CYAN, 0, 236)
        uptime_string = 'Uptime: ' + uptime
        screen.print_at(uptime_string, width - len(uptime_string), 0, COLOUR_BLUE, 0, 236)
        screen.print_at('Net In/Out:', center - 30, 1, COLOUR_CYAN, 0, 238)
        screen.print_at('QPS:', center - 23, 2, COLOUR_CYAN, 0, 238)
        screen.print_at('Innodb R/W:', center - 30, 3, COLOUR_CYAN, 0, 238)
        screen.print_at('Slow QPS:', center - 28, 4, COLOUR_CYAN, 0, 238)
        # query dist
        screen.print_at('Sel: ', 0, 1, COLOUR_CYAN, 0, 238)
        screen.print_at('Ins: ', 0, 2, COLOUR_CYAN, 0, 238)
        screen.print_at('Upd: ', 0, 3, COLOUR_CYAN, 0, 238)
        screen.print_at('Del: ', 0, 4, COLOUR_CYAN, 0, 238)
        screen.print_at(f"{bytesize(query_dist['select'], '')} / {query_dist['select_pct']}%", 5, 1, 166, 0, 238)
        screen.print_at(f"{bytesize(query_dist['insert'], '')} / {query_dist['insert_pct']}%", 5, 2, 166, 0, 238)
        screen.print_at(f"{bytesize(query_dist['update'], '')} / {query_dist['update_pct']}%", 5, 3, 166, 0, 238)
        screen.print_at(f"{bytesize(query_dist['delete'], '')} / {query_dist['delete_pct']}%", 5, 4, 166, 0, 238)
        # thread distribution
        screen.print_at('Threads', center + 30, 1, COLOUR_CYAN, 0, 238)
        screen.print_at('Conn:', center + 24, 2, COLOUR_CYAN, 0, 238)
        screen.print_at('Running:', center + 21, 3, COLOUR_CYAN, 0, 238)
        screen.print_at('Max:', center + 25, 4, COLOUR_CYAN, 0, 238)
        screen.print_at('SysMax:', center + 22, 5, COLOUR_CYAN, 0, 238)
        screen.print_at(f"{thread_dist['connected']} ({int(thread_dist['conn_pct'])}%)", center + 30, 2, 166, 0, 238)
        screen.print_at(f"{thread_dist['running']}", center + 30, 3, 166, 0, 238)
        screen.print_at(f"{thread_dist['max_threads']} ({int(thread_dist['max_conn_pct'])}%)", center + 30, 4, 166, 0,
                        238)
        screen.print_at(f"{thread_dist['max_conn']}", center + 30, 5, 166, 0, 238)
        # qps info
        qps_now_str = f"{qps_delta:06}"
        screen.print_at(qps_now_str, center - len(qps_now_str) - 2, 2, 166, 0, 238)
        screen.print_at(f"{int(qps):06}", center + 1, 2, 166, 0, 238)
        # key efficiency
        screen.print_at(f"Key Eff: ", 0, 5, COLOUR_CYAN, 0, 238)
        screen.print_at(f"{key_efficiency:3}%", 9, 5, 166, 0, 238)
        # network
        net_string_avg = f"{bytesize(network_avg['bytes_in_avg'])}/{bytesize(network_avg['bytes_out_avg'])}"
        net_string_now = f"{bytesize(network_delta['bytes_in'])}/{bytesize(network_delta['bytes_out'])}"
        screen.print_at(net_string_now, center - len(net_string_now) - 2, 1, 166, 0, 238)
        screen.print_at(net_string_avg, center + 1, 1, 166, 0, 238)
        # innodb io
        innodb_io_now = f"{bytesize(innodb_io_stats_delta['innodb_data_in'])}/" \
                        f"{bytesize(innodb_io_stats_delta['innodb_data_out'])}"
        screen.print_at(innodb_io_now, center - len(innodb_io_now) - 2, 3, 166, 0, 238)
        screen.print_at(
            f"{bytesize(int(innodb_io_stats['data_in_avg']))}/"
            f"{bytesize(int(innodb_io_stats['data_out_avg']))}", center + 1, 3, 166, 0, 238)
        # innodb buffer pool
        buff_pool_label1 = "InnoDB Buff"
        screen.print_at('Size:', width - 14, 2, COLOUR_CYAN, 0, 238)
        screen.print_at('Usage:', width - 15, 3, COLOUR_CYAN, 0, 238)
        screen.print_at('Eff:', width - 13, 4, COLOUR_CYAN, 0, 238)
        buff_pool_str2 = f"{bytesize(buff_pool['pool_size'])}"
        buff_pool_str3 = f"{int(buff_pool['buffer_pool_pct'])}%"
        buff_pool_str4 = f"{int(buff_pool['pool_hit_ratio'])}%"
        screen.print_at(buff_pool_label1, width - 11, 1, COLOUR_CYAN, 0, 238)
        screen.print_at(buff_pool_str2, width - len(buff_pool_str2), 2, 166, 0, 238)
        screen.print_at(buff_pool_str3, width - len(buff_pool_str3), 3, 166, 0, 238)
        screen.print_at(buff_pool_str4, width - len(buff_pool_str4), 4, 166, 0, 238)
        # slow qps
        slow_qps_now = f"{bytesize(now_qps, '')}"
        screen.print_at(slow_qps_now, center - len(slow_qps_now) - 2, 4, 166, 0, 238)
        screen.print_at(f"{bytesize(slow_qps, '')}", center + 1, 4, 166, 0, 238)
        # screen.print_at(f"{width} / {height}", center - 15, 10, 166, 0, 238)
        # processlist labels
        screen.print_at("TID", 4, 6, COLOUR_CYAN, 0, 238)
        screen.print_at("USER", 14, 6, COLOUR_CYAN, 0, 238)
        screen.print_at("SRC", 20, 6, COLOUR_CYAN, 0, 238)
        screen.print_at("DB", 38, 6, COLOUR_CYAN, 0, 238)
        screen.print_at("CMD", 41, 6, COLOUR_CYAN, 0, 238)
        screen.print_at("TIME", 51, 6, COLOUR_CYAN, 0, 238)
        screen.print_at("STATE", 58, 6, COLOUR_CYAN, 0, 238)
        screen.print_at("QUERY", 65, 6, COLOUR_CYAN, 0, 238)
        # processlist
        line = 7
        for row in processlist:
            output = []
            output.append(row[0])
            output.append(row[1][:10])
            if row[2]:
                temp = row[2]
                temp = temp.split(':')
                output.append(re.sub("localhost", "localhost", temp[0]))
            else:
                output.append('')
            output.append(row[3])
            output.append(row[4])
            output.append(row[5])
            if row[6]:
                output.append(row[6][:8])
            else:
                output.append(row[6])
            if row[7]:
                output.append(row[7].replace("\n", '').replace("  ", ""))
            else:
                output.append(row[7])
            screen.print_at("{0!s:>7s} {1!s:>10} {2!s:>10} {3!s:>10} {4!s:7} "
                            "{5!s:>6} {6!s:<8} {7!s:155}".format(*output), 0, line, 166, 0, 238)
            line += 1
            if line == height:
                break
        screen.refresh()
        sleep(2)


Screen.wrapper(pymyhealth)
