import PyMyHealth
import mysql.connector
from asciimatics.screen import Screen
from asciimatics.constants import *
from asciimatics.event import KeyboardEvent
from asciimatics.exceptions import StopApplication
from asciimatics.paths import DynamicPath
from time import sleep
import argparse
import sys

# todo Handle screen resize
# todo Handle keyboard input
# todo Add optional screens for other info
# todo Optionally add mouse support

parser = argparse.ArgumentParser(description='PyMyHealth is top for MySQL')
parser.add_argument('-d', '--database', nargs='?', help='Database', default='INFORMATION_SCHEMA', type=str)
parser.add_argument('-u', '--user', nargs='?', help='User', default='root', type=str)
parser.add_argument('-p', '--password', nargs='?', help='Password', default='', type=str)
parser.add_argument('-H', '--host', nargs='?', help='Host to connect to', default='localhost', type=str)
parser.add_argument('-P', '--port', nargs='?', help='Port', default='3306', type=int)
parser.add_argument('-S', '--socket', nargs='?', help='Socket', type=str)
parser.add_argument('-r', '--refresh', nargs='?', help='Screen refresh rate', default='2', type=int)
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


class KeyboardController(DynamicPath):
    def process_event(self, event):
        if isinstance(event, KeyboardEvent):
            if event.key_code in [ord('q'), ord('Q'), Screen.ctrl('c')]:
                raise StopApplication("User quit")
            else:
                return event
        else:
            return event


def pymyhealth(screen):
    running = True
    while running:
        if screen.has_resized:
            screen.force_update()
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
        if sys.platform == 'win32':
            light_blue = COLOUR_WHITE
            light_grey = COLOUR_BLACK
            orange = COLOUR_MAGENTA
        else:
            light_blue = 236
            light_grey = 238
            orange = 166
        screen.print_at('_' * width, 0, 0, COLOUR_BLACK, 0, light_blue)
        screen.print_at(' ' * width, 0, 1, 4, 0, light_grey)
        screen.print_at(' ' * width, 0, 2, light_grey, 0, light_grey)
        screen.print_at(' ' * width, 0, 3, light_grey, 0, light_grey)
        screen.print_at(' ' * width, 0, 4, light_grey, 0, light_grey)
        screen.print_at(' ' * width, 0, 5, light_grey, 0, light_grey)
        screen.print_at('_' * width, 0, 6, COLOUR_BLUE, 0, light_blue)
        # first line
        screen.print_at('Py', 0, 0, COLOUR_BLUE, 0, light_blue)
        screen.print_at('My', 2, 0, COLOUR_CYAN, 0, light_blue)
        screen.print_at('SQL ', 4, 0, orange, 0, light_blue)
        screen.print_at('Health', 8, 0, COLOUR_CYAN, 0, light_blue)
        # center labels
        screen.print_at('Now | Avg', center - 5, 0, COLOUR_BLUE, 0, light_blue)
        for i in range(7):
            screen.print_at('|', center - 1, i, COLOUR_BLUE, 0, light_blue)
        screen.print_at(info['version_comment'], center - 37, 0, COLOUR_CYAN, 0, light_blue)
        screen.print_at(info['version'], center - 44, 0, COLOUR_CYAN, 0, light_blue)
        screen.print_at(info['user'] + ' on ' + info['hostname'] + ':' + str(info['port']),
                        center + 8, 0, COLOUR_CYAN, 0, light_blue)
        uptime_string = 'Uptime: ' + uptime
        screen.print_at(uptime_string, width - len(uptime_string), 0, COLOUR_BLUE, 0, light_blue)
        screen.print_at('Net In/Out:', center - 30, 1, COLOUR_CYAN, 0, light_grey)
        screen.print_at('QPS:', center - 23, 2, COLOUR_CYAN, 0, light_grey)
        screen.print_at('Innodb R/W:', center - 30, 3, COLOUR_CYAN, 0, light_grey)
        screen.print_at('Slow QPS:', center - 28, 4, COLOUR_CYAN, 0, light_grey)
        # query dist
        screen.print_at('Sel: ', 0, 1, COLOUR_CYAN, 0, light_grey)
        screen.print_at('Ins: ', 0, 2, COLOUR_CYAN, 0, light_grey)
        screen.print_at('Upd: ', 0, 3, COLOUR_CYAN, 0, light_grey)
        screen.print_at('Del: ', 0, 4, COLOUR_CYAN, 0, light_grey)
        screen.print_at(f"{bytesize(query_dist['select'], '')} / {query_dist['select_pct']}%",
                        5, 1, orange, 0, light_grey)
        screen.print_at(f"{bytesize(query_dist['insert'], '')} / {query_dist['insert_pct']}%",
                        5, 2, orange, 0, light_grey)
        screen.print_at(f"{bytesize(query_dist['update'], '')} / {query_dist['update_pct']}%",
                        5, 3, orange, 0, light_grey)
        screen.print_at(f"{bytesize(query_dist['delete'], '')} / {query_dist['delete_pct']}%",
                        5, 4, orange, 0, light_grey)
        # thread distribution
        screen.print_at('Threads', center + 30, 1, COLOUR_CYAN, 0, light_grey)
        screen.print_at('Conn:', center + 24, 2, COLOUR_CYAN, 0, light_grey)
        screen.print_at('Running:', center + 21, 3, COLOUR_CYAN, 0, light_grey)
        screen.print_at('Max:', center + 25, 4, COLOUR_CYAN, 0, light_grey)
        screen.print_at('SysMax:', center + 22, 5, COLOUR_CYAN, 0, light_grey)
        screen.print_at(f"{thread_dist['connected']} ({int(thread_dist['conn_pct'])}%)",
                        center + 30, 2, orange, 0, light_grey)
        screen.print_at(f"{thread_dist['running']}", center + 30, 3, orange, 0, light_grey)
        screen.print_at(f"{thread_dist['max_threads']} ({int(thread_dist['max_conn_pct'])}%)",
                        center + 30, 4, orange, 0, light_grey)
        screen.print_at(f"{thread_dist['max_conn']}", center + 30, 5, orange, 0, light_grey)
        # qps info
        qps_now_str = f"{qps_delta:06}"
        screen.print_at(qps_now_str, center - len(qps_now_str) - 2, 2, orange, 0, light_grey)
        screen.print_at(f"{int(qps):06}", center + 1, 2, orange, 0, light_grey)
        # key efficiency
        screen.print_at(f"Key Eff: ", 0, 5, COLOUR_CYAN, 0, light_grey)
        screen.print_at(f"{key_efficiency:3}%", 9, 5, orange, 0, light_grey)
        # network
        net_string_avg = f"{bytesize(network_avg['bytes_in_avg'])}/{bytesize(network_avg['bytes_out_avg'])}"
        net_string_now = f"{bytesize(network_delta['bytes_in'])}/{bytesize(network_delta['bytes_out'])}"
        screen.print_at(net_string_now, center - len(net_string_now) - 2, 1, orange, 0, light_grey)
        screen.print_at(net_string_avg, center + 1, 1, orange, 0, light_grey)
        # innodb io
        innodb_io_now = f"{bytesize(innodb_io_stats_delta['innodb_data_in'])}/" \
                        f"{bytesize(innodb_io_stats_delta['innodb_data_out'])}"
        screen.print_at(innodb_io_now, center - len(innodb_io_now) - 2, 3, orange, 0, light_grey)
        screen.print_at(
            f"{bytesize(int(innodb_io_stats['data_in_avg']))}/"
            f"{bytesize(int(innodb_io_stats['data_out_avg']))}", center + 1, 3, orange, 0, light_grey)
        # innodb buffer pool
        buff_pool_label1 = "InnoDB Buff"
        screen.print_at('Size:', width - 14, 2, COLOUR_CYAN, 0, light_grey)
        screen.print_at('Usage:', width - 15, 3, COLOUR_CYAN, 0, light_grey)
        screen.print_at('Eff:', width - 13, 4, COLOUR_CYAN, 0, light_grey)
        buff_pool_str2 = f"{bytesize(buff_pool['pool_size'])}"
        buff_pool_str3 = f"{int(buff_pool['buffer_pool_pct'])}%"
        buff_pool_str4 = f"{int(buff_pool['pool_hit_ratio'])}%"
        screen.print_at(buff_pool_label1, width - 11, 1, COLOUR_CYAN, 0, light_grey)
        screen.print_at(buff_pool_str2, width - len(buff_pool_str2), 2, orange, 0, light_grey)
        screen.print_at(buff_pool_str3, width - len(buff_pool_str3), 3, orange, 0, light_grey)
        screen.print_at(buff_pool_str4, width - len(buff_pool_str4), 4, orange, 0, light_grey)
        # slow qps
        slow_qps_now = f"{bytesize(now_qps, '')}"
        screen.print_at(slow_qps_now, center - len(slow_qps_now) - 2, 4, orange, 0, light_grey)
        screen.print_at(f"{bytesize(slow_qps, '')}", center + 1, 4, orange, 0, light_grey)
        # screen.print_at(f"{width} / {height}", center - 15, 10, 166, 0, 238)
        # processlist labels
        screen.print_at("TID", 4, 6, COLOUR_CYAN, 0, light_grey)
        screen.print_at("USER", 14, 6, COLOUR_CYAN, 0, light_grey)
        screen.print_at("SRC", 20, 6, COLOUR_CYAN, 0, light_grey)
        screen.print_at("DB", 38, 6, COLOUR_CYAN, 0, light_grey)
        screen.print_at("CMD", 41, 6, COLOUR_CYAN, 0, light_grey)
        screen.print_at("TIME", 51, 6, COLOUR_CYAN, 0, light_grey)
        screen.print_at("STATE", 58, 6, COLOUR_CYAN, 0, light_grey)
        screen.print_at("QUERY", 66, 6, COLOUR_CYAN, 0, light_grey)
        # processlist
        line = 7
        for row in processlist:
            output = [row[0], row[1][:10]]
            if row[2]:
                temp = row[2]
                temp = temp.split(':')
                output.append(temp[0])
            else:
                output.append('')
            if row[3]:
                output.append(row[3][:10])
            else:
                output.append(row[3])
            output.append(row[4])
            output.append(row[5])
            if row[6]:
                output.append(row[6][:9])
            else:
                output.append(row[6])
            if row[7]:
                output.append(row[7].replace("\n", ' ').replace("  ", ""))
            else:
                output.append(row[7])
            screen.print_at("{0!s:>7s} {1!s:>10} {2!s:>10} {3!s:>10} {4!s:7} "
                            "{5!s:>6} {6!s:<9} {7!s:155}".format(*output), 0, line, orange, 0, light_grey)
            line += 1
            if line == height:
                break
        screen.refresh()
        sleep(args.refresh)


Screen.wrapper(pymyhealth, catch_interrupt=False)
