
# Todo: Slow report
# Todo: Table Fragmentation
# Todo: trx info

class PyMyHealth:

    def __init__(self, connection, debug=False):
        self.connection = connection
        self.metrics = {}
        self.metric_history = {}
        self.debug = debug

    def get_metric(self, metric_name):
        return self.metrics[metric_name]

    def set_metric(self, metric_name, metric_value):
        if metric_name in self.metrics:
            old = self.metrics[metric_name]
        else:
            old = metric_value
        delta = metric_value - old
        if delta < 0:
            delta = 0
        self.metrics[metric_name] = metric_value
        if metric_name in self.metric_history:
            self.metric_history[metric_name].append(delta)
        else:
            self.metric_history[metric_name] = []
            self.metric_history[metric_name].append(delta)
        if len(self.metric_history[metric_name]) == 21:
            self.metric_history[metric_name].pop(0)
        return delta

    def get_metric_history(self, metric_name):
        return self.metric_history[metric_name]

    def query_metric(self, metric_name, metric_type='status', delta=True):
        if metric_type == 'status':
            query = f'SHOW GLOBAL STATUS LIKE "{metric_name}"'
        else:
            query = f'SHOW GLOBAL VARIABLES LIKE "{metric_name}"'
        cursor = self.connection.cursor()
        cursor.execute(query)
        row = cursor.fetchone()
        if self.debug:
            print(row[0], row[1])
            print(self.metrics)
            print(self.metric_history)
        if metric_type == 'status' and delta:
            return self.set_metric(row[0], int(row[1]))
        else:
            return row[1]

    def info(self):
        query = 'SELECT @@version, @@version_comment, @@hostname, @@port, user()'
        cursor = self.connection.cursor()
        cursor.execute(query)
        row = cursor.fetchone()
        res = {'version': row[0], 'version_comment': row[1],
               'hostname': row[2], 'port': row[3], 'user': row[4]}
        return res

    def pretty_info(self):
        res = self.info()
        return f"{res['hostname']}:{res['port']} {res['version_comment']} {res['version']}"

    def uptime(self):
        query = 'SHOW STATUS LIKE "Uptime"'
        cursor = self.connection.cursor()
        cursor.execute(query)
        row = cursor.fetchone()
        res = int(row[1])
        ret = {
            'days': int(res / 86400),
            'hours': int((res % 86400) / 3600),
            'minutes': int(((res % 86400) % 3600) / 60),
            'seconds': int(((res % 86400) % 3600) % 60)
        }
        if self.debug:
            print(f"Uptime: {ret['days']} Days {ret['hours']} hours {ret['minutes']} min {ret['seconds']} seconds")
        return ret

    def pretty_uptime(self):
        res = self.uptime()
        return "{0:d} Days {1:02d}:{2:02d}:{3:02d}".format(
            res['days'], res['hours'], res['minutes'], res['seconds'])

    def mytop_uptime(self):
        res = self.uptime()
        return "{0:d}+{1:02d}:{2:02d}:{3:02d}".format(
            res['days'], res['hours'], res['minutes'], res['seconds'])

    def query_distribution(self):
        res = {
            'select': self.query_metric('Com_select'),
            'insert': self.query_metric('Com_insert'),
            'update': self.query_metric('Com_update'),
            'delete': self.query_metric('Com_delete')
        }
        total = 0
        if self.debug:
            print(res)
        for key in res:
            total = total + res[key]
        res['total'] = total
        if total > 0:
            res['select_pct'] = (res['select'] / total) * 100
            res['insert_pct'] = (res['insert'] / total) * 100
            res['update_pct'] = (res['update'] / total) * 100
            res['delete_pct'] = (res['delete'] / total) * 100
        else:
            res['select_pct'] = 0
            res['insert_pct'] = 0
            res['update_pct'] = 0
            res['delete_pct'] = 0
        return res

    def pretty_query_distribution(self):
        res = self.query_distribution()
        return f"S:I:U:D-{res['select']}:{res['insert']}:{res['update']}:{res['delete']}" \
               f"-{res['select_pct']}%:{res['insert_pct']}%:{res['update_pct']}%:{res['delete_pct']}%"

    def pretty_query_distribution_columns(self):
        res = self.query_distribution()
        output = "{0:>7s}{1:>7s}{2:>7s}{3:>7s}\n" \
                 "{4:7d}{5:7d}{6:7d}{7:7d}\n" \
                 "{8:>6.0f}%{9:>6.0f}%{10:>6.0f}%{11:>6.0f}%".format(
                     'Sel', 'Ins', 'Upd', 'Del', res['select'], res['insert'], res['update'], res['delete'],
                     res['select_pct'], res['insert_pct'], res['update_pct'], res['delete_pct'])
        return output

    def thread_distribution(self):
        res = {
            'connected':     int(self.query_metric('Threads_connected', 'status', False)),
            'running':       int(self.query_metric('Threads_running', 'status', False)),
            'max_conn':      int(self.query_metric('max_connections', 'variables')),
            'max_threads':   int(self.query_metric('Max_used_connections', 'status', False))
        }
        if res['max_conn'] > 0:
            res['conn_pct'] = (res['connected'] / res['max_conn']) * 100
            res['max_conn_pct'] = (res['max_threads'] / res['max_conn']) * 100
        else:
            res['conn_pct'] = 0
            res['max_conn_pct'] = 0
        return res

    def pretty_thread_distribution_columns(self):
        res = self.thread_distribution()
        output = "{0:>8s}{1:>8s}{2:>8s}{3:>8s}\n" \
                 "{4:8d}{5:8d}{6:8d}{7:8d}\n" \
                 "{8:>7.0f}%{9:>8.0s}{10:>7.0f}%{11:>7.0s}".format(
                     'Threads', 'Running', 'Max', 'MaxConn', res['connected'],
                     res['running'], res['max_threads'], res['max_conn'],
                     res['conn_pct'], ' ', res['max_conn_pct'], ' ')
        return output

    def key_efficiency(self):
        key_reads = int(self.query_metric('Key_reads', 'status', False))
        key_read_requests = int(self.query_metric('Key_read_requests', 'status', False))
        if key_read_requests > 0:
            return (1 - (key_reads / key_read_requests)) * 100
        else:
            return 0

    def key_efficiency_delta(self):
        key_reads = int(self.query_metric('Key_reads'))
        key_read_requests = int(self.query_metric('Key_read_requests'))
        if key_read_requests > 0:
            return (1 - (key_reads / key_read_requests)) * 100
        else:
            return 0

    def innodb_buffer_pool_status(self):
        buffer_pool_data = int(self.query_metric('Innodb_buffer_pool_pages_data', 'status', False))
        buffer_pool_misc = int(self.query_metric('Innodb_buffer_pool_pages_misc', 'status', False))
        buffer_pool_free = int(self.query_metric('Innodb_buffer_pool_pages_free', 'status', False))
        buffer_pool_pages = buffer_pool_data + buffer_pool_misc + buffer_pool_free
        buffer_pool_pages_used = buffer_pool_data + buffer_pool_misc
        if buffer_pool_pages > 0:
            buffer_pool_pct = (buffer_pool_pages_used / buffer_pool_pages) * 100
        else:
            buffer_pool_pct = 0
        page_size = int(self.query_metric('Innodb_page_size', 'stats', False))
        pool_size = buffer_pool_pages * page_size
        read_requests = int(self.query_metric('Innodb_buffer_pool_read_requests', 'status', False))
        pool_reads = int(self.query_metric('Innodb_buffer_pool_reads'))
        if read_requests + pool_reads > 0:
            pool_hit_ratio = (read_requests / (read_requests + pool_reads)) * 100
        else:
            pool_hit_ratio = 0
        res = {
            'buffer_pool_data':  buffer_pool_data,
            'buffer_pool_misc':  buffer_pool_misc,
            'buffer_pool_free':  buffer_pool_free,
            'buffer_pool_pages': buffer_pool_pages,
            'page_size':         page_size,
            'read_requests':     read_requests,
            'pool_reads':        pool_reads,
            'pool_hit_ratio':    pool_hit_ratio,
            'buffer_pool_pct':   buffer_pool_pct,
            'pool_size':         pool_size
        }
        return res

    def innodb_io_stats(self):
        innodb_data_in = int(self.query_metric('Innodb_data_read', 'status', False))
        innodb_data_out = int(self.query_metric('Innodb_data_written', 'status', False))
        seconds_since_reset = int(self.query_metric('Uptime_since_flush_status', 'status', False))
        if seconds_since_reset > 0:
            data_in_sec = innodb_data_in / seconds_since_reset
            data_out_sec = innodb_data_out / seconds_since_reset
        else:
            data_in_sec = 0
            data_out_sec = 0
        res = {
            'innodb_data_in':  innodb_data_in,
            'innodb_data_out': innodb_data_out,
            'seconds_since_reset': seconds_since_reset,
            'data_out_avg': data_out_sec,
            'data_in_avg':  data_in_sec
        }
        return res

    def innodb_io_stats_delta(self):
        return {
            'innodb_data_in': int(self.query_metric('Innodb_data_read')),
            'innodb_data_out': int(self.query_metric('Innodb_data_written'))
        }

    def get_qps(self):
        questions = int(self.query_metric('Questions', 'status', False))
        seconds_since_reset = int(self.query_metric('Uptime_since_flush_status', 'status', False))
        if seconds_since_reset > 0:
            qps = questions / seconds_since_reset
        else:
            qps = 0
        return qps

    def get_qps_delta(self):
        return int(self.query_metric('Questions'))

    def get_network(self):
        res = {
            'bytes_in': int(self.query_metric('Bytes_received', 'status', False)),
            'bytes_out': int(self.query_metric('Bytes_sent', 'status', False))
        }
        seconds_since_reset = int(self.query_metric('Uptime_since_flush_status', 'status', False))
        if seconds_since_reset > 0:
            res['bytes_in_avg'] = int(res['bytes_in'] / seconds_since_reset)
            res['bytes_out_avg'] = int(res['bytes_out'] / seconds_since_reset)
        else:
            res['bytes_in_avg'] = 0
            res['bytes_out_avg'] = 0
        return res

    def get_network_delta(self):
        return {
            'bytes_in': int(self.query_metric('Bytes_received')),
            'bytes_out': int(self.query_metric('Bytes_sent'))
        }

    def get_slow_qps(self):
        seconds_since_reset = int(self.query_metric('Uptime_since_flush_status', 'status', False))
        slow_queries = int(self.query_metric('Slow_queries'))
        if seconds_since_reset > 0:
            return int(slow_queries / seconds_since_reset)
        else:
            return 0

    def get_slow_delta(self):
        return int(self.query_metric('Slow_queries'))

    def check_for_lock_waits_table(self):
        query = "select count(1) from information_schema.tables where table_name = 'INNODB_LOCK_WAITS'"
        cursor = self.connection.cursor()
        cursor.execute(query)
        row = cursor.fetchone()
        return row[0]

    def get_lock_waits(self):
        cursor = self.connection.cursor()
        if self.check_for_lock_waits_table():
            query = """
                SELECT
                 r.trx_mysql_thread_id                            AS waiting_thread,
                 r.trx_query                                      AS waiting_query,
                 "n/a"                                            AS waiting_rows_modified,
                 TIMESTAMPDIFF(SECOND, r.trx_started, NOW())      AS waiting_age,
                 TIMESTAMPDIFF(SECOND, r.trx_wait_started, NOW()) AS waiting_wait_secs,
                 rp.user                                          AS waiting_user,
                 rp.host                                          AS waiting_host,
                 rp.db                                            AS waiting_db,
                 b.trx_mysql_thread_id                            AS blocking_thread,
                 b.trx_query                                      AS blocking_query,
                 "n/a"                                            AS blocking_rows_modified,
                 TIMESTAMPDIFF(SECOND, b.trx_started, NOW())      AS blocking_age,
                 TIMESTAMPDIFF(SECOND, b.trx_wait_started, NOW()) AS blocking_wait_secs,
                 bp.user                                          AS blocking_user,
                 bp.host                                          AS blocking_host,
                 bp.db                                            AS blocking_db,
                 CONCAT(bp.command, IF(bp.command = 'Sleep', CONCAT(' ', bp.time),   '')) AS blocking_status,
                 CONCAT(lock_mode, ' ', lock_type, ' ', lock_table, '(', lock_index, ')') AS lock_info
                FROM INFORMATION_SCHEMA.INNODB_LOCK_WAITS w
                JOIN INFORMATION_SCHEMA.INNODB_TRX b   ON  b.trx_id  = w.blocking_trx_id
                JOIN INFORMATION_SCHEMA.INNODB_TRX r   ON  r.trx_id  = w.requesting_trx_id
                JOIN INFORMATION_SCHEMA.INNODB_LOCKS l ON  l.lock_id = w.requested_lock_id
                LEFT JOIN INFORMATION_SCHEMA.PROCESSLIST bp ON bp.id = b.trx_mysql_thread_id
                LEFT JOIN INFORMATION_SCHEMA.PROCESSLIST rp ON rp.id = r.trx_mysql_thread_id"""
        else:
            query = """
                SELECT
                   r.trx_mysql_thread_id                            AS waiting_thread,
                   r.trx_query                                      AS waiting_query,
                   r.trx_rows_modified                              AS waiting_rows_modified,
                   TIMESTAMPDIFF(SECOND, r.trx_started, NOW())      AS waiting_age,
                   TIMESTAMPDIFF(SECOND, r.trx_wait_started, NOW()) AS waiting_wait_secs,
                   rp.user                                          AS waiting_user,
                   rp.host                                          AS waiting_host,
                   rp.db                                            AS waiting_db,
                   b.trx_mysql_thread_id                            AS blocking_thread,
                   b.trx_query                                      AS blocking_query,
                   b.trx_rows_modified                              AS blocking_rows_modified,
                   TIMESTAMPDIFF(SECOND, b.trx_started, NOW())      AS blocking_age,
                   TIMESTAMPDIFF(SECOND, b.trx_wait_started, NOW()) AS blocking_wait_secs,
                   bp.user                                          AS blocking_user,
                   bp.host                                          AS blocking_host,
                   bp.db                                            AS blocking_db,
                   CONCAT(bp.command, IF(bp.command = 'Sleep', CONCAT(' ', bp.time),   '')) AS blocking_status,
                   CONCAT(lock_mode, ' ', lock_type, ' ', object_schema, object_name, '(', index_name, ')') AS lock_info
                FROM performance_schema.data_lock_waits w
               JOIN INFORMATION_SCHEMA.INNODB_TRX b   ON  b.trx_id  = w.blocking_engine_transaction_id
               JOIN INFORMATION_SCHEMA.INNODB_TRX r   ON  r.trx_id  = w.requesting_engine_transaction_id
               JOIN performance_schema.data_locks l ON  l.engine_lock_id = w.requesting_engine_lock_id
               LEFT JOIN INFORMATION_SCHEMA.PROCESSLIST bp ON bp.id = b.trx_mysql_thread_id
               LEFT JOIN INFORMATION_SCHEMA.PROCESSLIST rp ON rp.id = r.trx_mysql_thread_id;
            """
        cursor.execute(query)
        rows = []
        for row in cursor:
            rows.append(row)
        return rows

    def get_processlist(self, sleeping=False, sys=False, event_scheduler=False):
        cursor = self.connection.cursor()
        query = """
                SELECT
                 ID,
                 USER,
                 HOST,
                 DB,
                 COMMAND,
                 TIME,
                 STATE,
                 INFO
                FROM INFORMATION_SCHEMA.PROCESSLIST
                WHERE 1 = 1 """
        if not sleeping:
            query += "AND COMMAND != 'Sleep' "
        if not sys:
            query += "AND USER != 'system user' "
        if not event_scheduler:
            query += "AND USER != 'event_scheduler' "
        query += "ORDER BY TIME DESC"
        if self.debug:
            print(query)
        cursor.execute(query)
        rows = []
        for row in cursor:
            rows.append(row)
        return rows
