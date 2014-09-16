sql="""
SELECT
    waiting.locktype           AS waiting_locktype,
    waiting.relation::regclass AS waiting_table,
    waiting_stm.current_query  AS waiting_query,
    waiting.mode               AS waiting_mode,
    waiting.pid                AS waiting_pid,
    other.locktype             AS other_locktype,
    other.relation::regclass   AS other_table,
    other_stm.current_query    AS other_query,
    other.mode                 AS other_mode,
    other.pid                  AS other_pid,
    other.granted              AS other_granted
FROM
    pg_catalog.pg_locks AS waiting
JOIN
    pg_catalog.pg_stat_activity AS waiting_stm
    ON (
        waiting_stm.procpid = waiting.pid
    )
JOIN
    pg_catalog.pg_locks AS other
    ON (
        (
            waiting."database" = other."database"
        AND waiting.relation  = other.relation
        )
        OR waiting.transactionid = other.transactionid
    )
JOIN
    pg_catalog.pg_stat_activity AS other_stm
    ON (
        other_stm.procpid = other.pid
    )
WHERE
    NOT waiting.granted
AND
    waiting.pid <> other.pid
AND
    other.granted
"""

import os
from textwrap import wrap

import psycopg2
import psycopg2.extras
from graphviz import Digraph


path = os.getenv("PATH")
path += ';c:/program files (x86)/graphviz2.38/bin'
os.putenv("PATH", path)

dbhost= raw_input("host: ")
dbdb = raw_input("database: ")
dbuser= raw_input("username: ")
dbpw= raw_input("password: ")

conn = psycopg2.connect(host=dbhost, user=dbuser, database=dbdb, password=dbpw)

crs = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
crs.execute(sql)

dot = Digraph()

tables = []
pids = []
edges = []
for record in crs:
    other_pid = record["other_pid"]
    waiting_pid = record["waiting_pid"]
    table = record['other_table']

    if other_pid not in pids:
        dot.node( str(other_pid), record["other_query"], _attributes={"color" : "red", "style" : "filled"})
        pids.append( other_pid)

    if waiting_pid not in pids:
        dot.node( str(waiting_pid), '\r\n'.join(wrap(record["waiting_query"], 30)), _attributes={"fontsize" : "8"})
        pids.append( waiting_pid)

    if table not in tables:
        dot.node(table, _attributes={"shape" : "rectangle"})


        tables.append(table)

    edge = ( str(other_pid), table)
    if edge not in edges:
        dot.edge( edge[0], edge[1])
        edges.append(edge)

    edge = ( str(waiting_pid), str(other_pid))
    if edge not in edges:
        dot.edge( edge[0], edge[1])
        edges.append(edge)

dot.render('c:/temp/graphen.gb', view=True)







