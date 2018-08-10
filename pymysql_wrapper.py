"""
A lightweight wrapper for PyMySQL
Methods:
    get_one() - get a single row
    get_all() - get all rows
    last_id() - get the last insert id
    insert() - insert a row
    update() - update rows
    delete() - delete rows
    query()  - run a raw sql query
    commit() - commit current sql
    is_open() - check connection alive
"""
import pymysql
from collections import namedtuple


class DataBase:
    conn = None
    cur = None
    conf = None

    def __init__(self, **kwargs):
        self.conf = kwargs
        self.connect()

    def __del__(self):
        self.cur.close()
        self.conn.close()

    def connect(self):
        """
        Connect to the mysql server
        """
        try:
            self.conn = pymysql.connect(**self.conf)
            self.cur = self.conn.cursor()
        except ConnectionError:
            print('%s@%s\nConnection Failed!' % (self.conf['user'], self.conf['host']))
            raise

    def get_one(self, table=None, fields='*', where=None, order=None, limit=(0, 1)):
        """
        Get a single result

        :param table: table name
        :param fields: list of fields to select
        :param where: ('id=%s and name=%s', [1, 'test'])
        :param order: [filed, ASC|DESC]
        :param limit: [limit1, limit2]
        :return: single result
        """

        cur = self._select(table, fields, where, order, limit)
        result = cur.fetchone()

        row = None
        if result:
            Row = namedtuple('Row', [f[0] for f in cur.description])
            row = Row(*result)

        return row

    def get_all(self, table=None, fields='*', where=None, order=None, limit=None):
        """
        Get all results

        :param table: table name
        :param fields: list of fields to select
        :param where: ('id=%s and name=%s', [1, 'test'])
        :param order: [filed, ASC|DESC]
        :param limit: [limit1, limit2]
        :return: single result
        """

        cur = self._select(table, fields, where, order, limit)
        result = cur.fetchall()

        rows = None
        if result:
            Row = namedtuple('Row', [f[0] for f in cur.description])
            rows = [Row(*r) for r in result]

        return rows

    def last_id(self):
        return self.cur.lastrowid

    def insert(self, table, data):
        """
        Insert a record
        """

        query = self._serialize_insert(data)
        sql = 'INSERT INTO %s (%s) VALUES (%s)' % (table, query[0], query[1])

        return self.query(sql, tuple(data.values())).rowcount

    def update(self, table, data, where=None):
        """
        Update a record
        """
        query = self._serialize_update(data)

        sql = 'UPDATE %s SET %s' % (table, query)
        if where and len(where) > 0:
            sql += ' WHERE %s' % where[0]

        return self.query(
            sql, tuple(data.values()) + where[1] if where and len(where) > 1
            else tuple(data.values())).rowcount

    def delete(self, table, where=None):
        """
        Delete rows based on a where condition
        """
        sql = 'DELETE FROM %s' % table
        if where and len(where) > 0:
            sql += ' WHERE %s' % where[0]
        return self.query(
            sql, where[1] if where and len(where) > 1 else None).rowcount

    def query(self, sql, params=None):
        """
        Run a raw query
        """

        # check if connection is alive, else reconnect
        try:
            self.cur.execute(sql, params)
        except pymysql.OperationalError:
            # todo fix timeout and reconnect
            self.connect()
            self.cur.execute(sql, params)
        except IOError:
            print('%s\nQuery Failed!' % sql)
            raise

        return self.cur

    def commit(self):
        return self.conn.commit()

    def is_open(self):
        return self.conn.open

    # non-public methods

    def _serialize_insert(self, data):
        """
        Format input dict into string
        """
        keys = ','.join(data.keys())
        values = ','.join(['%s'] * len(data))

        return [keys, values]

    def _serialize_update(self, data):
        """
        Format input dict into string
        """
        return '=%s'.join(data.keys()) + '=%s'

    def _select(self, table=None, fields=(), where=None, order=None, limit=None):
        """
        Run a select query
        """

        sql = 'SELECT %s FROM `%s`' % (','.join(fields), table)

        if where and len(where) > 0:
            sql += ' WHERE %s' % where[0]

        if order:
            sql += ' ORDER BY %s' % order[0]

            if len(order) > 1:
                sql += ' %s' % order[1]

        if limit:
            sql += ' LIMIT %s' % limit[0]

            if len(limit) > 1:
                sql += ', %s' % limit[1]

        return self.query(sql, where[1] if where and len(where) > 1 else None)
