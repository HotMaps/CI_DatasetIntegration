import psycopg2
import sys
from pprint import pprint

class DBError(Exception):
    '''base error'''
    pass

class DBInsertError(DBError):
    '''raised when something happened during insertion of data'''
    def __init__(self, expression, message):
        self.expression = expression
        self.message = message


def str_with_quotes(obj):
    return '"' + str(obj) + '"'


def str_with_single_quotes(obj):
    try:
        if obj.startswith('ST_GeomFromEWKT') or obj.startswith('ST_GeomFromText'):
            return str(obj)
    except:
        pass
    try:
        if obj == 'NULL':
            return str(obj)
    except:
        pass
    try:
        return '\'' + obj.replace("'", "''") + '\''
    except:
        return str(obj)


class DB(object):
    conn_string = ''
    conn = None

    """docstring for DB."""

    def __init__(self, host, port, database, user, password):
        super(DB, self).__init__()
        self.conn_string = conn_string="host='" + host + "' " + \
                              "port='" + port + "' " + \
                              "dbname='" + database + "' " + \
                              "user='" + user + "' " + \
                              "password='" + password + "'"
        try:
            self.conn = psycopg2.connect(conn_string)
        except psycopg2.Error as e:
            print(e)
            self.close_connection()
            raise

    def query(self, query, data=None, commit=False, notices=False):
        try:
            print("executing Query: ", query)
            cursor = self.conn.cursor()
            cursor.execute(query, data)
            if notices:
                pprint(self.conn.notices)
            if commit:
                self.conn.commit()

            if cursor.description:
                return cursor.fetchall()
            else:
                return None

        except psycopg2.Error as e:
            print(e)
            self.close_connection()
            raise

    def insert(self, table, columns, types, values, commit=False, notices=False):
        # check lenght of parameters
        if len(columns) != len(types) or len(columns) != len(values):
            print('column names, types or values don\'t match.')
            print(columns, len(columns))
            print(types, len(types))
            print(values, len(values))
            raise DBInsertError('Error while trying to insert data in database', 'Column names, types or values don\'t match.')

        # build query
        query = 'INSERT INTO ' + table + ' '
        # add columns
        query = query + '(' + ', '.join([x for x in columns]) + ') '
        # add values
        query = query + 'VALUES('
        for i, val in enumerate(values):
            if i > 0:
                query = query + ", "
            if types[i].startswith('varchar') or types[i] == 'datetime' or types[i].startswith('numeric') or types[i] == 'bigint' or types[i] == 'boolean' or types[i] == 'timestamp':
                query = query + "%s"
            elif types[i] == 'geometry':
                query = query + "ST_GeomFromEWKT(%s)"
        query = query + ') '
        return self.query(query=query, data=values, commit=commit, notices=notices)

    def drop_table(self, table_name, cascade=False, notices=False):
        try:
            print('Droping table ', table_name)
            cursor = self.conn.cursor()
            query = 'DROP TABLE IF EXISTS ' + table_name
            if cascade:
                query = query + ' CASCADE'
            cursor.execute(query)
            if notices:
                pprint(self.conn.notices)
            self.conn.commit()
        except Exception as e:
            print(e)
            raise

    def create_table(self, table_name, col_names, col_types, constraints_str='', id_col_name='id', notices=False):
        try:
            print('Creating table ', table_name)
            # lower all
            col_names = [x.lower() for x in col_names]
            col_types = [x.lower() for x in col_types]

            query = 'CREATE TABLE IF NOT EXISTS ' + table_name + \
                    '(' + id_col_name + ' bigserial PRIMARY KEY, ' + \
                    ', '.join(' '.join(n) for n in zip(col_names, col_types)) + '); ' + \
                    constraints_str + '; '
            print(query)
            cursor = self.conn.cursor()
            cursor.execute(query)
            if notices:
                pprint(self.conn.notices)
            self.conn.commit()
        except Exception as e:
            print(e)
            raise

    def close_connection(self, ):
        self.conn.close()
