import psycopg2
import sys

def str_with_quotes(obj):
    return '"' + str(obj) + '"'


def str_with_single_quotes(obj):
    return '\'' + str(obj) + '\''


class DB(object):
    conn_string = ''
    conn = None

    """docstring for DB."""

    def __init__(self, conn_string):
        super(DB, self).__init__()
        self.conn_string = conn_string
        try:
            self.conn = psycopg2.connect(conn_string)
        except psycopg2.Error as e:
            print(e)
            self.close_connection()
            sys.exit(1)

    def query(self, query, commit=False):
        try:
            print("executing Query: ", query)
            cursor = self.conn.cursor()
            cursor.execute(query)
            if commit:
                self.conn.commit()
            return cursor.fetchall()
        except psycopg2.Error as e:
            print(e)
            return None

    def drop_table(self, table_name):
        try:
            print('Droping table ', table_name)
            cursor = self.conn.cursor()
            cursor.execute('DROP TABLE IF EXISTS ' + table_name)
            self.conn.commit()
        except Exception as e:
            print(e)

    def create_table(self, table_name, col_names, col_types, id_col_name='id'):
        try:
            print('Creating table ', table_name)
            # lower all
            col_names = [x.lower() for x in col_names]
            col_types = [x.lower() for x in col_types]

            query = 'CREATE TABLE IF NOT EXISTS ' + table_name + '(' + id_col_name + ' bigserial, ' + ', '.join(
                ' '.join(n) for n in zip(col_names, col_types)) + ')'
            print(query)
            cursor = self.conn.cursor()
            cursor.execute(query)
            self.conn.commit()
        except Exception as e:
            print(e)

    def close_connection(self, ):
        self.conn.close()
