import pymssql


class DatabaseBuilder:
    def __init__(self):
        self.server_name = "LAPTOP-41GMGU12"

    def initialize_database(self):
        self.create_connection()
        self.cursor.execute("CREATE DATABASE shipments_db;")
        self.create_tables()
        self.cursor.commit()
        self.close_resources()

    def create_connection(self):
        self.conn = pymssql.connect(server=self.server_name)
        self.cursor = self.conn.cursor()

    def close_resources(self):
        self.conn.close()

    def create_tables(self):
        pass

    def create_shipments_table(self):
        pass
