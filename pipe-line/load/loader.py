import pymssql


class Loader:
    def __init__(self):
        self.server_name = "LAPTOP-41GMGU12"

    def create_connection(self):
        self.conn = pymssql.connect(server=self.server_name)
        self.cursor = self.conn.cursor()

    def close_resources(self):
        self.conn.close()

    def load_database(self):
        pass
