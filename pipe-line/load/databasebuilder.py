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
        self.create_shipments_table()
        self.create_cargo_items_tables()
        self.create_cargo_types_tables()
        self.create_tariffs_tables()
        self.create_voyages_table()
        self.create_foreign_ports_table()
        self.create_domestic_ports_table()
        self.create_vessels_tables()
        self.add_foreign_keys() //NEED TO DO THIS

    def create_shipments_table(self):
        self.cursor.execute("""CREATE TABLE shipments(
                                shipment_id INT PRIMARY KEY,
                                voyage_id INT,
                                weight FLOAT
                                weight_unit VARCHAR(25)         
                                );
                                """)
        self.conn.commit()

    def create_voyages_table(self):
        self.cursor.execute("""CREATE TABLE voyages(
                                voyage_id INT PRIMARY KEY,
                                port_of_unlading_id INT,
                                foreign_port_of_lading_id INT,
                                arrival_date DATE,
                                vessel_id INT,
                                place_of_receipt VARCHAR(255)
                                );
                                """)
        self.conn.commit()

    def create_domestic_ports_table(self):
        self.cursor.execute("""CREATE TABLE domestic_ports(
                                port_id INT PRIMARY KEY,
                                city VARCHAR(255),
                                state VARCHAR(255)
                                );
                                """)
        self.conn.commit()

    def create_foreign_ports_table(self):
        self.cursor.execute("""CREATE TABLE foreign_ports(
                                port_id INT PRIMARY KEY,
                                city VARCHAR(255),
                                country VARCHAR(255)
                                );
                                """)
        self.conn.commit()

    def create_vessels_tables(self):
        self.cursor.execute("""CREATE TABLE vessels(
                                vessel_id INT PRIMARY KEY,
                                name VARCHAR(255),
                                country_code VARCHAR(25),
                                );
                                """)
        self.conn.commit()

    def create_cargo_types_tables(self):
        self.cursor.execute("""CREATE TABLE cargo_types(
                                cargo_type_id INT PRIMARY KEY,
                                description_text VARCHAR(255),
                                );
                                """)
        self.conn.commit()

    def create_cargo_types_tables(self):
        self.cursor.execute("""CREATE TABLE cargo_types(
                                cargo_type_id INT PRIMARY KEY,
                                description_text VARCHAR(255),
                                );
                                """)
        self.conn.commit()

    def create_cargo_items_tables(self):
        self.cursor.execute("""CREATE TABLE cargo_items(
                                cargo_type_id INT,
                                shipment_id INT,
                                piece_count INT
                                PRIMARY KEY(cargo_type_id, shipment_id)
                                );
                                """)
        self.conn.commit()

    def create_tariffs_tables(self):
        self.cursor.execute("""CREATE TABLE tariffs(
                                tariff_id INT PRIMARY KEY,
                                shipment_id INT,
                                harmonized_number VARCHAR(25),
                                harmonized_value VARCHAR(25),
                                harmonized_weight FLOAT,
                                harmonized_weight_unit VARCHAR(25)
                                );
                                """)
        self.conn.commit()
