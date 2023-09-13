import pymssql


class DatabaseBuilder:
    def __init__(self):
        self.server_name = "LAPTOP-41GMGU12"

    def initialize_database(self):
        self.create_connection()
        print("CONNECTED TO SQL SERVER")
        self.create_database()
        print("DATABASE CREATED")
        self.create_tables()
        print("TABLES CREATED")
        self.close_resources()
        print("RESOURCES CLOSED")

    def create_connection(self):
        self.conn = pymssql.connect(server=self.server_name)
        self.conn.autocommit(True)
        self.cursor = self.conn.cursor()

    def close_resources(self):
        self.conn.close()

    def create_database(self):
        self.cursor.execute("CREATE DATABASE shipments_db;")
        self.cursor.execute("USE shipments_db")
        self.conn.commit()

    def create_tables(self):
        self.create_shipments_table()
        self.create_cargo_items_tables()
        self.create_cargo_types_tables()
        self.create_tariffs_tables()
        self.create_voyages_table()
        self.create_foreign_ports_table()
        self.create_domestic_ports_table()
        self.create_vessels_tables()
        self.add_foreign_keys()

    def create_shipments_table(self):
        self.cursor.execute("""CREATE TABLE shipments(
                                shipment_id INT PRIMARY KEY,
                                voyage_id INT,
                                weight FLOAT,
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
                                piece_count INT,
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

    def add_foreign_keys(self):
        self.add_foreign_keys_to_cargo_items()
        self.add_foreign_keys_to_shipments()
        self.add_foreign_keys_to_tariffs()
        self.add_foreignKeys_to_voyages()

    def add_foreignKeys_to_voyages(self):
        self.cursor.execute("""ALTER TABLE voyages
                            ADD CONSTRAINT domestic_port_FK
                            FOREIGN KEY(port_of_unlading_id) 
                            REFERENCES domestic_ports(port_id);""")
        self.cursor.execute("""ALTER TABLE voyages
                            ADD CONSTRAINT foreign_port_FK
                            FOREIGN KEY(foreign_port_of_lading_id) 
                            REFERENCES foreign_ports(port_id);""")
        self.cursor.execute("""ALTER TABLE voyages
                            ADD CONSTRAINT vessel_FK
                            FOREIGN KEY(vessel_id) 
                            REFERENCES vessels(vessel_id);""")
        self.conn.commit()

    def add_foreign_keys_to_shipments(self):
        self.cursor.execute("""ALTER TABLE shipments
                            ADD CONSTRAINT voyage_FK
                            FOREIGN KEY(voyage_id) 
                            REFERENCES voyages(voyage_id);""")
        self.conn.commit()

    def add_foreign_keys_to_tariffs(self):
        self.cursor.execute("""ALTER TABLE tariffs
                            ADD CONSTRAINT shipment_FK
                            FOREIGN KEY(shipment_id) 
                            REFERENCES shipments(shipment_id);""")
        self.conn.commit()

    def add_foreign_keys_to_cargo_items(self):
        self.cursor.execute("""ALTER TABLE cargo_items
                            ADD CONSTRAINT cargo_type_FK 
                            FOREIGN KEY(cargo_type_id) 
                            REFERENCES cargo_types(cargo_type_id);""")
        self.cursor.execute("""ALTER TABLE cargo_items
                            ADD CONSTRAINT cargo_shipment_FK 
                            FOREIGN KEY(shipment_id) 
                            REFERENCES shipments(shipment_id);""")
        self.conn.commit()
