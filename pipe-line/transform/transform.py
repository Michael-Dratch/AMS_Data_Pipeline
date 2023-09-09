class Transformer:

    def make_tables(self, headers_df, cargo_descriptions_df, tariffs_df):
        self.make_ports_table(headers_df)
        self.make_vessels_table(headers_df)
        self.make_voyages_table(headers_df)
        self.make_shipments_table(headers_df)
        self.cargo_types(cargo_descriptions_df)
        self.cargo_items_table(cargo_descriptions_df)
        self.make_tariffs_table(tariffs_df)

    def make_ports_table(self, headers_df):
        pass

    def make_vessels_table(self, headers_df):
        pass

    def make_voyages_table(self, headers_df):
        pass

    def make_shipments_table(self, headers_df):
        pass

    def make_tariffs_table(self, tariffs_df):
        pass

    def cargo_items_table(self, cargo_descriptions_df):
        pass

    def cargo_types(self, cargo_descriptions_df):
        pass
