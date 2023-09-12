class Transformer:

    def make_tables(self, headers_df, cargo_descriptions_df, tariffs_df):
        self.make_domestic_ports_table(headers_df)
        self.make_foreign_ports_table(headers_df)
        self.make_vessels_table(headers_df)
        self.make_voyages_table(headers_df)
        self.make_shipments_table(headers_df)
        self.cargo_types(cargo_descriptions_df)
        self.cargo_items_table(cargo_descriptions_df)
        self.make_tariffs_table(tariffs_df)

    def make_domestic_ports_table(self, headers_df):
        ports = headers_df.groupby("port_of_unlading").count()
        ports.reset_index()
        ports = ports[ports['port_of_unlading'].str.contains('[a-z,A-Z]')]
        ports = self.split_port_name_into_seperate_fields(ports)
        ports = ports[['city', 'state']]
        return ports

    def make_foreign_ports_table(self, headers_df):
        ports = headers_df.groupby("port_of_unlading").count()
        ports.reset_index()
        ports = ports[ports['port_of_unlading'].str.contains('[a-z,A-Z]')]
        ports = self.split_port_name_into_seperate_fields(
            ports, isDomestic=False)
        ports = ports[['city', 'country']]
        return ports

    def split_port_name_into_seperate_fields(self, ports_df, isDomestic=True):
        label = 'country'
        if isDomestic:
            label = 'state'
        ports_df[label] = ports_df['port_of_unlading'].astype(
            'string').apply(lambda name: name.split(',')[-1])
        ports_df['city'] = ports_df['port_of_unlading'].astype(
            'string').apply(lambda name: name.rsplit(',', 1)[0])
        return ports_df

    def make_vessels_table(self, headers_df):
        vessels = headers_df[['vessel_name', 'vessel_country_code']]
        vessels = vessels.groupby(
            ['vessel_name', 'vessel_country_code']).count().reset_index()
        return vessels

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
