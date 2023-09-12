class Transformer:

    def make_tables(self, headers_df, cargo_descriptions_df, tariffs_df):
        headers_df = self.add_foreign_keys(headers_df)
        self.make_domestic_ports_table(headers_df)
        self.make_foreign_ports_table(headers_df)
        self.make_vessels_table(headers_df)
        self.make_voyages_table(headers_df)
        self.make_shipments_table(headers_df)
        self.cargo_types(cargo_descriptions_df)
        self.cargo_items_table(cargo_descriptions_df)
        self.make_tariffs_table(tariffs_df)

    def add_foreign_keys(self, headers_df):
        headers_df = self.add_port_of_unlading_keys(headers_df)
        headers_df = self.add_port_of_lading_keys(headers_df)
        headers_df = self.add_vessel_keys(headers_df)
        headers_df = self.add_voyage_keys(headers_df)
        return headers_df

    def add_port_of_unlading_keys(self, headers_df):
        header_df = header_df.sort_values("port_of_unlading")
        header_df['port_of_unlading_id'] = header_df['port_of_unlading'].rank(method='dense').astype(
            int)
        return header_df

    def add_port_of_lading_keys(self, headers_df):
        header_df = header_df.sort_values("foreign_port_of_lading")
        header_df['foreign_port_of_lading_id'] = header_df['foreign_port_of_lading'].rank(method='dense').astype(
            int)
        return header_df

    def add_vessel_keys(self, header_df):
        header_df = header_df.sort_values("vessel_name")
        header_df["vessel_id"] = header_df["vessel_name"].rank(
            method='dense').astype(int)
        return header_df

    def add_voyage_keys(self, header_df):
        header_df['vessel_date'] = header_df['vessel_name'] + \
            header_df['actual_arrival_date']
        header_df = header_df.sort_values(by='vessel_date')
        header_df['voyage_id'] = header_df['vessel_date'].rank(
            method='dense').astype('int')
        return header_df

    def make_domestic_ports_table(self, headers_df):
        ports = headers_df.groupby(
            "port_of_unlading", "port_of_unlading_id").count()
        ports.reset_index()
        ports = ports[ports['port_of_unlading'].str.contains('[a-z,A-Z]')]
        ports = self.split_domestic_port_name_into_seperate_fields(ports)
        ports = ports[['port_of_unlading_id', 'city', 'state']]
        ports.rename({'port_of_unlading_id': 'port_id'})
        return ports

    def make_foreign_ports_table(self, headers_df):
        ports = headers_df.groupby("port_of_unlading").count()
        ports.reset_index()
        ports = ports[ports['port_of_unlading'].str.contains('[a-z,A-Z]')]
        ports = self.split_foreign_port_name_into_seperate_fields(
            ports, isDomestic=False)
        ports = ports[['foreign_port_of_lading', 'city', 'country']]
        ports.rename({'foreign_port_of_lading_id': 'port_id'})
        return ports

    def split_domestic_port_name_into_seperate_fields(self, ports_df):
        ports_df['state'] = ports_df['port_of_unlading'].astype(
            'string').apply(lambda name: name.split(',')[-1])
        ports_df['city'] = ports_df['port_of_unlading'].astype(
            'string').apply(lambda name: name.rsplit(',', 1)[0])
        return ports_df

    def split_foreign_port_name_into_seperate_fields(self, ports_df):
        ports_df['country'] = ports_df['foreign_port_of_lading'].astype(
            'string').apply(lambda name: name.split(',')[-1])
        ports_df['city'] = ports_df['foreign_port_of_lading'].astype(
            'string').apply(lambda name: name.rsplit(',', 1)[0])
        return ports_df

    def make_vessels_table(self, headers_df):
        vessels = headers_df[['vessel_id',
                              'vessel_name', 'vessel_country_code']]
        vessels = vessels.groupby(
            ['vessel_id', 'vessel_name', 'vessel_country_code']).count().reset_index()
        return vessels

    def make_voyages_table(self, headers_df):
        voyages = headers_df[['voyage_id', 'vessel_id', 'port_of_unlading_id',
                             'foreign_port_of_lading_id', 'actual_arrival_date', 'place_of_receipt']]
        return voyages

    def make_shipments_table(self, headers_df):
        shipments = headers_df[['identifier',
                                'voyage_id', 'weight', 'weight_unit']]
        shipments = shipments.rename({'identifier': 'shipment_id'})
        return shipments

    def make_tariffs_table(self, tariffs_df):
        pass

    def cargo_items_table(self, cargo_descriptions_df):
        pass

    def cargo_types(self, cargo_descriptions_df):
        pass
