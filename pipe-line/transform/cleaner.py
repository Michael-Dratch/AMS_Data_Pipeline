class Cleaner:

    def remove_rows_with_null_values(self, df, column_name):
        df = df.dropna(subset=[column_name])
        return df

    def remove_rows_with_non_alphabetical_values(self, df, column_name):
        letters = r'[A-Za-z]'
        df = df[df[column_name].str.contains(letters)]
        return df

    def remove_non_aphabetical_characters(self, df, column_name):
        df[column_name] = df[column_name].str.replace(
            r'[^\sa-zA-Z]', '')
        return df

    def make_values_lowercase(self, df, column_name):
        df[column_name] = df[column_name].str.lower()
        return df
