import pandas as pd
import re
from cleaner.cleaner import Cleaner


class CargoDescriptionCleaner(Cleaner):
    def __init__(self):
        super().__init__()
        self.description_column = 'description_text'

    def clean_cargo_description_data(self, df):
        df = df.drop_duplicates()
        df = self.remove_rows_with_null_values(df, self.desc_column)
        df = self.remove_rows_with_non_alphabetical_values(
            df, self.desc_column)
        df = self.remove_non_aphabetical_characters(df, self.desc_column)
        df = self.make_values_lowercase(df, self.desc_column)
        df = self.remove_empty_containers(df, self.desc_column)
        df = self.remove_adjectives(df, self.desc_column)
        return df

    def remove_adjectives(self, df, column_name):
        adjectives = ['new', 'spare']
        for adjective in adjectives:
            df[column_name] = df[column_name].str.replace(
                adjective, '')
        return df

    def remove_empty_containers(self, df, column_name):
        empty_words = ['empty', 'none']
        for word in empty_words:
            df = df[~(df[column_name].str.contains(word))]
        return df
