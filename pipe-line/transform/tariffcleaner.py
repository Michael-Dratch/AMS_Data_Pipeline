from transform.cleaner import Cleaner


class TariffCleaner(Cleaner):
    def __init__(self):
        super().__init__()

    def clean_cargo_description_data(self, df):
        return df
