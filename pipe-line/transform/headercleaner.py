from transform.cleaner import Cleaner


class HeaderCleaner(Cleaner):
    def __init__(self):
        super().__init__()

    def clean_header_data(self, df):
        return df
