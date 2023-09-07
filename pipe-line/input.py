import pandas as pd

file_path = "C:\\Users\\Owner\\Desktop\\AMS_DATA\\cargodesc\\ams__cargodesc_2020__202009291500_part_0.csv"


class Input:

    def load_csv(self, file_path):
        df = pd.read_csv(file_path)
        return df
