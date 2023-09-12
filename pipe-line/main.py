from extract.input import Input
from transform.cargodesccleaner import CargoDescriptionCleaner
from transform.headercleaner import Headercleaner
from transform.tariffcleaner import TariffCleaner
from transform.transform import Transformer
from load.databasebuilder import DatabaseBuilder
from load.loader import Loader

cargo_desc_path = "C:\\Users\\Owner\\Desktop\\AMS_DATA\\cargodesc\\ams__cargodesc_2020__202009291500_part_0.csv"

input = Input()
cargo_cleaner = CargoDescriptionCleaner()

cargo_df = input.load_csv(cargo_desc_path)
cleaned_cargo_df = cargo_cleaner.clean_cargo_description_data(cargo_df)

print(cleaned_cargo_df.head())
# cleaned_cargo_df.to_csv("cleaned_cargodesc.csv", sep=',', header=True)
