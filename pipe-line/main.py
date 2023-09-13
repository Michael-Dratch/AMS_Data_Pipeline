import gc

from extract.input import Input
from transform.cargodesccleaner import CargoDescriptionCleaner
from transform.headercleaner import HeaderCleaner
from transform.tariffcleaner import TariffCleaner
from transform.transform import Transformer
from load.loader import Loader


header_path = "C:\\Users\\Owner\\Desktop\\AMS_DATA\\header\\ams__header_2020__202009291500_part_0.csv"
cargo_desc_path = "C:\\Users\\Owner\\Desktop\\AMS_DATA\\cargodesc\\ams__cargodesc_2020__202009291500_part_0.csv"
tariff_path = "C:\\Users\\Owner\\Desktop\\AMS_DATA\\tariff\\ams__tariff_2020__202009291500_part_0.csv"

# EXTRACT
input = Input()
header_df = input.load_csv(header_path)
cargo_df = input.load_csv(cargo_desc_path)
tariff_df = input.load_csv(tariff_path)

# TRANSFORM
header_df = HeaderCleaner().clean_header_data(header_df)
cargo_df = CargoDescriptionCleaner().clean_cargo_description_data(cargo_df)
tariff_df = TariffCleaner().clean_tariff_data(tariff_df)

tables = Transformer().transform_tables(
    header_df, cargo_df, tariff_df)

# LOAD
Loader().load_database(tables)
