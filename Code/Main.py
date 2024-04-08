import DoPED
import pandas as pd


primary_energy_production = pd.read_csv("../Data/Primary_Energy_Production.csv")
primary_energy_import = pd.read_csv("../Data/Primary_Energy_Import.csv")
primary_energy_export = pd.read_csv("../Data/Primary_Energy_Export.csv")

doped = DoPED.DoPED(
    primary_energy_production, primary_energy_import, primary_energy_export
)
doped.filter_data()
doped.generate_resource_dataframes()
doped.format_all_datasets()
