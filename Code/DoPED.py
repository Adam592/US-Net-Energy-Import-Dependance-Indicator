import pandas as pd
import numpy as np


class DoPED:

    def __init__(self, pe_production, pe_import, pe_export):
        self.pe_production = pe_production
        self.pe_import = pe_import
        self.pe_export = pe_export

    def filter_data(self):
        desired_columns = ["YYYYMM", "Value", "Description"]
        self.pe_production = self.pe_production[desired_columns]
        self.pe_import = self.pe_import[desired_columns]
        self.pe_export = self.pe_export[desired_columns]

        mask_production = (
            (self.pe_production["Description"] == "Coal Production")
            | (self.pe_production["Description"] == "Natural Gas (Dry) Production")
            | (self.pe_production["Description"] == "Crude Oil Production")
            | (self.pe_production["Description"] == "Biomass Energy Production")
        )
        mask_import = (
            (self.pe_import["Description"] == "Coal Imports")
            | (self.pe_import["Description"] == "Natural Gas Imports")
            | (self.pe_import["Description"] == "Crude Oil Imports")
            | (self.pe_import["Description"] == "Biomass Imports")
        )
        mask_export = (
            (self.pe_export["Description"] == "Coal Exports")
            | (self.pe_export["Description"] == "Natural Gas Exports")
            | (self.pe_export["Description"] == "Crude Oil Exports")
            | (self.pe_export["Description"] == "Biomass Exports")
        )

        self.pe_production = self.pe_production[mask_production]
        self.pe_import = self.pe_import[mask_import]
        self.pe_export = self.pe_export[mask_export]

        self.all_data = pd.concat([self.pe_production, self.pe_import, self.pe_export])

    def dataset_for_each_resource(self, resource_name):

        resource_data = self.all_data[
            self.all_data["Description"].str.contains(f"{resource_name}", case=False)
        ]
        resource_data = resource_data.pivot_table(
            index="YYYYMM", columns="Description", values="Value", aggfunc="sum"
        ).reset_index()
        resource_data.columns = [
            "YYYYMM",
            f"{resource_name} Exports",
            f"{resource_name} Imports",
            f"{resource_name} Production",
        ]

        return resource_data

    def generate_resource_dataframes(self):
        self.coal_data = self.dataset_for_each_resource("Coal")
        self.natgas_data = self.dataset_for_each_resource("Natural Gas")
        self.crude_data = self.dataset_for_each_resource("Crude Oil")
        self.biomass_data = self.dataset_for_each_resource("Biomass")
