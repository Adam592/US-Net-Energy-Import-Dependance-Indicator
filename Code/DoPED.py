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

        return resource_data

    def generate_resource_dataframes(self):
        self.coal_data = self.dataset_for_each_resource("Coal")
        self.natgas_data = self.dataset_for_each_resource("Natural Gas")
        self.crude_data = self.dataset_for_each_resource("Crude Oil")
        self.biomass_data = self.dataset_for_each_resource("Biomass")

    def format_dataset(self, resource_data, resource_type):
        dataset = getattr(self, resource_data)

        dataset["YYYYMM"] = dataset["YYYYMM"].astype(str)
        dataset = dataset[
            dataset["YYYYMM"].str.endswith("13")
            & dataset["YYYYMM"].between("1973", "2023")
        ]
        dataset.rename(
            columns={dataset.columns[0]: "Year"},
            inplace=True,
        )

        dataset["Year"] = dataset["Year"].str[:4]
        dataset["Year"] = pd.to_numeric(dataset["Year"], errors="coerce")

        dataset[f"{resource_type} Imports"] = pd.to_numeric(
            dataset[f"{resource_type} Imports"], errors="coerce"
        )
        dataset[f"{resource_type} Exports"] = pd.to_numeric(
            dataset[f"{resource_type} Exports"], errors="coerce"
        )

        dataset[f"{resource_type} Net Import"] = (
            dataset[f"{resource_type} Imports"] - dataset[f"{resource_type} Exports"]
        )

        if resource_type == "Natural Gas":
            dataset[f"{resource_type} (Dry) Production"] = pd.to_numeric(
                dataset[f"{resource_type} (Dry) Production"], errors="coerce"
            )
            dataset[f"{resource_type} Supply"] = (
                dataset[f"{resource_type} (Dry) Production"]
                + dataset[f"{resource_type} Net Import"]
            )
        elif resource_type == "Biomass":
            dataset[f"{resource_type} Energy Production"] = pd.to_numeric(
                dataset[f"{resource_type} Energy Production"], errors="coerce"
            )
            dataset[f"{resource_type} Supply"] = (
                dataset[f"{resource_type} Energy Production"]
                + dataset[f"{resource_type} Net Import"]
            )
        else:
            dataset[f"{resource_type} Production"] = pd.to_numeric(
                dataset[f"{resource_type} Production"], errors="coerce"
            )
            dataset[f"{resource_type} Supply"] = (
                dataset[f"{resource_type} Production"]
                + dataset[f"{resource_type} Net Import"]
            )

        setattr(self, resource_data, dataset)

    def format_all_datasets(self):
        self.format_dataset("coal_data", "Coal")
        self.format_dataset("natgas_data", "Natural Gas")
        self.format_dataset("crude_data", "Crude Oil")
        self.format_dataset("biomass_data", "Biomass")

        print(self.biomass_data)
