import pandas as pd
import pathlib
import logging
import os
import sys

if "src" not in sys.path:
    sys.path.append("src")

from dlit_lu import inputs

LOG = logging.getLogger(__name__)

class GrowthRate:
    def __init__(self, config: inputs.DLitConfig):
        self.constraints_config = config.constraint
        LOG.info("Initializing Constraints Module")

    def load_data(self, config: inputs.SummaryInputs):
        LOG.info("Loading DDG Employment data...")
        ddg_pop = pd.read_csv(self.constraints_config.ddg_pop)
        ddg_emp = pd.read_csv(self.constraints_config.ddg_emp)
        dlog_population = pd.read_csv(self.constraints_config.dlog_population)
        dlog_employment = pd.read_csv(self.constraints_config.dlog_employment)
        lad_to_region_file = pd.read_csv(config.lad_to_region_file)
        lad_name = pd.read_csv(self.constraints_config.lad_name)
        region_name = pd.read_csv(self.constraints_config.region_name)
        
        return ddg_pop, ddg_emp, dlog_population, dlog_employment, lad_to_region_file, lad_name, region_name

    def id_to_name(self, data: pd.DataFrame, name_file: pathlib.Path, id_column: str, name_column: str) -> pd.DataFrame:
        name_df = pd.read_csv(name_file)
        data_with_name = data.merge(name_df[[id_column, name_column]], on=id_column, how='left')
        LOG.info(f"Added {name_column} to dataset with shape {data_with_name.shape}")
        return data_with_name

    def _lad_to_region(self, data: pd.DataFrame, lookup_path: pathlib.Path, lad_id: str, base_year_column: str, future_year_columns: list, region_id: str, lad_to_region_prop_col: str) -> pd.DataFrame:
        lookup_df = pd.read_csv(lookup_path)
        val_cols = [base_year_column] + future_year_columns
        totals_before = {col: data[col].sum() for col in val_cols}

        merged_df = data.merge(lookup_df[[lad_id, region_id, lad_to_region_prop_col]], on=lad_id, how='left')

        for col in val_cols:
            merged_df[col] = merged_df[col] * merged_df[lad_to_region_prop_col]

        region_agg = merged_df.groupby(region_id, as_index=False)[val_cols].sum()

        totals_after = {col: region_agg[col].sum() for col in val_cols}
        for col in val_cols:
            if not np.isclose(totals_before[col], totals_after[col]):
                raise ValueError(f"Total of '{col}' changed after aggregation: before={totals_before[col]}, after={totals_after[col]}")

        return region_agg

    def aggregate(self, ddg_pop, ddg_emp, dlog_population, dlog_employment, lad_to_region_file):
        year_columns = [col for col in dlog_population.columns if col.isdigit()]

        ddg_pop = ddg_pop[['LAD13CD'] + year_columns]
        ddg_emp = ddg_emp[['LAD13CD'] + year_columns]
        LOG.info(f"ddg_pop and ddg_emp subsets to LAD13CD and years: {year_columns}")

        lad_id = "lad2013_id"
        region_id = "ntem_region_id"
        lad_to_region_prop_col = "lad2013_to_ntem_region"

        # Aggregating from LAD to Region for DDG datasets
        region_ddg_pop = self._lad_to_region(ddg_pop, lad_to_region_file, lad_id, "2023", year_columns, region_id, lad_to_region_prop_col)
        region_ddg_emp = self._lad_to_region(ddg_emp, lad_to_region_file, lad_id, "2023", year_columns, region_id, lad_to_region_prop_col)
        LOG.info(f"Aggregated DDG population and employment data to region format.")

        # Aggregating from LAD to Region for DLOG datasets
        region_dlog_pop = self._lad_to_region(dlog_population, lad_to_region_file, lad_id, "2023", year_columns, region_id, lad_to_region_prop_col)
        region_dlog_emp = self._lad_to_region(dlog_employment, lad_to_region_file, lad_id, "2023", year_columns, region_id, lad_to_region_prop_col)
        LOG.info(f"Aggregated DLOG population and employment data to region format.")

        # Rename lad2013_id to LAD13CD for consistency
        ddg_pop = ddg_pop.rename(columns={'lad2013_id': 'LAD13CD'})
        ddg_emp = ddg_emp.rename(columns={'lad2013_id': 'LAD13CD'})

        # Add LAD and Region names to the data
        lad_name_file = self.constraints_config.lad_name
        ddg_pop = self.id_to_name(ddg_pop, lad_name_file, "LAD13CD", "LADNM")
        ddg_emp = self.id_to_name(ddg_emp, lad_name_file, "LAD13CD", "LADNM")
        dlog_pop = self.id_to_name(dlog_pop, lad_name_file, "LAD13CD", "LADNM")
        dlog_emp = self.id_to_name(dlog_emp, lad_name_file, "LAD13CD", "LADNM")

        # Add Region names to the region outputs
        region_name_file = self.constraints_config.region_name
        region_ddg_pop = self.id_to_name(region_ddg_pop, region_name_file, "ntem_region_id", "RegionName")
        region_ddg_emp = self.id_to_name(region_ddg_emp, region_name_file, "ntem_region_id", "RegionName")
        region_dlog_pop = self.id_to_name(region_dlog_pop, region_name_file, "ntem_region_id", "RegionName")
        region_dlog_emp = self.id_to_name(region_dlog_emp, region_name_file, "ntem_region_id", "RegionName")

        return region_ddg_pop, region_ddg_emp, region_dlog_pop, region_dlog_emp, ddg_pop, ddg_emp, dlog_pop, dlog_emp

    def save_outputs(self, region_ddg_pop, region_ddg_emp, region_dlog_pop, region_dlog_emp, ddg_pop, ddg_emp, dlog_pop, dlog_emp):

        output_folder = r"I:\Data\D-Log\DLIT\Outputs\test11\06_constraint"
        os.makedirs(output_folder, exist_ok=True)
        

        # Save all outputs to CSV in the specified folder
        region_ddg_pop.to_csv(output_folder / "region_ddg_pop.csv", index=False)
        region_ddg_emp.to_csv(output_folder / "region_ddg_emp.csv", index=False)
        region_dlog_pop.to_csv(output_folder / "region_dlog_pop.csv", index=False)
        region_dlog_emp.to_csv(output_folder / "region_dlog_emp.csv", index=False)
        ddg_pop.to_csv(output_folder / "ddg_pop.csv", index=False)
        ddg_emp.to_csv(output_folder / "ddg_emp.csv", index=False)
        dlog_pop.to_csv(output_folder / "dlog_pop.csv", index=False)
        dlog_emp.to_csv(output_folder / "dlog_emp.csv", index=False)

        LOG.info(f"All output files saved to {output_folder}")

    def run(self, config: inputs.DLitConfig):
        # Main function to run the data aggregation and name merging process
        ddg_pop, ddg_emp, dlog_population, dlog_employment, lad_to_region_file, lad_name, region_name = self.load_data(config)

        region_ddg_pop, region_ddg_emp, region_dlog_pop, region_dlog_emp, ddg_pop, ddg_emp, dlog_pop, dlog_emp = self.aggregate(
            ddg_pop, ddg_emp, dlog_population, dlog_employment, lad_to_region_file
        )

        self.save_outputs(region_ddg_pop, region_ddg_emp, region_dlog_pop, region_dlog_emp, ddg_pop, ddg_emp, dlog_pop, dlog_emp)

        LOG.info("Process completed successfully.")

if __name__ == "__main__":
    config = inputs.DLitConfig() 
    growth_rate = GrowthRate(config)
    growth_rate.run(config)