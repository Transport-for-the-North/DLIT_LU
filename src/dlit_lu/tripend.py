import pandas as pd
import logging
from typing import Dict, Any
import numpy as np
from pathlib import Path

# Local imports
from dlit_lu import inputs, utilities

# Third-party imports
from caf.base.data_structures import DVector
import caf.tem as ct

LOG = logging.getLogger(__name__)

class Tripends():

    def load_data(self, config: inputs.TripendsConfig): 

        soc_sic_emp = pd.read_csv(config.zone_soc_sic_emp)
        soc_sic_emp.fillna(0, inplace=True)
        print(soc_sic_emp)

        tt_pop = pd.read_csv(config.zone_tt_pop)
        tt_pop.fillna(0, inplace=True)
        print(tt_pop)

        return soc_sic_emp, tt_pop

    def process_data(self, data: pd.DataFrame, data_type: str) -> Dict[str, pd.DataFrame]:

        years = range(2024, 2067)
        categories = ['', '_large', '_small']

        processed_data = {}

        if data_type == "soc_sic_emp":
            index_cols = ['sic_2d', 'soc']
        elif data_type == "tt_pop":
            index_cols = ['tt']
        else:
            raise ValueError("Unknown data type")

        for year in years:
            for category in categories:
                year_col = f"{year}{category}"
                if year_col in data.columns:
                    pivot_df = data.pivot_table(index=index_cols, columns='normits_id', values=year_col, fill_value=0)
                    processed_data[f"{year_col}"] = pivot_df

        return processed_data

def run(config: inputs.DLitConfig):

    if config.run_tripend is None:
        raise ValueError("Cannot run tripend module without any tripend parameters")

    LOG.info("Initialising Tripend Module")

    config.output_folder.mkdir(exist_ok=True)

    key_constraint_path = config.output_folder / "07_tripend"
    key_constraint_path.mkdir(exist_ok=True)

    soc_sic_base_folder = key_constraint_path / "dlog_soc_sic_emp"
    tt_base_folder = key_constraint_path / "dlog_tt_pop"

    soc_sic_base_folder.mkdir(exist_ok=True)
    tt_base_folder.mkdir(exist_ok=True)

    tripend = Tripends()
    soc_sic_emp, tt_pop = tripend.load_data(config.tripend)

    data_types = ["soc_sic_emp", "tt_pop"]
    data_frames = [soc_sic_emp, tt_pop]
    base_folders = [soc_sic_base_folder, tt_base_folder]

    for df, dtype, base_folder in zip(data_frames, data_types, base_folders):
        processed_data = tripend.process_data(df, dtype)

        for year_col, pivot_df in processed_data.items():
            category = ''
            if '_large' in year_col:
                category = '_large'
            elif '_small' in year_col:
                category = '_small'

            output_folder = base_folder / f"{dtype}{category}"
            output_folder.mkdir(exist_ok=True)
            output_file = output_folder / f"dlog_{dtype}_{year_col}.hdf"
            pivot_df.to_hdf(str(output_file), key='data', mode='w')

            LOG.info(f"Data for {year_col} saved to {output_file}")

    LOG.info("Data processing completed")