import pandas as pd
import logging
from typing import Dict, Any
import numpy as np
from pathlib import Path

# Local imports
from dlit_lu import inputs, utilities

import caf.tem as ct

LOG = logging.getLogger(__name__)

class Tripends():

    def load_data(self, config: inputs.TripendsConfig): 
        soc_sic = pd.read_csv(config.zone_soc_sic_emp)
        print(soc_sic)
        tt = pd.read_csv(config.zone_tt_pop)
        print(tt)
        
        return soc_sic, tt  

    def tem():
        
        tem = ct.TEM(
            model_years=[2023],
            cenario="Core",
            output_zoning="normits",
            iteration_name="20250310",
            export_home=r"T:\ThomasPrince\TEM I-Drive Comparison\Outputs - caf.tem",
            return_segmentation=["hh_type", "p", "m", "tp"]
            )

        input_dir = Path(r"T:\ThomasPrince\TEM I-Drive Comparison\Inputs\01-HBProduction")

        HBProd = tem.HBProductionModel(
            population_paths={2023: input_dir / "lu_pop_2023.hdf"},
            trip_rates_path=input_dir / "hb_trip_rates_production.hdf",
            mode_time_splits_path=input_dir / "mode_time_split_production_hb_fr_reg.hdf",
            adjustment_path=input_dir / "trip_rate_adjustments_production_hb_fr.hdf",
            mts_adj_path=r"T:\ThomasPrince\TEM I-Drive Comparison\Inputs\01-HBProduction\mode_time_split_adjustments.hdf",
            population_translation_path=r"T:\ThomasPrince\TEM I-Drive Comparison\Inputs\normits_lsoa_2021_pop.csv"
            )

        HBProd.run()


def run(config: inputs.DLitConfig):

    if config.run_tripend is None:
        raise ValueError("Cannot run tripend module without any tripend parameters")

    LOG.info("Initialising Tripend Module")

    config.output_folder.mkdir(exist_ok=True)

    key_constraint_path = config.output_folder / "07_tripend"
    key_constraint_path.mkdir(exist_ok=True)

    tripend = Tripends()
    soc_sic, tt = tripend.load_data(config.tripend)  

    # Export the loaded data to CSV files in the 07_tripend folder
    soc_sic.to_csv(key_constraint_path / "zone_soc_sic_emp.csv", index=False)
    tt.to_csv(key_constraint_path / "zone_tt_pop.csv", index=False)

    LOG.info("Data exported to 07_tripend folder")