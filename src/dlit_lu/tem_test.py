
import pandas as pd
from pathlib import Path

from caf.ntem import  ntem_constants
from dlit_lu import inputs
import caf.tem as ct
import caf.base as cb

def tem(config: inputs.DLitConfig):

    tem = ct.TEM(
        model_years=[2023],
        scenario=ntem_constants.Scenarios.CORE,   
        output_zoning= config.dev_pattern.geo_boundary.value,   
        iteration_name="20250318", 
        export_home=r"T:\ThomasPrince\TEM I-Drive Comparison\Outputs - caf.tem",
        return_segmentation=["hh_type", "p", "m", "tp"]
    )

    input_dir = Path(r"T:\ThomasPrince\TEM I-Drive Comparison\Inputs\01-HBProduction")

    HBProd = tem.HBProductionModel(
        population_paths={2024: input_dir / "dlog_tt_pop_2024.dvec"},
        trip_rates_path=input_dir / "hb_trip_rates_production.hdf",
        mode_time_splits_path=input_dir / "mode_time_split_production_hb_fr_reg.hdf",
        adjustment_path=input_dir / "trip_rate_adjustments_production_hb_fr.hdf",
        mts_adj_path=r"T:\ThomasPrince\TEM I-Drive Comparison\Inputs\01-HBProduction\mode_time_split_adjustments.hdf",
        population_translation_path=r"T:\ThomasPrince\TEM I-Drive Comparison\Inputs\normits_lsoa_2021_pop.csv"
    )

    HBProd.run()

