import pandas as pd
import logging
from typing import Dict, Any, List
from pathlib import Path

# Local imports
from dlit_lu import inputs
from caf.base.data_structures import DVector
import caf.base as cb

LOG = logging.getLogger(__name__)

class Tripends:
    
    YEARS = range(2024, 2067)
    CATEGORIES = ['', '_large', '_small']
    DATA_CONFIG = {
        "soc_sic_emp": {
            "index_cols": ['sic_2_digit', 'soc'],
            "rename_cols": {'sic_2d': 'sic_2_digit'},
            "segments": ['sic_2_digit', 'soc']
        },
        "tt_pop": {
            "index_cols": ['tt'],
            "segments": ['gender_3', 'aws', 'soc', 'ns_sec', 'hh_type'],
        }
    }

    def __init__(self):
        self.cols_to_merge = ['gender_3', 'aws', 'ns_sec', 'soc', 'hh_type']
        self.rename_mapping = {
            'gender': 'gender_3',
            'aws': 'aws',
            'ns': 'ns_sec',
            'soc': 'soc',
            'hh_type': 'hh_type'
        }

    def merge_and_rename(self, 
                         data: pd.DataFrame, 
                         lookup: pd.DataFrame) -> pd.DataFrame:
        
        """Merge and rename columns for tt_pop data."""

        return data.merge(lookup, on='tt', how='left').rename(columns=self.rename_mapping)

    def process_data(self, 
                    data: pd.DataFrame, 
                    data_type: str, 
                    lookup: pd.DataFrame = None) -> Dict[str, pd.DataFrame]:
        
        """Process data for a given type and pivot it per year and category."""

        if data_type not in self.DATA_CONFIG:
            raise ValueError(f"Unknown data type: {data_type}")

        config = self.DATA_CONFIG[data_type]
        if "rename_cols" in config:
            data.rename(columns=config["rename_cols"], inplace=True)

        index_cols = config["index_cols"]

        if data_type == "tt_pop" and lookup is not None:
            data = self.merge_and_rename(data, lookup)
            index_cols = self.cols_to_merge  

        if 'normits_id' in data.columns:
            data['normits_id'] = data['normits_id'].astype('int64')

        processed_data = {
            f"{year}{category}": data.pivot_table(index=index_cols, columns='normits_id', values=f"{year}{category}", fill_value=0)
            for year in self.YEARS for category in self.CATEGORIES if f"{year}{category}" in data.columns
        }

        return processed_data

    def prepare_segmentation_input(self, 
                                   data_type: str) -> cb.SegmentationInput:
        
        """Prepare segmentation input based on data type."""

        if data_type not in self.DATA_CONFIG:

            raise ValueError(f"Unknown data type for segmentation: {data_type}")
        
        return cb.SegmentationInput(enum_segments=self.DATA_CONFIG[data_type]["segments"], naming_order=self.DATA_CONFIG[data_type]["segments"])

    def perform_segmentation_and_zoning(self, 
                                        data: pd.DataFrame, 
                                        data_type: str,
                                        output_path: Path):
        
        """Perform segmentation and zoning, then save the result."""

        seg = cb.Segmentation(self.prepare_segmentation_input(data_type))
        zoning = cb.ZoningSystem.get_zoning('normits')

        dvec = DVector(import_data=data, zoning_system=zoning, segmentation=seg)
        if data_type == "tt_pop":
            dvec = dvec.add_segments(['adult_nssec'])

        dvec.save(output_path)

def run(config: inputs.DLitConfig):

    """Main function to run the Tripends processing."""

    if config.run_tripend is None:
        raise ValueError("Cannot run tripend module without parameters")

    LOG.info("Initializing Tripend Module")
    config.output_folder.mkdir(exist_ok=True)

    data_files = {
        "soc_sic_emp": config.tripend.zone_soc_sic_emp,
        "tt_pop": config.tripend.zone_tt_pop,
        "tfn_tt": config.tripend.tfn_tt
    }

    data_frames = {key: pd.read_csv(path).fillna(0) for key, path in data_files.items()}

    base_folder = config.output_folder / "07_tripend"
    base_folder.mkdir(exist_ok=True)

    output_folders = {
        "soc_sic_emp": base_folder / "dlog_soc_sic_emp",
        "tt_pop": base_folder / "dlog_tt_pop"
    }

    for folder in output_folders.values():
        folder.mkdir(exist_ok=True)

    tripend = Tripends()

    for dtype in ["soc_sic_emp", "tt_pop"]:
        processed_data = tripend.process_data(
            data_frames[dtype], 
            dtype, 
            data_frames["tfn_tt"] if dtype == "tt_pop" else None
        )

        for year_col, pivot_df in processed_data.items():
            category = next((cat for cat in tripend.CATEGORIES if cat in year_col), '')

            output_folder = output_folders[dtype] / f"{dtype}{category}"
            output_folder.mkdir(exist_ok=True)

            output_file = output_folder / f"dlog_{dtype}_{year_col}.dvec"
            tripend.perform_segmentation_and_zoning(pivot_df, dtype, output_file)

    LOG.info("Data processing completed")