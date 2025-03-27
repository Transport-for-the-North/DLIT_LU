import pandas as pd
import logging
from typing import Dict, Any, List
from pathlib import Path

# Local imports
from dlit_lu import inputs, dev_pattern, constraint, utilities, global_classes, parser
from caf.base.data_structures import DVector
import caf.base as cb

LOG = logging.getLogger(__name__)

class TEConstraintProcessor(constraint.ConstraintProcessor):
    """
    This class extends ConstraintProcessor to specifically handle trip-end related
    constraints while reusing methods from the base class.
    """
    def __init__(self, config: inputs.DLitConfig) -> None:
        """
        Initializes the TripEndConstraintProcessor with trip-end specific configuration settings.

        Args:
            config (inputs.DLitConfig): The configuration object containing constraint details.
        """
        # Call the parent class's __init__ to inherit all other methods and attributes
        super().__init__(config)

        # Override the sector assignment if needed for trip-end specific logic
        # Here, you can define any additional initialization required
        self.config: inputs.DLitConfig = config
        self.sector: inputs.Sector = config.tripend.sector  # Modify if needed for trip-end-specific sector
        self.sector_info_map =  inputs.SECTOR_INFO_MAP 
        self.base_year_column = config.dev_pattern.base_year
        # Validate sector
        if self.sector not in self.sector_info_map:
            raise ValueError(f"Unsupported sector: {self.sector}")
        
        # Retrieve sector-specific metadata from predefined SECTOR_INFO_MAP
        self.sector_info: Dict[str, Any] = self.sector_info_map[self.sector]

        # Initialize the BaseZoneHandler for geo_boundary and other zone info
        self.base_zone_handler = dev_pattern.BaseZoneHandler(config)
        # Access zone information based on geo_boundary
        self.zone_info = self.base_zone_handler.zone_info        
        # Example: If you want to update other sector-specific information
        self.sector_info["lookup_path"] = self.zone_info["zone_to_lad_path"]  # config.dev_pattern.summary_data.normits_to_lad_file
        self.sector_info["zone_id"] = self.zone_info["group_by_column"] # "normits_v3.3_id"
        self.sector_info["sector_id"] = self.zone_info["lad_id_col"] # "lad2013_id"
        self.sector_info["zone_to_sector_prop_col"] = self.zone_info["zone_to_lad_prop"] # "normits_v3.3_to_lad2013"
        self.sector_info["sector_name"] = self.config.constraint.lad_name

    def load_zone_translation(self, model_zone) -> pd.DataFrame:
        """
        Loads the appropriate zone translation file based on the model_zone value.

        Args:
            model_zone (GeoBoundary): The model zone type to determine which translation file to load.

        Returns:
            pd.DataFrame: The translation file as a DataFrame, or None if no translation is needed.
        """

        if model_zone == inputs.GeoBoundary.NORMITS.value:
            # No translation file needed for normits
            return None     
        # elif model_zone == inputs.GeoBoundary.LSOA.value:
        #    return pd.read_csv(self.config.tripend.normits_to_lsoa)
        # elif model_zone == inputs.GeoBoundary.MSOA.value:
        #    return pd.read_csv(self.config.tripend.normits_to_msoa)
        # elif model_zone == inputs.GeoBoundary.NOHAM.value:
        #    return pd.read_csv(self.config.tripend.normits_to_noham)
        # elif model_zone == inputs.GeoBoundary.NORMS.value:
        #    return pd.read_csv(self.config.tripend.normits_to_norms)
        else:
            # Optional: raise an exception or handle an unsupported model_zone
           raise ValueError(f"Unsupported model_zone: {model_zone}")
        
    

    

class ByTeTransformation:
    def __init__(self, df: pd.DataFrame, base_year: str):
        """
        Initialize the class with the given DataFrame and base year.

        Parameters
        ----------
        df : pd.DataFrame
            Input DataFrame with mode and period columns.
        base_year : int
            The base year to rename "prod" and "attr".
        """
        self.df = df.copy()

        # Exclude unwanted categories for mode and period
        self.df = self.df[~self.df["mode"].isin([7])]  # Exclude mode category 7 for Air
        self.df = self.df[~self.df["period"].isin([5, 6])]  # Exclude period categories 5 and 6 for Saturday and Sunday
        self.df["mode"] = self.df["mode"].replace({4: 3})  # Merge 4 into 3 for mode to combine car driver and car passenger
        self.base_year = base_year


    def groupby_sum(self, val: str):
        """
        Group the DataFrame by "normits_v3.3_id", "purpose", "mode", and "period",
        summing the specified value column.

        Parameters
        ----------
        val : str
            The column to be summed, either "prod" or "attr".

        Returns
        -------
        pd.DataFrame
            Grouped DataFrame with summed values.
        """
        if val not in self.df.columns:
            raise ValueError(f"Column '{val}' not found in DataFrame")
        
        grouped_df = self.df.groupby(["normits_v3.3_id", "purpose", "mode"], as_index=False)[val].sum()

        # Rename columns
        grouped_df = grouped_df.rename(columns={
            "purpose": "p",
            "mode": "m",
            val: str(self.base_year)  # Rename "prod" or "attr" to base year
        })

        return grouped_df


        
# class Tripends:
    
#     YEARS = range(2024, 2067)
#     CATEGORIES = ['', '_large', '_small']
#     DATA_CONFIG = {
#         "soc_sic_emp": {
#             "index_cols": ['sic_2_digit', 'soc'],
#             "rename_cols": {'sic_2d': 'sic_2_digit'},
#             "segments": ['sic_2_digit', 'soc']
#         },
#         "tt_pop": {
#             "index_cols": ['tt'],
#             "segments": ['gender_3', 'aws', 'soc', 'ns_sec', 'hh_type'],
#         },
#         "hh": {
#             "index_cols": ['accom_h', 'ns_sec', 'adults', 'car_availability', 'children'],
#             "segments": ['accom_h', 'ns_sec', 'adults', 'car_availability', 'children'],
#         }
#     }

#     def __init__(self, config: inputs.DLitConfig) -> None:
#         self.cols_to_merge = ['gender_3', 'aws', 'ns_sec', 'soc', 'hh_type']
#         self.rename_mapping = {
#             'gender': 'gender_3',
#             'aws': 'aws',
#             'ns': 'ns_sec',
#             'soc': 'soc',
#             'hh_type': 'hh_type'
#         }
#         # Initialize the BaseZoneHandler for geo_boundary and other zone info
#         self.base_zone_handler = dev_pattern.BaseZoneHandler(config)
#         self.zone_info = self.base_zone_handler.zone_info
#         self.zone_id = self.zone_info["group_by_column"]

#     def merge_and_rename(self, 
#                          data: pd.DataFrame, 
#                          lookup: pd.DataFrame) -> pd.DataFrame:
        
#         """Merge and rename columns for tt_pop data."""

#         return data.merge(lookup, on='tt', how='left').rename(columns=self.rename_mapping)

#     def process_data(self, 
#                     data: pd.DataFrame, 
#                     data_type: str, 
#                     lookup: pd.DataFrame = None) -> Dict[str, pd.DataFrame]:
        
#         """Process data for a given type and pivot it per year and category."""

#         if data_type not in self.DATA_CONFIG:
#             raise ValueError(f"Unknown data type: {data_type}")

#         config = self.DATA_CONFIG[data_type]
#         if "rename_cols" in config:
#             data.rename(columns=config["rename_cols"], inplace=True)

#         index_cols = config["index_cols"]

#         if data_type == "tt_pop" and lookup is not None:
#             data = self.merge_and_rename(data, lookup)
#             index_cols = self.cols_to_merge  

#         if self.zone_id in data.columns:
#             data[self.zone_id] = data[self.zone_id].astype('int64')

#         processed_data = {
#             f"{year}{category}": data.pivot_table(index=index_cols, columns=self.zone_id, values=f"{year}{category}", fill_value=0)
#             for year in self.YEARS for category in self.CATEGORIES if f"{year}{category}" in data.columns
#         }

#         return processed_data

#     def prepare_segmentation_input(self, 
#                                    data_type: str) -> cb.SegmentationInput:
        
#         """Prepare segmentation input based on data type."""

#         if data_type not in self.DATA_CONFIG:

#             raise ValueError(f"Unknown data type for segmentation: {data_type}")
        
#         return cb.SegmentationInput(enum_segments=self.DATA_CONFIG[data_type]["segments"], naming_order=self.DATA_CONFIG[data_type]["segments"])

#     def perform_segmentation_and_zoning(self, 
#                                         data: pd.DataFrame, 
#                                         data_type: str,
#                                         output_path: Path):
        
#         """Perform segmentation and zoning, then save the result."""

#         seg = cb.Segmentation(self.prepare_segmentation_input(data_type))
#         zoning = cb.ZoningSystem.get_zoning('normits')

#         dvec = DVector(import_data=data, zoning_system=zoning, segmentation=seg)
#         if data_type == "tt_pop" :
#             dvec = dvec.add_segments(['adult_nssec'])

#         if data_type == "hh" :
#             dvec = dvec.add_segments(['adult_nssec', 'total'])

#         dvec.save(output_path)


def run(config: inputs.DLitConfig):

    """Main function to run the Tripends processing."""

    if config.run_tripend is None:
        raise ValueError("Cannot run tripend module without parameters")

    LOG.info("Initializing Tripend Module")
    config.output_folder.mkdir(exist_ok=True)
    base_year = config.dev_pattern.base_year
    base_folder = config.output_folder / "07_tripend"
    base_folder.mkdir(exist_ok=True)
    model_zone = config.dev_pattern.geo_boundary.value


    LOG.info("Instantiating TEConstraintProcessor")
    te_cp = TEConstraintProcessor(config)
    zone_translation = te_cp.load_zone_translation(model_zone)

    
    LOG.info("Loading and transforming base year trip end data")
    # Load datasets
    datasets = {
        "by_hb_fr": pd.read_csv(config.tripend.by_fr_hb),
        "by_hb_to": pd.read_csv(config.tripend.by_to_hb),
        "by_nhb": pd.read_csv(config.tripend.by_nhb)
    }

    # Process each dataset
    results = {}
    for name, df in datasets.items():
        total_prod_before = df["prod"].sum()
        total_attr_before = df["attr"].sum()
        prod_before = df.groupby(["purpose", "mode", "period"])["prod"].sum()
        attr_before = df.groupby(["purpose", "mode", "period"])["attr"].sum()        
        transformer = ByTeTransformation(df, base_year)
           
        # Store transformed DataFrame
        results[name] = {
            "prod_before": prod_before,
            "attr_before": attr_before,
            "prod": transformer.groupby_sum("prod"),
            "attr": transformer.groupby_sum("attr"),
        }
    # Initialize summary table to check total before and after transformation
    summary_data = []
    for name in results.keys():
        # Get total sum from aggregated "prod" and "attr"
        total_prod = results[name]["prod"][str(base_year)].sum()
        total_attr = results[name]["attr"][str(base_year)].sum()
        # Store summary data
        summary_data.append({
            "dataset": name,
            "total_prod_beforetrans": total_prod_before,
            "total_attr_beforetrans": total_attr_before,
            "total_prod": total_prod,
            "total_attr": total_attr,
        })
    # Convert summary data to DataFrame
    summary_df = pd.DataFrame(summary_data)
    utilities.write_to_csv(base_folder / "by_te_summary.csv", summary_df)

    utilities.write_to_csv(base_folder / "by_hb_fr_prod_before.csv", results["by_hb_fr"]["prod_before"])
    utilities.write_to_csv(base_folder / "by_hb_fr_attr_before.csv", results["by_hb_fr"]["attr_before"])
    utilities.write_to_csv(base_folder / "by_hb_to_prod_before.csv", results["by_hb_to"]["prod_before"])
    utilities.write_to_csv(base_folder / "by_hb_to_attr_before.csv", results["by_hb_to"]["attr_before"])
    
    # Write transformed data to CSV
    utilities.write_to_csv(base_folder / "by_hb_fr_prod.csv", results["by_hb_fr"]["prod"])
    utilities.write_to_csv(base_folder / "by_hb_fr_attr.csv", results["by_hb_fr"]["attr"])
    utilities.write_to_csv(base_folder / "by_hb_to_prod.csv", results["by_hb_to"]["prod"])
    utilities.write_to_csv(base_folder / "by_hb_to_attr.csv", results["by_hb_to"]["attr"])



    # LOG.info("Loading land use data processed by previous dev_pattern module")
    # data_files = te_cp.load_dlog_lu_data(model_zone)

    # data_frames = {key: pd.read_csv(path).fillna(0) for key, path in data_files.items()}


    # output_folders = {
    #     "soc_sic_emp": base_folder / "dlog_soc_sic_emp",
    #     "tt_pop": base_folder / "dlog_tt_pop",
    #     "hh": base_folder / "dlog_hh"
    # }

    # for folder in output_folders.values():
    #     folder.mkdir(exist_ok=True)
    

    # LOG.info("Transforming and processing land use data for tripend module")
    # tripend = Tripends(config)

    # for dtype in ["soc_sic_emp", "tt_pop", "hh"]:
    #     processed_data = tripend.process_data(
    #         data_frames[dtype], 
    #         dtype, 
    #         data_frames["tfn_tt"] if dtype == "tt_pop" else None
    #     )

    #     for year_col, pivot_df in processed_data.items():
    #         category = next((cat for cat in tripend.CATEGORIES if cat in year_col), '')

    #         output_folder = output_folders[dtype] / f"{dtype}{category}"
    #         output_folder.mkdir(exist_ok=True)

    #         output_file = output_folder / f"dlog_{dtype}_{year_col}.dvec"
    #         tripend.perform_segmentation_and_zoning(pivot_df, dtype, output_file)
    
    LOG.info("Generating or Loading Dlog trip end data")
    if config.tripend.tripend_data is not None:
        trip_end_data =  parser.parse_tripend_input(config)
    else:
        trip_end_data = None # later, it should be caf.tem


    LOG.info("Data processing completed")