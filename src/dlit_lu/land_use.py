# standard imports
import logging
import pathlib
# third party imports
import pandas as pd
# local imports
from dlit_lu import utilities, global_classes, parser, inputs

# constants
LOG = logging.getLogger(__name__)

def run(input_data: global_classes.DLogData, config: inputs.DLitConfig):
    LOG.info("Initialising Land Use Module")
    
    config.output_folder.mkdir(exist_ok=True)
    
    msoa = parser.parse_msoa(config.msoa_shapefile_path)
    LOG.info("Disaggregating mixed into residential and employment")
    data = utilities.disagg_mixed(
        utilities.to_dict(input_data))

    emp_unit_year_columns = list(filter(lambda x: x.startswith("emp_year_"), data["employment"].columns.to_list()))+["units_(floorspace)"]
    res_unit_year_columns = list(filter(lambda x: x.startswith("res_year_"), data["residential"].columns.to_list()))+["units_(dwellings)"]

    construction_land_use_data = data.copy()
    demolition_land_use_data = data.copy()

    LOG.info("Disaggregating LUCs")

    construction_land_use_data["employment"] = utilities.disagg_land_use_codes(construction_land_use_data["employment"], "proposed_land_use",
        emp_unit_year_columns, 
        input_data.proposed_land_use_split)

    demolition_land_use_data["employment"] = utilities.disagg_land_use_codes(demolition_land_use_data["employment"], "existing_land_use",
        emp_unit_year_columns, 
        input_data.existing_land_use_split)
    
    demolition_land_use_data["residential"] = utilities.disagg_land_use_codes(demolition_land_use_data["residential"], "existing_land_use",
        res_unit_year_columns, 
        input_data.existing_land_use_split)
    for disagg_data in [demolition_land_use_data, construction_land_use_data]:
        LOG.info("performing MSOA geospatial lookup") 
        msoa_sites = utilities.msoa_site_geospatial_lookup(disagg_data, msoa)

        LOG.info("Disaggregating dwellings into population by dwelling type")
        msoa_pop_column_names = ["zone_id", "dwelling_type", "n_uprn", "pop_per_dwelling", "population"]

        msoa_sites["residential"]= utilities.disagg_dwelling(
            msoa_sites["residential"],
            config.msoa_dwelling_pop_path,
            msoa_pop_column_names,
            res_unit_year_columns,
            )
        LOG.info("Trimming data")

        res_keep_columns  =  res_unit_year_columns+["msoa11cd", "dwelling_type", "existing_land_use" ]
        emp_keep_columns = emp_unit_year_columns + ["msoa11cd", "existing_land_use", "proposed_land_use" ]
        msoa_sites["residential"] = msoa_sites["residential"][res_keep_columns]
        msoa_sites["employment"] = msoa_sites["employment"][emp_keep_columns]

        LOG.info("Writing Land Use disaggregation and geospatial lookup results")

    
def rebase_to_msoa(data: dict[str,pd.DataFrame])->pd.DataFrame:
    res_data = data["residential"]
    emp_data = data["employment"]
    
    



    LOG.info("Ending Land Use Module")