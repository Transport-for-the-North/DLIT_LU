# standard imports
import logging
import pathlib
# third party imports
# local imports
from dlit_lu import utilities, global_classes, parser, inputs

# constants
LOG = logging.getLogger(__name__)

def run(input_data: global_classes.DLogData, config: inputs.DLitConfig):
    LOG.info("Disaggregating mixed into residential and employment")
    
    msoa = parser.parse_msoa(config.msoa_shapefile_path)

    data = utilities.disagg_mixed(
        utilities.to_dict(input_data))

    LOG.info("Disaggregating proposed LUCs")

    emp_unit_year_columns = list(filter(lambda x: x.startswith("emp_year_"), data["employment"].columns.to_list()))+["units_(floorspace)"]
    data["employment"] = utilities.disagg_land_use_codes(data["employment"], "proposed_land_use",
        emp_unit_year_columns, 
        input_data.proposed_land_use_split)
     
    msoa_sites = utilities.msoa_site_geospatial_lookup(data, msoa)

    utilities.disagg_dwelling(msoa_sites["residential"], config.msoa_dwelling_pop_path)

    utilities.write_to_excel(config.output_folder/ "site_with_msoa.xlsx", msoa_sites)