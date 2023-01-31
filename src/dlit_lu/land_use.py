# standard imports
import logging
# third party imports
import pandas as pd
import pathlib
# local imports
from dlit_lu import utilities, global_classes, parser, inputs
DEMOLITION_ANALYSIS = True
CONSTRUCTION_ANALYSIS = True
# constants
LOG = logging.getLogger(__name__)


def run(input_data: global_classes.DLogData, config: inputs.DLitConfig):
    """runs process for converting DLOG to MSOA build out profiles

    disaggregaes mixed into employment and residential and land use codes
    applys dwelling types using land use split by MSOA
    rebases to MSOA build-out profiles

    Parameters
    ----------
    input_data : global_classes.DLogData
        data to perform conversion 
    config : inputs.DLitConfig
        config file
    """
    LOG.info("Initialising Land Use Module")

    config.output_folder.mkdir(exist_ok=True)


    traveller_type_factor = analyse_traveller_type_distribution(config.msoa_traveller_type_path)

    msoa = parser.parse_msoa(config.msoa_shapefile_path)
    LOG.info("Disaggregating mixed into residential and employment")
    data = utilities.disagg_mixed(
        utilities.to_dict(input_data))

    emp_unit_year_columns = list(filter(lambda x: x.startswith(
        "emp_year_"), data["employment"].columns.to_list()))+["units_(floorspace)"]
    res_unit_year_columns = list(filter(lambda x: x.startswith(
        "res_year_"), data["residential"].columns.to_list()))+["units_(dwellings)"]

    disagg_lucs = {}

    if CONSTRUCTION_ANALYSIS:
        LOG.info("Disaggregating employment proposed LUCs")
        construction_land_use_data = data.copy()
        construction_land_use_data["employment"] = utilities.disagg_land_use_codes(construction_land_use_data["employment"], "proposed_land_use",
                                                                                   emp_unit_year_columns,
                                                                                   input_data.proposed_land_use_split)
        disagg_lucs["construction"] = construction_land_use_data

    if DEMOLITION_ANALYSIS:
        demolition_land_use_data = data.copy()

        LOG.info("Disaggregating employment existing LUCs")

        demolition_land_use_data["employment"] = utilities.disagg_land_use_codes(demolition_land_use_data["employment"], "existing_land_use",
                                                                                 emp_unit_year_columns,
                                                                                 input_data.existing_land_use_split)
        LOG.info("Disaggregating residential existing LUCs")
        demolition_land_use_data["residential"] = utilities.disagg_land_use_codes(demolition_land_use_data["residential"], "existing_land_use",
                                                                                  res_unit_year_columns,
                                                                                  input_data.existing_land_use_split)

        disagg_lucs["demolition"] = demolition_land_use_data

    msoa_base = {}
    for key, disagg_data in disagg_lucs.items():
        LOG.info("performing MSOA geospatial lookup")
        msoa_sites = utilities.msoa_site_geospatial_lookup(disagg_data, msoa)

        LOG.info("Disaggregating dwellings into population by dwelling type")
        msoa_pop_column_names = ["zone_id", "dwelling_type",
                                 "n_uprn", "pop_per_dwelling", "zone", "pop_aj_factor" ,"population"]

        msoa_sites["residential"] = utilities.disagg_dwelling(
            msoa_sites["residential"],
            config.msoa_dwelling_pop_path,
            msoa_pop_column_names,
            res_unit_year_columns,
        )

        LOG.info("Trimming data")

        if key == "construction":
            res_keep_columns = res_unit_year_columns + \
                ["msoa11cd", "dwelling_type"]
            emp_keep_columns = emp_unit_year_columns + \
                ["msoa11cd", "proposed_land_use"]
            res_rename = {"dwelling_type": "land_use"}
            emp_rename = {"proposed_land_use": "land_use"}

        elif key == "demolition":
            res_keep_columns = res_unit_year_columns + \
                ["msoa11cd", "existing_land_use"]
            emp_keep_columns = emp_unit_year_columns + \
                ["msoa11cd", "existing_land_use"]
            res_rename = {"existing_land_use": "land_use"}
            emp_rename = {"existing_land_use": "land_use"}

        LOG.info("Rebasing to MSOA and Land use")
        msoa_sites["residential"] = msoa_sites["residential"][res_keep_columns].rename(
            columns=res_rename)
        msoa_sites["employment"] = msoa_sites["employment"][emp_keep_columns].rename(
            columns=emp_rename)

        msoa_base[key] = rebase_to_msoa(msoa_sites)

        res_file_name = "residential_"+ key + "_msoa_build_out.csv"
        emp_file_name = "employment_"+ key + "_msoa_build_out.csv"

        if key == "construction":
            LOG.info("Disaggregating by traveller type")
            msoa_base[key]["residential"] = apply_pop_land_use(msoa_base[key]["residential"], res_unit_year_columns, traveller_type_factor) 
            msoa_base[key]["residential"].rename({"land_use":"dwelling_type"}, inplace = True)

        #figure out method to automatically out digit of names  
        
        LOG.info("Writing Land Use disaggregation and geospatial lookup results")
        utilities.write_to_csv(
            config.output_folder / res_file_name, msoa_base[key]["residential"])
        utilities.write_to_csv(
            config.output_folder / emp_file_name, msoa_base[key]["employment"])

    

    LOG.info("Ending Land Use Module")


def rebase_to_msoa(data: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """rebases to MSOA build out profiles

    converts data from site_reference_id base to MSOA and land use

    Parameters
    ----------
    data : dict[str,pd.DataFrame]
        data to rebase

    Returns
    -------
    pd.DataFrame
        rebased data
    """
    res_data = data["residential"].set_index("msoa11cd", "land_use")
    emp_data = data["employment"].set_index("msoa11cd", "land_use")
    res_data = res_data.groupby(["msoa11cd", "land_use"]).sum()
    emp_data = emp_data.groupby(["msoa11cd", "land_use"]).sum()
    return {"residential": res_data, "employment": emp_data}

def analyse_traveller_type_distribution(file_path: pathlib.Path)-> pd.DataFrame:
    """calculates the factors for each traveller type 

    aggregates across all zones and dwelling types

    Parameters
    ----------
    file_path : pathlib.Path
        file path to TfN population land use

    Returns
    -------
    pd.DataFrame
        contains factors for msoa traveller type
    """    
    data = pd.read_csv(file_path)
    agg_zones = data.groupby("tfn_traveller_type").sum()
    ratios = (agg_zones["people"]/agg_zones["people"].sum()).reset_index(drop=False)
    msoa_ratios = []
    for id_ in data["msoa_zone_id"].unique():
        temp = ratios.copy()
        temp["msoa_zone_id"] = pd.Series([id_]).repeat(len(ratios)).reset_index(drop=True)
        msoa_ratios.append(temp)
    all_msoa_ratios = pd.concat(msoa_ratios, axis = 0).set_index(["msoa_zone_id", "tfn_traveller_type"])
    all_msoa_ratios.columns = ["ratios"]
    return all_msoa_ratios

def apply_pop_land_use(data: pd.DataFrame, unit_columns: list[str],  tt_factors: pd.DataFrame)->pd.DataFrame:
    data_ratios = data.reset_index(drop=False).merge(tt_factors.reset_index(drop = False), left_on = "msoa11cd", right_on = "msoa_zone_id").set_index(["msoa11cd", "land_use", "tfn_traveller_type"])
    data_ratios = data_ratios.loc[:,unit_columns].multiply(data_ratios["ratios"], axis = 0)
    return data_ratios