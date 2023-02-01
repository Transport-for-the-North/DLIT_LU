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

    average_infill_values = inputs.InfillingAverages.load_yaml(config.output_folder/inputs.AVERAGE_INFILLING_VALUES_FILE)

    traveller_type_factor = analyse_traveller_type_distribution(config.land_use.msoa_traveller_type_path)

    msoa = parser.parse_msoa(config.land_use.msoa_shapefile_path)
    LOG.info("Disaggregating mixed into residential and employment")
    data = utilities.disagg_mixed(
        utilities.to_dict(input_data))

    emp_unit_year_columns = list(filter(lambda x: x.startswith(
        "emp_year_"), data["employment"].columns.to_list()))
    res_unit_year_columns = list(filter(lambda x: x.startswith(
        "res_year_"), data["residential"].columns.to_list()))


    LOG.info("Disaggregating employment proposed LUCs")
    construction_land_use_data = data.copy()
    construction_land_use_data["employment"] = utilities.disagg_land_use_codes(construction_land_use_data["employment"], "proposed_land_use",
                                                                                emp_unit_year_columns,
                                                                                input_data.proposed_land_use_split)

    demolition_land_use_data = data.copy()
    demolition_land_use_data["residential"] = convert_to_GFA(
        demolition_land_use_data["residential"],
        "total_site_area_size_hectares",
        "units_(dwellings)",
        res_unit_year_columns,
        average_infill_values.average_gfa_site_area_ratio)

    LOG.info("Disaggregating employment existing LUCs")

    demolition_land_use_data["employment"] = utilities.disagg_land_use_codes(demolition_land_use_data["employment"], "existing_land_use",
                                                                                emp_unit_year_columns,
                                                                                input_data.existing_land_use_split)
    LOG.info("Disaggregating residential existing LUCs")
    demolition_land_use_data["residential"] = utilities.disagg_land_use_codes(demolition_land_use_data["residential"], "existing_land_use",
                                                                                res_unit_year_columns,
                                                                                input_data.existing_land_use_split)

    demolition_land_use_data["residential"].loc[:,res_unit_year_columns] = -demolition_land_use_data["residential"].loc[:,res_unit_year_columns]
    demolition_land_use_data["employment"].loc[:,emp_unit_year_columns] = -demolition_land_use_data["employment"].loc[:,emp_unit_year_columns]
    
    construction_land_use_data["employment"]["land_use"]= construction_land_use_data["employment"]["proposed_land_use"]
    demolition_land_use_data["employment"]["land_use"] = demolition_land_use_data["employment"]["existing_land_use"]
    demolition_land_use_data["residential"]["land_use"] = demolition_land_use_data["residential"]["existing_land_use"]

    residential_build_out =  construction_land_use_data["residential"]
    employment_build_out = pd.concat([
        construction_land_use_data["employment"],
        demolition_land_use_data["employment"],
        demolition_land_use_data["residential"]
        ], ignore_index=True)

    LOG.info("performing MSOA geospatial lookup")
    res_msoa_sites = utilities.msoa_site_geospatial_lookup(residential_build_out, msoa)
    emp_msoa_sites = utilities.msoa_site_geospatial_lookup(employment_build_out, msoa)

    LOG.info("Disaggregating dwellings into population by dwelling type")
    msoa_pop_column_names = ["zone_id", "dwelling_type",
                                "n_uprn", "pop_per_dwelling", "zone", "pop_aj_factor" ,"population"]

    res_msoa_sites = utilities.disagg_dwelling(
        res_msoa_sites,
        config.land_use.msoa_dwelling_pop_path,
        msoa_pop_column_names,
        res_unit_year_columns,
    )


    res_msoa_sites = res_msoa_sites.loc[:,res_unit_year_columns + \
        ["msoa11cd", "dwelling_type"]]
    emp_msoa_sites = emp_msoa_sites.loc[:, emp_unit_year_columns + \
        ["msoa11cd", "land_use"]]

    LOG.info("Rebasing to MSOA and Land use")

    res_msoa_base = res_msoa_sites.groupby(["msoa11cd", "dwelling_type"]).sum()
    emp_msoa_base = emp_msoa_sites.groupby(["msoa11cd", "land_use"]).sum()


    LOG.info("Disaggregating by traveller type")
    res_msoa_sites = apply_pop_land_use(res_msoa_sites, res_unit_year_columns, traveller_type_factor) 

    res_msoa_base.rename({"msoa11cd":"msoa_zone_id"}, inplace=True)
    emp_msoa_base.rename({"msoa11cd":"msoa_zone_id"}, inplace=True)

    res_msoa_base = clean_column_names(res_msoa_base)
    emp_msoa_base = clean_column_names(emp_msoa_base)
    
    res_file_name = "residential_msoa_build_out.csv"
    emp_file_name = "employment_msoa_build_out.csv"
    
    LOG.info("Writing Land Use disaggregation and geospatial lookup results")
    utilities.write_to_csv(
        config.output_folder / res_file_name, res_msoa_base)
    utilities.write_to_csv(
        config.output_folder / emp_file_name, emp_msoa_base)

    LOG.info("Ending Land Use Module")


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
    data_ratios = data.reset_index(drop=False).merge(tt_factors.reset_index(drop = False), left_on = "msoa11cd", right_on = "msoa_zone_id").set_index(["msoa11cd", "dwelling_type", "tfn_traveller_type"])
    data_ratios = data_ratios.loc[:,unit_columns].multiply(data_ratios["ratios"], axis = 0)
    return data_ratios

def clean_column_names(df):

    cols = df.columns
    new_cols = {}
    for col in cols:
        # Check if the column name contains a digit
        if any(char.isdigit() for char in col):
            # If it does, extract the digits from the string and use them as the new column name
            new_col = "".join(filter(str.isdigit, col))
            new_cols[col] = new_col
        else:
            # If it doesn't, use the original column name
            new_cols[col] = col
    df.rename(columns=new_cols, inplace=True)
    return df

def convert_to_GFA(data:pd.DataFrame, area_col: str, unit_col: str, unit_year_columns: list[str], factor: float)->pd.DataFrame:
    data_to_gfa = data.copy()
    data_to_gfa.loc[:,unit_col]= data_to_gfa[area_col]*factor
    data_to_gfa.loc[:,unit_year_columns] = data.loc[:, unit_year_columns].divide(data.loc[:, unit_col], axis = 0).multiply(data_to_gfa[unit_col], axis = 0)
    return data_to_gfa