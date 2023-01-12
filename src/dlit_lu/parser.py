"""parses the DLog data and auxiliary data 
"""
# standard imports
import pathlib
import logging
from typing import Optional

# third party imports
import pandas as pd
import geopandas as gpd

# local imports
from dlit_lu import global_classes, inputs

# constants
LOG = logging.getLogger(__name__)


def parse_dlog(
    config: inputs.DLitConfig
) -> global_classes.DLogData:
    """parses the dlog excel spreadsheet

    creates a pd.DataFrame for each of the sheets and creates a DLogData object

    Parameters
    ----------
    config: inputs.DLit.Config
        config object generated from the config yaml file

    Returns
    -------
    global_classes.DLogData
        the parsed data
    """
    # read in column names
    res_column_names = pd.read_csv(
        config.residential_column_names_path).iloc[:, 0].tolist()
    emp_column_names = pd.read_csv(
        config.employment_column_names_path).iloc[:, 0].tolist()
    mix_column_names = pd.read_csv(
        config.mixed_column_names_path).iloc[:, 0].tolist()

    #read in column to remove from data 
    ignore_columns = pd.read_csv(
        config.ignore_columns_path).iloc[:, 0].str.lower().tolist()

    #parse sheets
    LOG.info("Parsing Residential sheet")
    residential_data = parse_sheet(
        config.dlog_input_file, config.residential_sheet_name, 2, res_column_names, ignore_columns)
    LOG.info("Parsing Employment sheet")
    employment_data = parse_sheet(
        config.dlog_input_file, config.employment_sheet_name, 2, emp_column_names, ignore_columns)
    LOG.info("Parsing Mixed sheet")
    mixed_data = parse_sheet(
        config.dlog_input_file, config.mixed_sheet_name, 2, mix_column_names, ignore_columns)
    LOG.info("Parsing Lookup sheet")
    lookup = parse_lookup(config.dlog_input_file, config.lookups_sheet_name)

    data_output = global_classes.DLogData(
        combined_data=None,
        residential_data=residential_data,
        employment_data=employment_data,
        mixed_data=mixed_data,
        lookup=lookup,
    )
    return data_output


def parse_sheet(
    input_file_path: pathlib.Path,
    sheet_name: str,
    skip_rows: int,
    column_names: Optional[list[str]] = None,
    ignore_columns: Optional[list[str]] = None,
) -> pd.DataFrame:
    """parses a sheet in a excel spread sheet

    Parameters
    ----------
    input_file_path : pathlib.Path
        the excel file to parse
    sheet_name : str
        the name of the sheet to parse
    skip_rows : int
        number of rows to skip
    column_names : Optional[list[str]] optional
        , by default None
        the column names of the excel spread sheet
    ignore_columns : Optional[list[str]] optional
        , by default None
        the column names of columns that will be returned in the
        output. If None all columns are returned

    Returns
    -------
    pd.DataFrame
        sheet parsed into dataframe
    """
    data = pd.read_excel(
        input_file_path, sheet_name=sheet_name, engine="openpyxl", skiprows=skip_rows
    )
    data["existing_land_use"] = parse_landuse_codes(data["existing_land_use"])
    data["proposed_land_use"] = parse_landuse_codes(data["proposed_land_use"])

    if column_names is not None:
        data.columns = [name.lower() for name in column_names]
    else:
        data.columns = data.columns.str.lower()

    if ignore_columns is not None:
        data = data.drop(columns=ignore_columns)

    return data


def parse_landuse_codes(codes: pd.Series) -> pd.Series:
    """parses the land use code columns into lists

    some formatting ammendments to aid parsing and analysis

    Parameters
    ----------
    codes : pd.Series
        a land use code column of strings

    Returns
    -------
    pd.Series
        a land use code column parsed into a list
    """

    codes = codes.astype("string")
    codes = codes.str.lower()
    codes = codes.str.replace("and", ",").str.replace(
        "/", ",").str.replace(r"\s+", "")
    codes = codes.str.replace(
        "[", "").str.replace("]", "").str.replace("\'", "")
    codes = codes.str.split(",")
    return codes


def parse_lookup(
    input_file_path: pathlib.Path, lookup_sheet_name: str
) -> global_classes.Lookup:
    """parses the lookup tables from the lookup sheet
    Parameters
    ----------
    input_file_path : pathlib.Path
        DLog file path
    lookup_sheet_name : str
        the lookup sheet name within the excel document

    Returns
    -------
    Lookup:
        the lookup tables formatted within a NamedTuple
    """
    # tables column location within worksheet, doesnt unclude tables with non-standard format
    table_location = {
        "site_type": "A:B",
        "construction_status": "D:E",
        "planning_status": "G:H",
        "webtag": "J:K",
        "development_type": "M:N",
        "years": "P:Q",
        "distribution_profile": "S:T",
        "adoption_status": "X:Y",
    }
    #parse standard format sheets
    standard_format_tables = {}
    for key, value in table_location.items():
        table = pd.read_excel(
            input_file_path,
            sheet_name=lookup_sheet_name,
            engine="openpyxl",
            names=[key, "id"],
            index_col=1,
            usecols=value,
        )
        table.dropna(how="any", inplace=True)
        standard_format_tables[key] = table

    #parse no standard format sheets

    land_use_codes = pd.read_excel(
        input_file_path,
        sheet_name=lookup_sheet_name,
        engine="openpyxl",
        usecols="V",
    ).squeeze("columns")
    # ^ series as 1 col and squeeze called

    land_use_codes.dropna(how="any", inplace=True)
    land_use_codes = land_use_codes.str.lower()

    local_authority = pd.read_excel(
        input_file_path,
        sheet_name=lookup_sheet_name,
        header=None,
        engine="openpyxl",
        usecols="Z:AA",
    )
    # ^ No header in worksheet, id and value switched wrt others

    local_authority.columns = ["id", "local_authority"]
    local_authority.set_index("id", drop=True, inplace=True)
    local_authority.dropna(how="any", inplace=True)

    lookup_exit_variable = global_classes.Lookup(
        site_type=standard_format_tables["site_type"],
        construction_status=standard_format_tables["construction_status"],
        webtag=standard_format_tables["webtag"],
        development_type=standard_format_tables["development_type"],
        planning_status=standard_format_tables["planning_status"],
        years=standard_format_tables["years"],
        distribution_profile=standard_format_tables["distribution_profile"],
        land_use_codes=land_use_codes,
        adoption_status=standard_format_tables["adoption_status"],
        local_authority=local_authority,
    )
    return lookup_exit_variable


def read_auxiliary_data(
    valid_luc_path: pathlib.Path,
    known_invalid_luc_path: pathlib.Path,
    out_of_date_luc_path: pathlib.Path,
    incomplete_luc_path: pathlib.Path,
    lpa_shapefile_path: pathlib.Path
) -> global_classes.AuxiliaryData:
    """
    reads in auxilliary data

    e.g. incorrect land use codes and LPA regions

    Parameters
    ----------
    valid_luc_path : pathlib.Path
        path to csv file containing allowed land use codes
    known_invalid_luc_path : pathlib.Path
        path to csv file that contains a lookup for known errors
        that cannot be fixed automatically. 
    out_of_date_luc_path : pathlib.Path
        path to csv file that contains a lookup for out of date codes
        and their indate replacements
    incomplete_luc_path : pathlib.Path
        path to csv file that contains a lookup incomplete codes and 
        their possible replacements
    lpa_shapefile_path : pathlib.Path
        path to the LPA shape file, used for analysis results graph

    Returns
    -------
    global_classes.AuxiliaryData
        auxiliary data parsed into pd.Dataframes contained within the 
        object
    """
    LOG.info("Parsing auxiliary files")
    # parse land use code file
    allowed_land_use_codes = pd.read_csv(valid_luc_path)
    allowed_land_use_codes.loc[:, "land_use_codes"] = allowed_land_use_codes[
        "land_use_codes"
    ].str.lower().str.replace(r"\s+", "")

    #parse out of date land use codes
    out_of_date_luc = pd.read_csv(out_of_date_luc_path)
    out_of_date_luc.loc[:, "out_of_date_land_use_codes"] = out_of_date_luc[
        "out_of_date_land_use_codes"
    ].str.lower().str.replace(r"\s+", "")
    out_of_date_luc.loc[:, "replacement_codes"] = parse_landuse_codes(out_of_date_luc[
        "replacement_codes"
    ])

    #parse incomplete land use codes
    incomplete_luc = pd.read_csv(incomplete_luc_path)
    incomplete_luc.loc[:, "incomplete_land_use_codes"] = incomplete_luc[
        "incomplete_land_use_codes"
    ].str.lower().str.replace(r"\s+", "")
    incomplete_luc.loc[:, "land_use_code"] = parse_landuse_codes(incomplete_luc[
        "land_use_code"
    ])

    #parse known invalid land use codes
    known_invalid_luc = pd.read_csv(known_invalid_luc_path)
    known_invalid_luc.loc[:, "known_invalid_code"] = known_invalid_luc[
        "known_invalid_code"
    ].str.lower().str.replace(r"\s+", "")
    known_invalid_luc.loc[:, "corrected_code"] = parse_landuse_codes(known_invalid_luc[
        "corrected_code"
    ])
    
    # parse local planning regions
    # TODO add column names to config file
    regions = gpd.read_file(lpa_shapefile_path)
    return global_classes.AuxiliaryData(
        allowed_land_use_codes, known_invalid_luc, out_of_date_luc, incomplete_luc, regions
    )
