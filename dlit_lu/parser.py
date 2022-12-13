"""parses the DLog data and auxiliary data 
"""
# standard imports
import pathlib
import logging

# third party imports
import pandas as pd
import geopandas as gpd

# local imports
from dlit_lu import global_classes

# constants
LOG = logging.getLogger(__name__)


def parse_dlog(
    input_file_path: pathlib.Path,
    comb_sheet_name: str,
    res_sheet_name: str,
    emp_sheet_name: str,
    mix_sheet_name: str,
    lookup_sheet_name: str,
    comb_column_names_path: pathlib.Path,
    res_column_names_path: pathlib.Path,
    emp_column_names_path: pathlib.Path,
    mix_column_names_path: pathlib.Path,
) -> global_classes.DLogData:
    """parses the dlog excel spreadsheet

    creates a pd.DataFrame for each of the sheets and creates a DLogData object

    Parameters
    ----------
    input_file_path : pathlib.Path
        file path for dlog excel file
    comb_sheet_name : str
        combined sheet name in spreadsheet
    res_sheet_name : str
        residential sheet name in spreadsheet
    emp_sheet_name : str
        employment sheet name in spreadsheet
    mix_sheet_name : str
        mixed sheet name in spreadsheet
    lookup_sheet_name : str
        lookup sheet name in spreadsheet
    comb_column_names : list[str]
        combined column names
    res_column_names : list[str]
        residential column names
    emp_column_names : list[str]
        employment column names
    mix_column_names : list[str]
        mixed column names

    Returns
    -------
    global_classes.DLogData
        the parsed data
    """

    comb_column_names= pd.read_csv(comb_column_names_path).iloc[:,0].tolist()
    res_column_names= pd.read_csv(res_column_names_path).iloc[:,0].tolist()
    emp_column_names= pd.read_csv(emp_column_names_path).iloc[:,0].tolist()
    mix_column_names= pd.read_csv(mix_column_names_path).iloc[:,0].tolist()

    LOG.info("Parsing Residential sheet")
    residential_data = parse_sheet(input_file_path, res_sheet_name, 2, res_column_names)
    LOG.info("Parsing Employment sheet")
    employment_data = parse_sheet(input_file_path, emp_sheet_name, 2, emp_column_names)
    LOG.info("Parsing Mixed sheet")
    mixed_data = parse_sheet(input_file_path, mix_sheet_name, 2, mix_column_names)
    LOG.info("Parsing Combined sheet")
    combined_data = parse_sheet(input_file_path, comb_sheet_name, 1, comb_column_names)
    LOG.info("Parsing Lookup sheet")
    lookup = parse_lookup(input_file_path, lookup_sheet_name)

    data_exit = global_classes.DLogData(
        combined_data=combined_data,
        residential_data=residential_data,
        employment_data=employment_data,
        mixed_data=mixed_data,
        lookup=lookup,
    )
    return data_exit


def parse_sheet(
    input_file_path: pathlib.Path,
    sheet_name: str,
    skip_rows: int,
    column_names: list[str],
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
    column_names : list[str]
        the column names of the excel spread sheet

    Returns
    -------
    pd.DataFrame
        sheet parsed into dataframe
    """
    data = pd.read_excel(
        input_file_path, sheet_name=sheet_name, engine="openpyxl", skiprows=skip_rows
    )
    data.loc[:, "site_name"] = data["site_name"].astype("string")
    data["existing_land_use"] = parse_landuse_codes(data["existing_land_use"])
    data["proposed_land_use"] = parse_landuse_codes(data["proposed_land_use"])
    data.columns = [name.lower() for name in column_names]
    return data


def parse_landuse_codes(codes: pd.Series) -> pd.Series:
    """parses the land use code columns into lists

    some minor formatting ammendments to aid parsing

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
    codes = codes.str.replace("and", ",").str.replace("/", ",").str.replace(r"\s+", "")
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
    standard_format_tables = {}
    for key, value in table_location.items():
        table = pd.read_excel(
            input_file_path,
            sheet_name=lookup_sheet_name,
            engine="openpyxl",
            names=[key, "ID"],
            index_col=1,
            usecols=value,
        )
        table.dropna(how="any", inplace=True)
        standard_format_tables[key] = table

    # tables not formatted as standard
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
    local_authority.columns = ["ID", "local_authority"]
    local_authority.set_index("ID", drop=True, inplace=True)
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
    out_of_date_luc_path: pathlib.Path,
    incomplete_luc_path: pathlib.Path,
    lpa_shapefile_path: pathlib.Path
) -> global_classes.AuxiliaryData:
    """reads in auxilliary data

    e.g. incorrect land use codes and LPA regions

    Parameters
    ----------
    incorrect_luc_file_path : pathlib.Path
        path for incorrect luc excel spreadsheet
    LPA_shapefile_path : pathlib.Path
        lpa regions shapefile

    Returns
    -------
    global_classes.auxiliary_data
        named tuple to contain the auxiliary data
    """
    LOG.info("Parsing auxiliary files")
    #parse land use code file
    allowed_land_use_codes = pd.read_csv(valid_luc_path)
    allowed_land_use_codes.loc[:, "land_use_codes"] = allowed_land_use_codes[
        "land_use_codes"
    ].str.lower()
    out_of_date_luc = pd.read_csv(out_of_date_luc_path)
    out_of_date_luc.loc[:, "out_of_date_land_use_codes"] = out_of_date_luc[
        "out_of_date_land_use_codes"
    ].str.lower()
    incomplete_luc = pd.read_csv(incomplete_luc_path)
    incomplete_luc.loc[:, "incomplete_land_use_codes"] = incomplete_luc[
        "incomplete_land_use_codes"
    ].str.lower()
    #parse local planning regions
    regions = gpd.read_file(lpa_shapefile_path)#TODO add column names to config file
    return global_classes.AuxiliaryData(
        allowed_land_use_codes, out_of_date_luc, incomplete_luc, regions
    )
