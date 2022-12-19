"""Automatically fixes and infills data where possible

    IN PROGRESS
"""
# standard imports
import logging
from typing import Optional
# third party imports
import pandas as pd
import numpy as np
# local imports
from dlit_lu import global_classes, utilities

# constants
LOG = logging.getLogger(__name__)


def fix_inavlid_syntax(data: global_classes.DLogData, auxiliary_data: global_classes.AuxiliaryData) -> global_classes.DLogData:
    """fixes invalid syntax issues with inputted data data

    IN PROGRESS

    Parameters
    ----------
    data : global_classes.DLogData
        contains data to be tested should contain the filter columns outputted from analyse module
    auxiliary_data : global_classes.AuxiliaryData
        auxiliary data outted from parser

    Returns
    -------
    global_classes.DLogData
        data with syntax issues fixed
    """
    LOG.info("performing automatic fixes")
    data_dict = {"residential": data.residential_data,
                 "employment": data.employment_data, "mixed": data.mixed_data}
    data_dict = utilities.to_dict(data)
    corrected_format = fix_site_ref_id(data_dict)
    corrected_format = incorrect_luc_formatting(corrected_format, {"residential": ["existing_land_use"], "employment": [
                                                "existing_land_use", "proposed_land_use"], "mixed": ["existing_land_use", "proposed_land_use"]}, auxiliary_data)
    calc_luc_ratio = luc_ratio(corrected_format, auxiliary_data)

    dwelling_area_ratio = unit_area_ratio(dict((k, data_dict[k]) for k in ("residential", "mixed")), {"residential": "total_units", "mixed": "dwellings"}, {
        "residential": "total_site_area_size_hectares", "mixed": "total_area_ha"})

    floorspace_area_ratio = unit_area_ratio(dict((k, data_dict[k]) for k in ("employment", "mixed")), {
        "employment": "total_area_sqm", "mixed": "floorspace_sqm"}, {
        "employment": "site_area_ha", "mixed": "total_area_ha"})

    corrected_format = old_incomplete_known_luc(corrected_format, {"residential": ["existing_land_use"], "employment": [
        "existing_land_use", "proposed_land_use"], "mixed": ["existing_land_use", "proposed_land_use"]}, auxiliary_data)

    corrected_format = fix_missing_lucs(corrected_format, {"residential": ["existing_land_use"], "employment": [
        "existing_land_use", "proposed_land_use"], "mixed": ["existing_land_use", "proposed_land_use"]}, ["unknown", "mixed"], auxiliary_data.allowed_codes["land_use_codes"].to_list())
    return global_classes.DLogData(
        None,
        corrected_format["residential"],
        corrected_format["employment"],
        corrected_format["mixed"],
        data.lookup,
    )


def incorrect_luc_formatting(
    data: dict[str, pd.DataFrame],
    columns: dict[str, list[str]],
    auxiliary_data: global_classes.AuxiliaryData,
) -> dict[str, pd.DataFrame]:
    """fixes data with invalid land use code formatting

    Parameters
    ----------
    data : dict[str, pd.DataFrame]
        data to be fixed
    columns : dict[str, list[str]]
        columns that contain data that reqiures fixing
        should have identical keys to data
    auxiliary_data : global_classes.AuxiliaryData
        auxiliary_data outputted from parser

    Returns
    -------
    dict[str, pd.DataFrame]
        data with repaired land use codes
    """
    possible_error_codes = [
        s for s in auxiliary_data.allowed_codes["land_use_codes"].tolist() if "(" in s or ")" in s]
    wrong_format_check = [
        s.replace("(", "").replace(")", "") for s in possible_error_codes
    ]
    format_lookup = pd.DataFrame(
        [possible_error_codes, wrong_format_check]).transpose()
    format_lookup.columns = ["land_use_code", "incorrect_format"]
    format_lookup = format_lookup.append(
        {"land_use_code": "sg", "incorrect_format": "suigeneris"}, ignore_index=True)

    fixed_format = {}

    # no assumption required
    for key, value in data.items():
        # this ensures fixed format is a copy, not a pointer
        fixed_format[key] = value.copy()
        for column in columns[key]:
            fixed_format[key][column] = fixed_format[key][column].apply(
                find_and_replace_luc,
                lookup_table=format_lookup,
                find_column_name="incorrect_format",
                replace_column_name="land_use_code",
            )

    return fixed_format


def old_incomplete_known_luc(
    data: dict[str, pd.DataFrame],
    columns: dict[str, list[str]],
    auxiliary_data: global_classes.AuxiliaryData,
) -> dict[str, pd.DataFrame]:
    """fixes data with invalid land use code formatting

    Parameters
    ----------
    data : dict[str, pd.DataFrame]
        data to be fixed
    columns : dict[str, list[str]]
        columns that contain data that reqiures fixing
        should have identical keys to data
    auxiliary_data : global_classes.AuxiliaryData
        auxiliary_data outputted from parser

    Returns
    -------
    dict[str, pd.DataFrame]
        data with repaired land use codes
    """
    out_of_date_codes = auxiliary_data.out_of_date_luc
    incomplete_codes = auxiliary_data.incomplete_luc
    known_invalid_codes = auxiliary_data.known_invalid_luc
    fixed_format = {}

    # inferances required values correspond to > 1 new code
    for key, value in data.items():
        fixed_format[key] = value.copy()
        for column in columns[key]:
            fixed_format[key][column] = value[column].apply(
                find_and_replace_luc,
                lookup_table=known_invalid_codes,
                find_column_name="known_invalid_code",
                replace_column_name="corrected_code",
            )
            fixed_format[key][column] = value[column].apply(
                find_and_replace_luc,
                lookup_table=incomplete_codes,
                find_column_name="incomplete_land_use_codes",
                replace_column_name="land_use_code",
            )
            fixed_format[key][column] = fixed_format[key][column].apply(
                find_and_replace_luc,
                lookup_table=out_of_date_codes,
                find_column_name="out_of_date_land_use_codes",
                replace_column_name="replacement_codes",
            )

    return fixed_format


def find_lucs(data: pd.DataFrame, column: str, code: str) -> Optional[pd.DataFrame]:
    """returns all entries with a given land use code

    if no entrues have the land use code, None is returned

    Parameters
    ----------
    data : pd.DataFrame
        data to be filtered
    column : str
        column to be filtered
    code : str
        code to filter by

    Returns
    -------
    Optional[pd.DataFrame]
        filtered data, if no entries are found returns None
    """
    exploded_luc = data[column].explode()
    matching_lucs = exploded_luc[exploded_luc == code]
    if len(matching_lucs) == 0:
        return None
    matching_data = data.loc[matching_lucs.index, :]
    return matching_data


def fix_missing_lucs(
    data: dict[str, pd.DataFrame],
    columns: dict[str, list[str]],
    missing_values: list[str],
    fill_value: list[str]
    ) -> dict[str, pd.DataFrame]:
    """fills missing luc values

    currently fills the same value regardless of how the empty value is defined

    Parameters
    ----------
    data : dict[str, pd.DataFrame]
        data to be infilled
    columns : dict[str, str]
        columns to be infilled in data
    missing_values : list[str]
        defines missing values (empty value is automatically infilled)
    fill_value : list
        value to infill missing values with

    Returns
    -------
    dict[str, pd.DataFrame]
        infilled data
    """
    missing_values_lookup = pd.DataFrame([missing_values]).transpose()
    missing_values_lookup.columns = ["missing_values"]
    missing_values_lookup.loc[:, "fill_value"] = pd.Series(
        [fill_value]).repeat(len(missing_values_lookup)).reset_index(drop=True)
    fixed_codes = {}
    for key, value in data.items():
        fixed_codes[key] = value.copy()
        for column in columns[key]:
            fixed_codes[key][column] = fixed_codes[key][column].apply(
                find_and_replace_luc,
                lookup_table=missing_values_lookup,
                find_column_name="missing_values",
                replace_column_name="fill_value",
                fill_empty_value=fill_value,
            )
    return fixed_codes


def luc_ratio(
    data: dict[str, pd.DataFrame],
    auxiliary_data: global_classes.AuxiliaryData,
    columns: list[str] = ["proposed_land_use"]
    ) -> pd.DataFrame:
    """calculates the average floorspace taken by each  luc

    assumes the floorspace is evenly distributed between the 
    luc defined in each entry

    Parameters
    ----------
    data : dict[str, pd.DataFrame]
        data to be analysed
    auxiliary_data : global_classes.AuxiliaryData
        auxiliary data read in from parser
    columns : list[str], optional
        columns to analyse, by default ["proposed_land_use"]

    Returns
    -------
    pd.DataFrame
        the total count, total floorspace and average floorspace for
        each luc
    """    
    land_use_codes = auxiliary_data.allowed_codes
    land_use_codes_count = land_use_codes.copy()
    land_use_codes_count["count"] = 0
    land_use_codes_count["total_floorspace"] = 0
    for code in land_use_codes["land_use_codes"]:
        for _, value in data.items():
            for column in columns:

                code_in_entry = find_lucs(value, column, code)

                if code_in_entry is None:
                    continue

                have_floorspace = pd.DataFrame([code_in_entry["missing_areas_or_dwellings_no_site_area"],
                                                code_in_entry["missing_areas_or_dwellings_with_site_area"]]).transpose()
                have_floorspace = code_in_entry[~have_floorspace.any(axis=1)]
                have_floorspace.loc[:, "units_(floorspace)"] = have_floorspace["units_(floorspace)"] / \
                    have_floorspace[column].apply(lambda x: len(x))
                total_floorspace = have_floorspace["units_(floorspace)"].sum(
                )

                land_use_codes_count.loc[land_use_codes_count["land_use_codes"] == code, "count"] = land_use_codes_count.loc[land_use_codes_count["land_use_codes"] == code, "count"] + len(
                    have_floorspace)
                land_use_codes_count.loc[land_use_codes_count["land_use_codes"]
                                         == code, "total_floorspace"] = land_use_codes_count.loc[land_use_codes_count["land_use_codes"] == code, "total_floorspace"] + total_floorspace
    land_use_codes_count["average_floorspace"] = land_use_codes_count["total_floorspace"] / \
        land_use_codes_count["count"]
    return land_use_codes_count


def unit_area_ratio(data: dict[str, pd.DataFrame], unit_columns: dict[str, str], area_columns: dict[str, str]) -> float:
    """calculate the ratio for unit to area

    Only give identical units i.e. all dwelling or all floorspace 

    Parameters
    ----------
    data : dict[str, pd.DataFrame]
        data to be analysed
    unit_columns : dict[str, str]
        columns with unit (e.g. total_dwelling for residental)
        for each sheet, same keys as data
    area_columns : dict[str, str]
        columns with site area for each sheet, same keys as data

    Returns
    -------
    float
        _description_
    """    
    all_ratios = np.array([])
    for key, value in data.items():
        data_subset = value.loc[value["missing_area"] == False, :]
        data_subset = data_subset.loc[data_subset["missing_areas_or_dwellings_with_site_area"] == False, :]
        # data subset only contains entries with site area and dwelling/floorspace
        all_ratios = np.append(all_ratios, (data_subset[unit_columns[key]] /
                                            data_subset[area_columns[key]]))
    return all_ratios.mean()


def find_and_replace_luc(
    luc_entry: list[str],
    lookup_table: pd.DataFrame,
    find_column_name: str,
    replace_column_name: str,
    fill_empty_value: Optional[list[str]] = None
) -> pd.Series:
    """finds and replaces LUC using a lookup table

    able to deal with str and list[str] in replacement column

    Parameters
    ----------
    luc_column : pd.Series
        column to find and replace values in
    lookup_table : pd.DataFrame
        contains the find and replace values
    find_column_name : str
        name of the find column in lookup_table 
    replace_column_name : str
        name of the replace column in lookup_table 


    Returns
    -------
    pd.Series
        LUC codes that have had find and replace applied
    """
    try:
        if len(luc_entry) == 0:
            if fill_empty_value is not None:
                if isinstance(fill_empty_value, str):
                    return [fill_empty_value]
                elif isinstance(fill_empty_value, list):
                    return fill_empty_value
                else:
                    LOG.warning(
                        f"fill_empty_value is a {type(fill_empty_value)}, that is neither a str or list[str]")
            else:
                return []
    except TypeError:
        return []
    for find_code in lookup_table[find_column_name]:
        if find_code not in luc_entry:
            continue
        replacement_code = lookup_table.loc[
            lookup_table[find_column_name] == find_code, replace_column_name
        ].values[0]
        # deals with multiple of the same code
        for _ in range(luc_entry.count(find_code)):
            luc_entry.remove(find_code)

            if isinstance(replacement_code, str):
                luc_entry.append(replacement_code)
            elif isinstance(replacement_code, list):
                luc_entry = luc_entry + replacement_code
            else:
                LOG.warning(
                    f"{replace_column_name} contians {type(replacement_code)}, that is neither a str or list[str]")
    return luc_entry


def fix_site_ref_id(data: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """creates a sit reference id for entries that do not have one

    Parameters
    ----------
    data : dict[str, pd.DataFrame]
        data to be repaired, must have  missing_site_ref filter column

    Returns
    -------
    dict[str, pd.DataFrame]
        data with infilled site ref IDs
    """
    fixed_ids = {}
    # find max id value
    overall_max_id_value = 0
    for _, value in data.items():
        max_id_value = value.loc[value["missing_site_ref"]
                                 == False, "site_reference_id"].max()
        if max_id_value > overall_max_id_value:
            overall_max_id_value = max_id_value

    for key, value in data.items():
        fixed_ids[key] = value.copy()
        missing_ids = value[value["missing_site_ref"] == True]

        if len(missing_ids) == 0:
            continue
        # calculate new ids & reset max id value
        new_ids = overall_max_id_value + \
            np.arange(1, len(missing_ids)+1, dtype=int)
        overall_max_id_value = new_ids.max()

        fixed_ids[key].loc[value["missing_site_ref"]
                           == True, "site_reference_id"] = new_ids
    return fixed_ids
