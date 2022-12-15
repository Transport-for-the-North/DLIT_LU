"""Automatically fixes and infills data where possible

    IN PROGRESS
"""
# standard imports
import logging
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

    out_of_date_codes = auxiliary_data.out_of_date_luc
    incomplete_codes = auxiliary_data.incomplete_luc

    fixed_format = {}

    for key, value in data.items():
        # this ensures fixed format is a copy, not a pointer
        fixed_format[key] = value.copy()

        for column in columns[key]:
            # no assumption required
            fixed_format[key][column] = fixed_format[key][column].apply(
                find_and_replace_luc,
                lookup_table=format_lookup,
                find_column_name="incorrect_format",
                replace_column_name="land_use_code",
            )

            # inferances required values correspond to > 1 new code
            fixed_format[key][column] = fixed_format[key][column].apply(
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


def find_and_replace_lucs(
    luc_column: pd.Series,
    lookup_table: pd.DataFrame,
    find_column_name: str,
    replace_column_name: str,
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
    fixed_luc = []
    exploded_land_use_codes = (  # splits land use list into new rows with same index
        luc_column.explode()
    )

    for find_code in lookup_table[find_column_name]:

        if not (exploded_land_use_codes == find_code).any():
            continue

        replacement_code = lookup_table.loc[
            lookup_table[find_column_name] == find_code, replace_column_name
        ].values[0]

        exploded_land_use_codes = pd.Series(np.where(
            exploded_land_use_codes == find_code, replacement_code, exploded_land_use_codes))
    # this ensures that any added lists added will be seperated - easier to reassemble
    exploded_land_use_codes = (exploded_land_use_codes.explode())

    # iterate through unique indices in exploded landuse codes & reassemble
    for i in pd.DataFrame([exploded_land_use_codes.index]).iloc[0, :].unique():
        luc_entry = exploded_land_use_codes[i]
        if isinstance(luc_entry, str):
            fixed_luc.append([luc_entry])
        else:
            try:
                fixed_luc.append(luc_entry.tolist())
            except AttributeError:  # deals with empty cells
                LOG.debug(f"failed to list on type {type(luc_entry)}")
                fixed_luc.append(luc_entry)

    return pd.Series(fixed_luc)


def find_and_replace_luc(
    luc_entry: list[str],
    lookup_table: pd.DataFrame,
    find_column_name: str,
    replace_column_name: str,
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
            return []
    except TypeError:
        return []
    for find_code in lookup_table[find_column_name]:
        if find_code not in luc_entry:
            continue
        luc_entry.remove(find_code)
        replacement_code = lookup_table.loc[
            lookup_table[find_column_name] == find_code, replace_column_name
        ].values[0]
        if isinstance(replacement_code, str):
            luc_entry.append(replacement_code)
        elif isinstance(replacement_code, list):
            luc_entry = luc_entry + replacement_code
        else:
            LOG.warning(
                f"{replace_column_name} contians {type(replacement_code)} that is neither a str or list[str]")
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
