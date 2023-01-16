"""handles the user inputted fixes 

takes user preference, generates file for user to input fixes and
integrates fixes with the existing data
"""
# standard imports
import pathlib
import os
import logging
from typing import Optional
# third party imports
import pandas as pd
import numpy as np

# local imports
from dlit_lu import global_classes, utilities, parser, analyse, inputs

LOG = logging.getLogger(__name__)


def user_input_file_builder(path: pathlib.Path, input_data: global_classes.DLogData) -> None:
    """builds file for user to edit 

    file can then be read and edits integrated into data

    Parameters
    ----------
    path : pathlib.Path
        location to save file
    input_data : global_classes.DLogData
        data to save
    """
    data = utilities.to_dict(input_data)
    utilities.write_to_excel(path, data)


def infill_user_inputs(
    data: dict[str, pd.DataFrame],
    modified_path: pathlib.Path,
    uneditable_columns: Optional[dict[str, list[str]]] = None,
) -> dict[str, pd.DataFrame]:
    """read in user edited file and integrate edits into data

    will fail if user edits column names or indices
    should work if modified path contains a subset of data as
    long as indices are consistent 

    Parameters
    ----------
    data : dict[str, pd.DataFrame]
        data to be infilled
    modified_path : pathlib.Path
        path to user modified data
    uneditable_columns : Optional[dict[str, list[str]]], optional
        columns that the user is forbidden to edit, by default None

    Returns
    -------
    dict[str, pd.DataFrame]
        data with user fixes integrated

    Raises
    ------
    FileNotFoundError
        if inputted file path does not exist
    ValueError
        if columns listed in uneditable have been modified 
    """
    # Read in the Excel file containing the subset of the data
    infilled_data = {}
    if not os.path.exists(modified_path):
        raise FileNotFoundError(f"{modified_path} does not exisit")

    for key, value in data.items():

        data_subset = parser.parse_sheet(modified_path, key, 0)

        # removes "" from luc lists
        data_subset["existing_land_use"] = data_subset[
            "existing_land_use"].apply(lambda x: [
                item for item in x if item != ""] if isinstance(x, list) else x)
        data_subset["proposed_land_use"] = data_subset[
            "proposed_land_use"].apply(lambda x: [
                item for item in x if item != ""] if isinstance(x, list) else x)

        if uneditable_columns is None:
            infilled_data[key] = value.copy()
            # Update the values in the original dataframe with the values in the subset
            infilled_data[key].update(data_subset)
            continue

        # Check whether any values in the specified columns have been changed
        else:
            if not value[uneditable_columns[key]].equals(data_subset[uneditable_columns[key]]):
                infilled_data[key] = value.copy()
                # Update the values in the original dataframe with the values in the subset
                infilled_data[key].update(data_subset)
            else:
                raise ValueError(
                    f"values in {uneditable_columns[key]} within {modified_path} have been modified, these values must remain constant")
    return infilled_data


def implement_user_fixes(
    config: inputs.DLitConfig,
    dlog_data: global_classes.DLogData,
    auxiliary_data: global_classes.AuxiliaryData,
    plot_graphs: bool, 
) -> Optional[global_classes.DLogData]:
    """intergrates user fixes into data

    handles user preferences writes the file for the user to edit,
    reads in user fixes and outputs the data with the user fixes
    integrated. outputs None if user wishes to end program 

    Parameters
    ----------
    config : inputs.DLitConfig
        config object from dlit config file
    dlog_data : global_classes.DLogData
        data to infill
    auxiliary_data : global_classes.AuxiliaryData
        auxiliary data from parser
    plot_graphs : bool
        whether to plot graphs during the data quality assessment

    Returns
    -------
    Optional[global_classes.DLogData]
        infilled data, None if user wishes to end program
    """

    # determines if user wishes to infill using exisiting file
    if os.path.exists(config.user_input_path):
        modification_file_ready = utilities.y_n_user_input(
            f"A file already exists at {config.user_input_path}."
            " Does this contain the fixes you wish to implement? (Y/N)\n")
    else:
        modification_file_ready = False

    # adds filter columns without producing report

    if modification_file_ready:
        user_changes = True
        LOG.info(
            f"Existing file {config.user_input_path} set as user infill input.")
    else:
        
        pre_user_fix_path = config.output_folder / "pre_user_fix"
        pre_user_fix_path.mkdir(exist_ok=True)

        analyse.data_report(
            dlog_data,
            pre_user_fix_path/"initial_data_quality_report.xlsx",
            config.output_folder,
            auxiliary_data,
            plot_graphs,
            True,
        )

        LOG.info(f"Intial data quality report saved as {pre_user_fix_path}")

        # checks if user wishes to infill data
        user_changes = utilities.y_n_user_input("Do you wish to "
                                                "manually fix data before it is infilled? (Y/N)\n")

        if user_changes:
            if os.path.exists(config.user_input_path):
                LOG.info("Creating file for user to edit.")
                # pauses to allow user to save existing file
                input(f"Overwriting {config.user_input_path}, if you wish to store any changes made"
                      ", please make a copy with a different name and press enter, otherwise press"
                      " enter.")

            user_input_file_builder(
                config.user_input_path, dlog_data)

            # allows user to end program to to edit data
            end_program = utilities.y_n_user_input(f"A file has been created at "
                                                   f"{config.user_input_path} for you to manually infill data. Would "
                                                   "you like to end the program and rerun when you have finished? Y "
                                                   "(end the program, modify the data then rerun) or N (data has been"
                                                   " modified)\n")

            if end_program:
                LOG.info("Ending program")
                return None

    if user_changes:
        LOG.info("Implementing user fixes")
        infilled_data = infill_user_inputs(
            dict((k, utilities.to_dict(dlog_data)[k]) for k in (
                ["residential", "employment", "mixed"])),
            config.user_input_path)
        converted_infilled_data = utilities.to_dlog_data(infilled_data, dlog_data.lookup)
        return converted_infilled_data
    return dlog_data


def create_user_changes_audit(
    file_path: pathlib.Path,
    input_modified: global_classes.DLogData,
    input_original: global_classes.DLogData
    ) -> None:
    """create user audit of changes implemented by the user

    produces an excel spreadsheet of the changed rows of 
    input_modified when compared to input original, with the changed
    values highlighted in red.

    Parameters
    ----------
    file_path : pathlib.Path
        location to output audit file
    input_modified : global_classes.DLogData
        user modified data
    input_original : global_classes.DLogData
        original data
    """    
    modified = utilities.to_dict(input_modified)
    original = utilities.to_dict(input_original)
    # Iterate over the dictionaries
    modified_colour_coded = {}

    pd.options.display.float_format = "{:.3f}".format
    for key, value in modified.items():
        modified_colour = value.copy()

        # convert to lists and datetime strings
        modified_colour = modified_colour.applymap(convert_list_to_string)
        original_df = original[key].applymap(convert_list_to_string)

        datetime_columns = modified_colour.select_dtypes(
            include="datetime").columns
        modified_colour[datetime_columns] = modified_colour[datetime_columns].astype(
            "string")
        original_df[datetime_columns] = original_df[datetime_columns].astype(
            "string")

        # seperate out numeric

        modified_number = modified_colour.select_dtypes(include="number")
        original_number = original_df.select_dtypes(include="number")

        # use np.isclose() to solve rounding errors

        differences_number = np.isclose(
            modified_number, original_number, rtol=1e-4, atol=0.001, equal_nan=True)

        differences_number = pd.DataFrame(
            differences_number, columns=modified_number.columns)

        # seperate out other

        modified_other = modified_colour.select_dtypes(exclude="number")
        original_other = original_df.select_dtypes(exclude="number")

        # replace nans with ""

        modified_other = modified_other.fillna("")
        original_other = original_other.fillna("")

        differences_other = modified_other.eq(original_other)

        # mash numerical and other back together
        differences = pd.concat(
            [differences_number, differences_other], axis=1)

        # reorder to match the df
        differences = differences[modified_colour.columns]

        # just get modified values
        modified_colour = modified_colour[~differences.all(axis=1)]
        # apply colour map
        modified_colour_coded[key] = modified_colour.style.apply(
            color_different_red, axis=None, differences=differences[~differences.all(axis=1)])
    utilities.write_to_excel(file_path, modified_colour_coded)


def color_different_red(_:pd.DataFrame, 
    differences:pd.DataFrame)->pd.DataFrame:
    """used to highlight values in red based on a array of bools 

    used when formatting an excel spread sheet

    Parameters
    ----------
    _ : pd.DataFrame
        not used
    differences : pd.DataFrame
        bool array with same dimensions as the dataframe for which this
        is applied 

    Returns
    -------
    
    pd.DataFrame
        array of colours used to apply formatting in an excel spread sheet
    """    
    output = np.where(differences, "", "background-color: red")
    return output


def convert_list_to_string(value: list[str])->str:
    """converts a list of strings to a string

    seperates values with ", "

    Parameters
    ----------
    value : list[str]
        

    Returns
    -------
    str
        list converted to string
    """    
    if isinstance(value, list):
        return ', '.join(value)
    return value
