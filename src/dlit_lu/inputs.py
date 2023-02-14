"""handles reading config file
"""
# standard imports
import json
import pathlib

# third party imports
import pydantic
import caf.toolkit

class DLitConfig(caf.toolkit.BaseConfig):
    """Manages reading / writing the tool's config file.


    Parameters
    ----------
    dlog_input_file: pathlib.Path
        Location of the DLog excel spreadsheet
    combined_sheet_name: str
        name of the combined sheet in the DLog spreadsheet
    residential_sheet_name: str
        name of the residential sheet in the DLog spreadsheet
    employment_sheet_name: str
        name of the employment sheet in the DLog spreadsheet
    mixed_sheet_name: str
        name of the mixed sheet in the DLog spreadsheet
    lookups_sheet_name: str
        name of the lookup sheet in the DLog spreadsheet
    output_folder: pathlib.Path
        file path of the folder where the outputs should be saved
    data_report_file_path: pathlib.Path
        file path where the data report should be saved
    combined_column_names_path: pathlib.Path
        path to a CSV containing the column names for the combined sheet in the DLog
        excel spreadsheet
    residential_column_names_path: pathlib.Path
        path to a CSV containing the column names for the residential sheet in the
        DLog excel spreadsheet
    employment_column_names_path: pathlib.Path
        path to a CSV containing the column names for the employment sheet in the
        DLog excel spreadsheet
    mixed_column_names_path: pathlib.Path
        path to a CSV containing the column names for the mixed sheet in the
        DLog excel spreadsheet
    ignore_columns_path: pathlib.Path
        path to a csv containing the columns in the dlog to ignore when reading
        in
    valid_luc_path: pathlib.Path
        path to a CSV containing valid land use codes
    out_of_date_luc_path: pathlib.Path
        path to a CSV containing out of date land use codes
    incomplete_luc_path: pathlib.Path
        path to a CSV containing incomplete land use codes 
    regions_shapefiles_path: pathlib.Path
        file path for the regions shape file

    Raises
    ------
    ValueError
        file doesn't exisit
    """

    dlog_input_file: pathlib.Path
    combined_sheet_name: str
    residential_sheet_name: str
    employment_sheet_name: str
    mixed_sheet_name: str
    lookups_sheet_name: str
    output_folder: pathlib.Path
    combined_column_names_path: pathlib.Path
    residential_column_names_path: pathlib.Path
    employment_column_names_path: pathlib.Path
    mixed_column_names_path: pathlib.Path
    ignore_columns_path: pathlib.Path
    valid_luc_path: pathlib.Path
    known_invalid_luc_path: pathlib.Path
    out_of_date_luc_path: pathlib.Path
    incomplete_luc_path: pathlib.Path
    regions_shapefiles_path: pathlib.Path
    user_input_path: pathlib.Path

    @pydantic.validator(
        "dlog_input_file",
        "regions_shapefiles_path",
        "combined_column_names_path",
        "residential_column_names_path",
        "employment_column_names_path",
        "mixed_column_names_path",
        "valid_luc_path",
        "out_of_date_luc_path",
        "incomplete_luc_path",
    )
    def _file_exists(  # Validator is class method pylint: disable=no-self-argument
        cls, value: pathlib.Path
    ) -> pathlib.Path:
        if not value.is_file():
            raise ValueError(f"file doesn't exist: {value}")
        return value
