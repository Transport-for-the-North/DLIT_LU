# TODO header
# standard imports
import logging
import pathlib
from typing import Optional
import os

# third party imports
import pandas as pd
import geopandas as gpd


# local imports
from dlit_lu import global_classes
# constants
LOG = logging.getLogger(__name__)


class DLitLog:
    """Manages the DLit tool log file.

    Parameters
    ----------
    file : pathlib.Path, optional
        File to save the log file to, if not given doesn't create
        a file. Can be done later with `DlitLog.add_file_handler`.
    """

    def __init__(self, file: Optional[pathlib.Path] = None):
        self.logger = logging.getLogger(__package__)
        self.logger.setLevel(logging.DEBUG)

        sh = logging.StreamHandler()
        sh.setLevel(logging.INFO)
        self.logger.addHandler(sh)

        logging.captureWarnings(True)
        self.init_message(logging.INFO)

        if file is not None:
            self.add_file_handler(file)

    def init_message(self, level: int) -> None:
        """Log tool initialisation message."""
        init_msg = "DLIT"
        self.logger.log(level, init_msg)
        self.logger.log(level, "-" * len(init_msg))

    def add_file_handler(self, file: pathlib.Path) -> None:
        """Add file handler to logger.

        Parameters
        ----------
        file : pathlib.Path
            Path to log file to create or append to.
        """
        if not file.parent.exists():
            file.parent.mkdir()

        exists = file.exists()

        fh = logging.FileHandler(file)
        fh.setLevel(logging.DEBUG)
        form = logging.Formatter(
            "{asctime} [{name:20.20}] [{levelname:10.10}] {message}", style="{"
        )
        fh.setFormatter(form)
        self.logger.addHandler(fh)

        self.init_message(logging.DEBUG)
        if not exists:
            self.logger.info("Created log file: %s", file)
        else:
            self.logger.info("Appending log messages to: %s", file)

    def __enter__(self):
        """Initialises logger and log file."""
        return self

    def __exit__(self, excepType, excepVal, traceback):
        """Closes log file.

        Note
        ----
        This function should not be called manually but will be
        called upon error / exit of a `with` statement.
        """
        # Write exception to logfile
        if excepType is not None or excepVal is not None or traceback is not None:
            self.logger.critical(
                "Oh no a critical error occurred", exc_info=True)
        else:
            self.logger.info("Program completed without any fatal errors")

        self.logger.info("Closing log file")
        logging.shutdown()


def output_file_checks(output_function):
    """decorator for out put fuctions

    will deal with permission errors and warn user when overwriting file


    Parameters
    ----------
    output_function : function
        output function, the first input must be the output file path
    """
    def wrapper_func(file_path, *args, **kwargs):
        if os.path.exists(file_path):
            LOG.warning(f"overwriting {file_path}")
        while True:
            try:
                output_function(file_path, *args, **kwargs)
                break
            except PermissionError:
                input(f"Please close {file_path}, then press enter. Cheers!")
        # Do something after the function.
    return wrapper_func

@output_file_checks
def write_to_csv(file_path:pathlib.Path, output: pd.DataFrame)-> None:
    """wirtes file to csv 

    used so wrapper with logging and permission error checks can be applied

    Parameters
    ----------
    file_path : pathlib.Path
        path to write csv to
    output : pd.DataFrame
        data to write
    """    
    output.to_csv(file_path)
    
@output_file_checks
def write_to_excel(file_path: pathlib.Path, outputs: dict[str, pd.DataFrame]) -> None:
    """write a dict of pandas DF to a excel spreadsheet

    the keys will become the sheet names 

    Parameters
    ----------
    file_path : pathlib.Path
        file path of the outputted spreadsheet
    outputs : dict[str, pd.DataFrame]
        data to output, str = sheet names, DF = data to write
    """
    with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
        for key, value in outputs.items():
            LOG.info(f"Writing {key}")
            value.to_excel(writer, sheet_name=key)


def to_dict(dlog_data: global_classes.DLogData) -> dict[str, pd.DataFrame]:
    """converts dlog_data to a dictionary

    only contains residential, employment and mixed data

    Parameters
    ----------
    dlog_data : global_classes.DLogData
        dlog data to convert to dict

    Returns
    -------
    dict[str, pd.DataFrame]
        converted data
    """
    return {
        "residential": dlog_data.residential_data,
        "employment": dlog_data.employment_data,
        "mixed": dlog_data.mixed_data,
    }


def to_dlog_data(dlog_data: dict[str, pd.DataFrame], lookup: global_classes.Lookup) -> global_classes.DLogData:
    """converts dictionary to DLOG data type 

    will deal with combined not being present by using None in its place

    Parameters
    ----------
    dlog_data : dict[str, pd.DataFrame]
        _description_
    lookup : global_classes.Lookup
        _description_

    Returns
    -------
    global_classes.DLogData
        _description_
    """    
    try:
        return global_classes.DLogData(
            dlog_data["combined"],
            dlog_data["residential"],
            dlog_data["employment"],
            dlog_data["mixed"],
            lookup,
        )
    except KeyError:
        return global_classes.DLogData(
            None,
            dlog_data["residential"],
            dlog_data["employment"],
            dlog_data["mixed"],
            lookup,
        )


def y_n_user_input(message: str) -> bool:
    """takes user input of y/n

    will loop until valid answer is given

    Parameters
    ----------
    message : str
        message to give to the user 

    Returns
    -------
    bool
        true if y false if n
    """
    while True:
        answer = input(message)
        answer_lower = answer.lower()
        if answer_lower == "y" or answer_lower == "yes":
            return True
        elif answer_lower == "n" or answer_lower == "no":
            return False
        else:
            LOG.warning(
                f"{answer_lower} does not look like \"y\" or \"n\" to me...")


def disagg_mixed(data: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """disaggregates the mixed data set into residential and employment

    assumes the columns in mixed relevent to each sheet will have identical
    column names to the those in the sheet

    Parameters
    ----------
    data : dict[str, pd.DataFrame]
        data set to disagg mixed

    Returns
    -------
    dict[str, pd.DataFrame]
        data set with just residential and employment
    """

    mix = data["mixed"]
    res = data["residential"].reset_index(drop=True)
    emp = data["employment"].reset_index(drop=True)

    mix_res = mix.loc[:, res.columns.unique()].reset_index(drop=True)
    mix_emp = mix.loc[:, emp.columns.unique()].reset_index(drop=True)

    res_new = pd.concat([res, mix_res],  ignore_index=True)
    emp_new = pd.concat([emp, mix_emp], ignore_index=True)

    return {"residential": res_new, "employment": emp_new}

def disagg_dwelling(data: pd.DataFrame, msoa_pop_path:pathlib.Path, msoa_pop_column_names:list[str], unit_columns: list[str])->pd.DataFrame:

    msoa_ratio = calc_msoa_proportion(msoa_pop_path, msoa_pop_column_names)

    msoa_ratio.reset_index("dwelling_type", inplace=True)

    data = data.merge(
        msoa_ratio, how = "left", left_on = "msoa11cd", right_on = "zone_id")

    for column in unit_columns:
        data.loc[:, column] = data[column]*data["dwelling_ratio"]*data["pop_per_dwelling"]

    return data 


def msoa_site_geospatial_lookup(
        data: dict[str, pd.DataFrame],
        msoa: gpd.GeoDataFrame,
        ) -> gpd.GeoDataFrame:
    """spatially joins MSOA shapefile to DLOG sites


    Parameters
    ----------
    data : dict[str, pd.DataFrame]
        data to join to msoa
    msoa : gpd.GeoDataFrame
        msoa data

    Returns
    -------
    gpd.GeoDataFrame
        spatially joined data
    """

    joined = {}
    for key, value in data.items():

        dlog_geom = gpd.GeoDataFrame(value, geometry=gpd.points_from_xy(
            value["easting"], value["northing"]))
        dlog_msoa = gpd.sjoin(dlog_geom, msoa, how="left")
        joined[key] = dlog_msoa
    
    return joined

def calc_msoa_proportion(msoa_pop_path: pathlib.Path, columns: list[str])->pd.DataFrame:
    """

    _extended_summary_

    Parameters
    ----------
    msoa_pop_shape_file : pd.DataFrame
        _description_

    Returns
    -------
    pd.DataFrame
        _description_
    """    
    msoa_pop = pd.read_csv(msoa_pop_path)
    msoa_pop.columns = columns
    msoa_pop.set_index(["zone_id", "dwelling_type"], inplace = True)
    msoa_pop["dwelling_ratio"] = msoa_pop["n_uprn"
        ]/msoa_pop["n_uprn"].groupby(level = "zone_id").sum()

    return msoa_pop


def disagg_land_use_codes(
    data: pd.DataFrame,
    luc_column: str,
    unit_columns: list[str],
    land_use_split: pd.DataFrame
) -> pd.DataFrame:
    """disaggregates land use into seperate rows

    calculates the split of the GFA using total GFA for each land use as a input

    Parameters
    ----------
    data : dict[str, pd.DataFrame]
        data to disaggregate
    luc_column : str
        columns to disaggregate
    unit_columns : dict[str, str]
        unit column to disagregate
    land_use_split : pd.DataFrame
        contains each land use and the total GFA the take up in the Dlog

    Returns
    -------
    pd.DataFrame
        disaggregated land use
    """

    disagg = data.explode(luc_column).reset_index(drop=True)

    site_luc = disagg.loc[:, ["site_reference_id", luc_column]]
    site_luc = site_luc.merge(
        land_use_split,
        how="left",
        left_on=luc_column,
        right_on="land_use_codes",
    )
    ratio_demonitator = site_luc.groupby(["site_reference_id"])["total_floorspace"].sum()
    site_luc.set_index("site_reference_id", inplace=True)
    ratio = site_luc["total_floorspace"]/ratio_demonitator
    ratio.index = disagg.index
    disagg.loc[:, unit_columns].multiply(ratio, axis = 0)
    return disagg
