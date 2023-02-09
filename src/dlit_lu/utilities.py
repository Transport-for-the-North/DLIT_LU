# TODO header
# standard imports
import logging
import pathlib
from typing import Optional
import os

# third party imports
import pandas as pd
import openpyxl

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

def to_dlog_data(dlog_data: dict[str, pd.DataFrame], lookup: global_classes.Lookup)->global_classes.DLogData:
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
def y_n_user_input(message: str)->bool:
    
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
        if answer_lower == "y" or answer_lower =="yes":
            return True
        elif answer_lower == "n" or answer_lower =="no":
            return False
        else:
            LOG.warning(f"{answer_lower} does not look like \"y\" or \"n\" to me...")

def disagg_mixed(data: dict[str, pd.DataFrame])->dict[str, pd.DataFrame]:
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
    
    mix_res = mix.loc[:,res.columns.unique()].reset_index(drop=True)
    mix_emp = mix.loc[:, emp.columns.unique()].reset_index(drop=True)


    res_new = pd.concat([res, mix_res],  ignore_index= True)
    emp_new = pd.concat([emp, mix_emp], ignore_index= True)

    return {"residential":res_new, "employment":emp_new}