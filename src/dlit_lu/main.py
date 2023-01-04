"""DLit DLog land use analysis and repair tool: Version 0.0.0: 09/12/2021

    This tool reads the DLog excel spreadsheet and analyses the data
    for any invalid data, returning the findings in a excel
    spradsheet. Any fixes that can be perfomed automatically will be
    performed. Any issues that require user intervention will be
    outputted to the user, to be infilled manually, read back in and
    intergrated with the DLog 

    Kieran Fishwick: kieran.fishwick@wsp.com 
"""
# standard imports
import pathlib
import logging
import os
from typing import Optional
# third party imports
from tqdm.contrib import logging as tqdm_log
# local imports
from dlit_lu import data_repair, inputs, parser, utilities, analyse, user_fixes, global_classes

# constants
CONFIG_PATH = pathlib.Path("d_lit-config.yml")
LOG = logging.getLogger(__package__)
LOG_FILE = "DLIT.log"
PLOT_GRAPHS = False


def run() -> None:
    with utilities.DLitLog() as dlit_log:
        with tqdm_log.logging_redirect_tqdm([dlit_log.logger]):
            main(dlit_log)


def main(log: utilities.DLitLog) -> None:
    """DLit DLog land use analysis and repair tool

    Parameters
    ----------
    log : utilities.DLitLog
        logging object
    """

    config = inputs.DLitConfig.load_yaml(CONFIG_PATH)

    config.output_folder.mkdir(exist_ok=True)

    log.add_file_handler(config.output_folder / LOG_FILE)

    #parse data
    dlog_data = parser.parse_dlog(
        input_file_path=config.dlog_input_file,
        comb_sheet_name=config.combined_sheet_name,
        res_sheet_name=config.residential_sheet_name,
        emp_sheet_name=config.employment_sheet_name,
        mix_sheet_name=config.mixed_sheet_name,
        lookup_sheet_name=config.lookups_sheet_name,
        comb_column_names_path=config.combined_column_names_path,
        res_column_names_path=config.residential_column_names_path,
        emp_column_names_path=config.employment_column_names_path,
        mix_column_names_path=config.mixed_column_names_path,
    )
    auxiliary_data = parser.read_auxiliary_data(
        config.valid_luc_path,
        config.known_invalid_luc_path,
        config.out_of_date_luc_path,
        config.incomplete_luc_path,
        config.regions_shapefiles_path,
    )
    #implement syntax and automatic fixes
    data_to_fix = dlog_data
    while True:
        fixed_data = user_fixes.implement_user_fixes(config, data_to_fix, auxiliary_data, PLOT_GRAPHS)
        if fixed_data is None:
            return
        report_path = config.output_folder / "post_user_fix_data_report.xlsx"
        analyse.data_report(
            fixed_data,
            report_path,
            config.output_folder,
            auxiliary_data,
            PLOT_GRAPHS,
            True,
            )
        LOG.info(f"post user fix report outputted to {report_path}")
        continue_analysis = utilities.y_n_user_input("A data report has been outputted"
        ", please review this. Woudl you like to continue (or add further changes)? (Y/N)\n")
        if continue_analysis:
            break
        else:
            data_to_fix = fixed_data
    #infill invalid data 
    fixed_data = data_repair.infill_data(fixed_data, auxiliary_data)

    post_fix_output_path = config.output_folder / "post_auto_fix"
    post_fix_output_path.mkdir(exist_ok=True)

    post_fix_data_filter_columns = analyse.data_report(
        fixed_data, post_fix_output_path / "post_fix_data_report.xlsx", post_fix_output_path, auxiliary_data, PLOT_GRAPHS, True)

    # temp outputs for debugging
    utilities.write_to_excel(config.output_folder / "post_fix_data.xlsx",
                             utilities.to_dict(post_fix_data_filter_columns))
    #dissag_fixed_data = utilities.disagg_mixed(utilities.to_dict(post_fix_data_filter_columns))
    print("this is temporary")

    
    
        
