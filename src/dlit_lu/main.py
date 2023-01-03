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
    data_to_fix = dlog_data
    while True:
        fixed_data = implement_user_fixes(config, data_to_fix, auxiliary_data)
        report_path = config.output_folder / "post_user_fix_data_report.xlsx"
        data_filter_columns =  analyse.data_report(
            fixed_data,
            report_path,
            config.output_folder,
            auxiliary_data,
            PLOT_GRAPHS,
            True,
            )
        LOG.info(f"post user fix report outputted to {report_path}")
        continue_analysis = utilities.y_n_user_input("A data report has been outputted"
        ", please review this. Are you happy with the changes made or would you like to"
        " add further changes? (Y/N)\n")
        if continue_analysis:
            break
        else:
            data_to_fix = fixed_data
    fixed_data = data_repair.infill_data(fixed_data, auxiliary_data)
    utilities.write_to_excel(
        config.output_folder / "pre_fix_data.xlsx", utilities.to_dict(data_filter_columns))
    post_fix_output_path = config.output_folder / "post_auto_fix"
    post_fix_output_path.mkdir(exist_ok=True)

    post_fix_data_filter_columns = analyse.data_report(
        fixed_data, post_fix_output_path / "post_fix_data_report.xlsx", post_fix_output_path, auxiliary_data, PLOT_GRAPHS, True)

    # temp outputs for debugging
    utilities.write_to_excel(config.output_folder / "post_fix_data.xlsx",
                             utilities.to_dict(post_fix_data_filter_columns))


def implement_user_fixes(
    config: inputs.DLitConfig,
    dlog_data: global_classes.DLogData,
    auxiliary_data: global_classes.AuxiliaryData,
    )->global_classes.DLogData:

    if os.path.exists(config.user_input_path):
        modification_file_ready = utilities.y_n_user_input(
            f"Do you have a modifications file at {config.user_input_path}"
            " that you would like to integrate to the data? (y/n)\n")
    else:
        modification_file_ready = False
    
    data_filter_columns = analyse.data_report(
                dlog_data,
                config.data_report_file_path,
                config.output_folder,
                auxiliary_data,
                False,
                False,
            )

    fixed_data = data_repair.fix_inavlid_syntax(
        data_filter_columns, auxiliary_data)

    if modification_file_ready:
        user_changes = True
    else:
        data_filter_columns = analyse.data_report(
            data_filter_columns,
            config.data_report_file_path,
            config.output_folder,
            auxiliary_data,
            PLOT_GRAPHS,
            True,
        )

        LOG.info(f"Intial data quality report saved as {config.data_report_file_path}")
        user_changes = utilities.y_n_user_input("Do you wish to"
            "manually fix data before it is infilled? (Y/N)\n")

        if user_changes:
            if os.path.exists(config.user_input_path):
                input(f"Overwriting {config.user_input_path}, if you wish to store any changes made"
                    "make a copy with a different name and press enter, otherwise press enter.")

            user_fixes.user_input_file_builder(
                config.user_input_path, fixed_data)

            end_program = utilities.y_n_user_input(f"A file has been created at "
                    f"{config.user_input_path} for you to manually infill data. Would "
                    "you like to end the program and rerun when you have finished ? Y "
                    "(end the program, modify the data then rerun) or N (data has been"
                    " modified)\n")

            if end_program:
                LOG.info("Ending program")
                return

    if user_changes:
        fixed_data = user_fixes.infill_user_inputs(
            dict((k, utilities.to_dict(fixed_data)[k]) for k in (["residential","employment", "mixed"])),
            config.user_input_path)
        fixed_data = utilities.to_dlog_data(fixed_data, dlog_data.lookup)
    return fixed_data
    
    
        
