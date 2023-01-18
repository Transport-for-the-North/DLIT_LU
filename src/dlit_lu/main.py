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
# third party imports
from tqdm.contrib import logging as tqdm_log
# local imports
from dlit_lu import data_repair, inputs, parser, utilities, analyse, user_fixes

# constants
CONFIG_PATH = pathlib.Path("d_lit-config.yml")
LOG = logging.getLogger(__package__)
LOG_FILE = "DLIT.log"

#whether to plot graphs during data quality assessments
PLOT_GRAPHS = False
#whether to write an initial data qualtity report
INITIAL_ASSESSMENT = False


def run() -> None:
    """initilises Logging and calls main
    """
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
    #load in config file
    config = inputs.DLitConfig.load_yaml(CONFIG_PATH)

    config.output_folder.mkdir(exist_ok=True)

    #set log file
    log.add_file_handler(config.output_folder / LOG_FILE)

    # parse data
    dlog_data = parser.parse_dlog(config)
    auxiliary_data = parser.read_auxiliary_data(
        config.valid_luc_path,
        config.known_invalid_luc_path,
        config.out_of_date_luc_path,
        config.incomplete_luc_path,
        config.regions_shapefiles_path,
    )
    # implement syntax fixes
    initial_assessment_folder = config.output_folder/"initial_assessment"
    if INITIAL_ASSESSMENT:
        initial_assessment_folder.mkdir(exist_ok=True)

    data_filter_columns = analyse.data_report(
        dlog_data,
        initial_assessment_folder/"DLOG_data_quality_assessment.xlsx",
        config.output_folder,
        auxiliary_data,
        PLOT_GRAPHS,
        INITIAL_ASSESSMENT,
    )

    syntax_fixed_data = data_repair.fix_inavlid_syntax(
        data_filter_columns, auxiliary_data)

    syntax_fixed_data = analyse.data_report(
        syntax_fixed_data,
        config.output_folder/"_",
        config.output_folder,
        auxiliary_data,
        False,
        False,
    )
    proposed_luc_split = analyse.luc_ratio(
        utilities.to_dict(syntax_fixed_data),
        auxiliary_data,
        ["proposed_land_use"])

    # user fixes
    #TODO streamline user fixes pipeline
    data_to_fix = syntax_fixed_data

    while True:
        user_fixed_data = user_fixes.implement_user_fixes(
            config, data_to_fix, auxiliary_data, PLOT_GRAPHS)

        if user_fixed_data is None:
            return

        post_user_fix_path = config.output_folder / "post_user_fix"
        post_user_fix_path.mkdir(exist_ok=True)
        post_user_fix_report_path =post_user_fix_path/ "data_report.xlsx"

        user_fixed_data = analyse.data_report(
            user_fixed_data,
            post_user_fix_report_path,
            config.output_folder,
            auxiliary_data,
            PLOT_GRAPHS,
            True,
        )
        LOG.info(f"post user fix report outputted to {post_user_fix_report_path}")

        continue_analysis = utilities.y_n_user_input("A data report has been outputted"
            ", please review this. Would you like to continue (or add further changes)? (Y/N)\n")

        if continue_analysis:
            user_fixes.create_user_changes_audit(
                config.output_folder/"user_changes_audit.xlsx", user_fixed_data, syntax_fixed_data)
            break
        else:
            #if user wishes to make more ammendments loop is restarted
            data_to_fix = user_fixed_data

    # infill invalid data
    infilled_fixed_data = data_repair.infill_data(
        user_fixed_data, auxiliary_data)

    #post fixes data report and write post fix data
    post_fix_output_path = config.output_folder / "post_fixes"
    post_fix_output_path.mkdir(exist_ok=True)

    post_fix_data_filter_columns = analyse.data_report(
        infilled_fixed_data, post_fix_output_path / "post_fix_data_report.xlsx",
        post_fix_output_path, auxiliary_data, PLOT_GRAPHS, True)

    utilities.write_to_excel(post_fix_output_path / "post_fix_data.xlsx",
                             utilities.to_dict(post_fix_data_filter_columns))

    # TODO finalise disagg mixed pipeline
    
    disagg_fixed_data = utilities.disagg_mixed(
        utilities.to_dict(post_fix_data_filter_columns))

    disagg_fixed_data = utilities.disagg_land_use(disagg_fixed_data, "proposed_land_use",
        {"residential": "units_(dwellings)", "employment":"units_(floorspace)"},
        proposed_luc_split)
    print("sandwich")
    
