# standard imports
import logging
# third party imports
import pandas as pd
# local imports
from dlit_lu import data_repair, inputs, parser, utilities, analyse, user_fixes, global_classes

# constants
LOG = logging.getLogger(__name__)
#whether to plot graphs during data quality assessments
PLOT_GRAPHS = False
#whether to write an initial data qualtity report
INITIAL_ASSESSMENT = False



def run(config: inputs.DLitConfig) -> global_classes.DLogData:
    """DLit DLog land use analysis and repair tool

    Parameters
    ----------
    log : utilities.DLitLog
        logging object
    """
    LOG.info("Initilising Analysis and Infill Module")
    config.output_folder.mkdir(exist_ok=True)

    # parse data
    dlog_data = parser.parse_dlog(config)
    auxiliary_data = parser.read_auxiliary_data(
        config.infill.valid_luc_path,
        config.infill.known_invalid_luc_path,
        config.infill.out_of_date_luc_path,
        config.infill.incomplete_luc_path,
        config.infill.regions_shapefiles_path,
    )
    # implement syntax fixes
    initial_assessment_folder = config.output_folder/"00_initial_assessment"
    if INITIAL_ASSESSMENT:
        initial_assessment_folder.mkdir(exist_ok=True)

    data_filter_columns = analyse.data_report(
        dlog_data,
        initial_assessment_folder/"initial_DLOG_data_quality_assessment.xlsx",
        initial_assessment_folder,
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
        "proposed_land_use")
    
    utilities.write_to_csv(config.proposed_luc_split_path, proposed_luc_split)

    existing_luc_split = analyse.luc_ratio(
        utilities.to_dict(syntax_fixed_data),
        auxiliary_data,
        "existing_land_use")

    utilities.write_to_csv(config.existing_luc_split_path, existing_luc_split)

    # user fixes
    #TODO streamline user fixes pipeline
    data_to_fix = syntax_fixed_data

    while True:
        user_fixed_data = user_fixes.implement_user_fixes(
            config, data_to_fix, auxiliary_data, PLOT_GRAPHS)

        if user_fixed_data is None:
            return

        post_user_fix_path = config.output_folder / "02_post_user_fix"
        post_user_fix_path.mkdir(exist_ok=True)
        post_user_fix_report_path =post_user_fix_path/ "post_user_fix_data_report.xlsx"

        user_fixed_data = analyse.data_report(
            user_fixed_data,
            post_user_fix_report_path,
            post_user_fix_path,
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
        user_fixed_data, auxiliary_data, config.output_folder, config)

    #infill build out profile
    res_columns = infilled_fixed_data.residential_data.columns
    emp_columns = infilled_fixed_data.employment_data.columns

    res_unit_year_columns = list(filter(lambda x: x.startswith("res_year_"), res_columns))
    emp_unit_year_columns = list(filter(lambda x: x.startswith("emp_year_"), emp_columns))

    infilled_fixed_data_dict = utilities.to_dict(infilled_fixed_data)

    infilled_fixed_data_dict["residential"] = data_repair.infill_year_units(
        infilled_fixed_data_dict["residential"],
        "res_distribution",
        "units_(dwellings)",
        res_unit_year_columns,
        dlog_data.lookup.years,
    )
    infilled_fixed_data_dict["employment"] = data_repair.infill_year_units(
        infilled_fixed_data_dict["employment"],
        "emp_distribution",
        "units_(floorspace)",
        emp_unit_year_columns,
        dlog_data.lookup.years,
    )
    infilled_fixed_data_dict["mixed"] = data_repair.infill_year_units(
        infilled_fixed_data_dict["mixed"],
        "res_distribution",
        "units_(dwellings)",
        res_unit_year_columns,
        dlog_data.lookup.years,
    )
    infilled_fixed_data_dict["mixed"] = data_repair.infill_year_units(
        infilled_fixed_data_dict["mixed"],
        "emp_distribution",
        "units_(floorspace)",
        emp_unit_year_columns,
        dlog_data.lookup.years,
    )
    infilled_fixed_data = utilities.to_dlog_data(
        infilled_fixed_data_dict,
        infilled_fixed_data.lookup)

    #post fixes data report and write post fix data
    post_fix_output_path = config.output_folder / "03_post_fixes"
    post_fix_output_path.mkdir(exist_ok=True)

    post_fix_data_filter_columns = analyse.data_report(
        infilled_fixed_data, post_fix_output_path / "post_fix_data_report.xlsx",
        post_fix_output_path, auxiliary_data, PLOT_GRAPHS, True)

    utilities.write_to_excel(post_fix_output_path / "post_fix_data.xlsx",
                             utilities.to_dict(post_fix_data_filter_columns))

    LOG.info("Ending Analysis and Infill Module, returning results")
    return global_classes.DLogData(
        None,
        post_fix_data_filter_columns.residential_data,
        post_fix_data_filter_columns.employment_data,
        post_fix_data_filter_columns.mixed_data,
        post_fix_data_filter_columns.lookup,
        proposed_luc_split,
        existing_luc_split,
    )
    