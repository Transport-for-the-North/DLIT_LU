"""Analyses a DLog data object for invalid/missing/contradictory data
when data report is called, returns inputted object with additional/
ammended filter columns, which contain bool values corresponding to
the type of invalid data found in each row. data_report also has the
out to write a data report and plot graphs displaying the findings.


Raises
------
ValueError
    geo_plotter: column: str must be passed if a chloropleth has been given
ValueError
    geo_plotter: column: str must be passed if a chloropleth has been given
ValueError
    geo_plotter: colour must be passed if points has been given
ValueError
    add_filter_column: no column found
ValueError
    analyse_invalid_luc: no column found
ValueError
    find_invalid_land_use_codes: no column found
"""
# standard imports
import logging
from typing import Optional
import pathlib
import os


# third party imports
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import folium

# local imports
from dlit_lu import global_classes, utilities

# constants
LOG = logging.getLogger(__name__)

# sets the tolerance for the geopandas simplify function
SIMPLIFY_TOLERANCE = 100

# makes CRS conversions more robust but less accurate
os.environ["PROJ_NETWORK"] = "OFF"


def data_report(
    dlog_data: global_classes.DLogData,
    report_file_path: pathlib.Path,
    output_folder_path: pathlib.Path,
    auxiliary_data: global_classes.AuxiliaryData,
    plot_maps: bool,
    write_report: bool,
) -> global_classes.DLogData:
    """Produces a data report to the provided file path

    performs the anlysis and calcuations. outputs the results into an
    excel spread sheet and produces visualations.

    Parameters
    ----------
    dlog_data : global_classes.DLogData
        the data to be analysed
    report_file_path : pathlib.Path
        file path of the output excel file (will overwrite)
    output_folder_path : pathlib.Path
        location of the output folder
    auxiliary_data : global_classes.AuxiliaryData
        data required for the analysis that is not in DLog
    plot_maps: bool
        whether the function should create visual outputs,
        this takes some time
    write_report: bool
        whether the function create visual outputs, this takes some time
    """
    res_data = dlog_data.residential_data
    emp_data = dlog_data.employment_data
    mix_data = dlog_data.mixed_data
    LOG.info("Performing data quality check")
    # calculates total entries
    analysis_summary = [
        {
            "Residential": len(res_data),
            "Employment": len(emp_data),
            "Mixed": len(mix_data),
        }
    ]

    analysis_summary_index_labels = ["total_entries"]
    analysis_summary_notes = [
        "The total number of entries read from the DLOG data"
    ]
    # -------------------find missing site reference ids---------------------------------
    # add missing ref ids filter - cant use parse_analysis_results since ResultsReport
    # hasnt been initiated
    data = {
        "residential": res_data,
        "employment": emp_data,
        "mixed": mix_data,
    }
    missing_ids = find_multiple_missing_values(
        data,
        {
            "residential": ["site_reference_id"],
            "employment": ["site_reference_id"],
            "mixed": ["site_reference_id"],
        },
        {"residential": [], "employment": [], "mixed": []},
    )
    missing_site_ref_filter_name = "missing_site_ref"
    invalid_data_filter = add_multiple_filter_columns(
        data, missing_ids, missing_site_ref_filter_name,
    )
    analysis_summary.append(
        {
            "Residential": len(missing_ids["residential"]),
            "Employment": len(missing_ids["employment"]),
            "Mixed": len(missing_ids["mixed"]),
        }
    )
    analysis_summary_index_labels.append("missing_site_reference_id")
    analysis_summary_notes.append(
        "Entries where a site reference ID has not been provided"
    )
    results_report = global_classes.ResultsReport(
        invalid_data_filter,
        analysis_summary,
        analysis_summary_index_labels,
        analysis_summary_notes,
        [missing_site_ref_filter_name],
    )
    # --------------------find invalid land use codes---------------------------------
    results_report = invalid_land_use_report(
        data, auxiliary_data, results_report,
    )
    # -------------- find missing years------------------------

    missing_years_columns = [
        "start_year_id",
        "end_year_id",
    ]

    missing_years = find_multiple_missing_values(
        data,
        {
            "residential": missing_years_columns,
            "employment": missing_years_columns,
            "mixed": missing_years_columns,
        },
        {
            "residential": ["unknown", 14],
            "employment": ["unknown", 14],
            "mixed": ["unknown", 14],
        },
    )
    results_report = parse_analysis_results(
        missing_years,
        results_report,
        "missing_years",
        "Entries with start and end years not defined",
    )
    # find missing years without a tag-certainty specified
    missing_years_no_webtag = find_multiple_missing_values(
        missing_years,
        {
            "residential": ["web_tag_certainty_id"],
            "employment": ["web_tag_certainty_id"],
            "mixed": ["web_tag_certainty_id"],
        },
        {
            "residential": [0, "-"],
            "employment": [0, "-"],
            "mixed": [0, "-"],
        },
    )
    results_report = parse_analysis_results(
        missing_years_no_webtag,
        results_report,
        "missing_years_no_webtag",
        "Entries with missing years and no WEBTAG certainity status, infilling not possible",
    )
    missing_years_with_webtag = {}
    for key, value in missing_years.items():
        missing_years_with_webtag[key] = value.drop(
            index=missing_years_no_webtag[key].index
        )

    results_report = parse_analysis_results(
        missing_years_with_webtag,
        results_report,
        "missing_years_with_webtag",
        "Entries with missing years that do have WEBTAG certainity status, infilling possible",
    )
    # ---------------------find missing areas--------------------------------------

    missing_area = find_multiple_missing_values(
        data,
        {
            "residential": ["total_site_area_size_hectares"],
            "employment": ["site_area_ha"],
            "mixed": ["total_area_ha"],
        },
        {
            "residential": [0, "-"],
            "employment": [0, "-"],
            "mixed": [0, "-"],
        },
    )
    results_report = parse_analysis_results(
        missing_area,
        results_report,
        "missing_area",
        "NON FATAL: Entries where site area have not been provided."
        " Only becomes critical error if dwellings/floorspace have not been provided.",
    )

    # ------------------- find missing areas_dwellings------------------------------
    # missing GFA or dwellings
    all_missing_d_a = find_multiple_missing_values(
        data,
        {
            "residential": ["total_units",],
            "employment": ["total_area_sqm"],
            "mixed": ["floorspace_sqm", "dwellings"],
        },
        {
            "residential": ["-", 0],
            "employment": ["-", 0],
            "mixed": ["-", 0],
        },
    )
    # missing GFA or dwellings with no site area - no assumption can be made
    missing_d_a_no_sa = find_multiple_missing_values(
        all_missing_d_a,
        {
            "residential": ["total_site_area_size_hectares",],
            "employment": ["site_area_ha"],
            "mixed": ["total_area_ha"],
        },
        {
            "residential": ["-", 0],
            "employment": ["-", 0],
            "mixed": ["-", 0],
        },
    )
    # missing GFA or dwellings with site area provided - assumptions can be made
    missing_d_a_with_sa = {}
    missing_d_a_with_sa["residential"] = all_missing_d_a[
        "residential"
    ].drop(index=missing_d_a_no_sa["residential"].index)
    missing_d_a_with_sa["employment"] = all_missing_d_a[
        "employment"
    ].drop(index=missing_d_a_no_sa["employment"].index)
    missing_d_a_with_sa["mixed"] = all_missing_d_a["mixed"].drop(
        index=missing_d_a_no_sa["mixed"].index
    )

    results_report = parse_analysis_results(
        missing_d_a_no_sa,
        results_report,
        "missing_gfa_or_dwellings_no_site_area",
        "Entries where areas (employment/mixed) or dwellings (residential/mixed) are"
        " not provided or are 0 where no site area is provided. User intervention required.",
    )
    results_report = parse_analysis_results(
        missing_d_a_with_sa,
        results_report,
        "missing_gfa_or_dwellings_with_site_area",
        "Entries where areas (employment/mixed) or dwellings (residential/mixed) are not provided"
        " or are 0 where site area is provided. Assumptions can be made",
    )

    # --------------------------find missing coords-----------------------------

    missing_coords_columns = ["easting", "northing"]
    missing_coords = find_multiple_missing_values(
        data,
        {
            "residential": missing_coords_columns,
            "employment": missing_coords_columns,
            "mixed": missing_coords_columns,
        },
        {"residential": [], "employment": [], "mixed": []},
    )
    results_report = parse_analysis_results(
        missing_coords,
        results_report,
        "missing_coords",
        "Entries where coordinates have not been provided (easting/northing)",
    )
    # --------------------------find missing distribution---------------------------
    missing_dist = find_multiple_missing_values(
        data,
        {
            "residential": ["res_distribution"],
            "employment": ["emp_distribution"],
            "mixed": ["res_distribution", "emp_distribution"],
        },
        {"residential": [0], "employment": [0], "mixed": [0]},
    )
    results_report = parse_analysis_results(
        missing_dist,
        results_report,
        "missing_dist",
        "Entries where a distribution (build up profile) has not been provided",
    )
    # --------------------------find inactive entries------------------------
    inactive_entries = find_inactivate_entries(data)

    results_report = parse_analysis_results(
        inactive_entries,
        results_report,
        "inactive_entries",
        'NON FATAL: entries where active has not been specified as "t"',
    )
    # ---------------------------contra constr, planning, tag------------------
    contra_constr_planning_tag = find_contradictory_tag_const_plan(data)

    results_report = parse_analysis_results(
        contra_constr_planning_tag,
        results_report,
        "contradictory_construction_planning_tag",
        "NON FATAL: entries with contradictor construction_status,"
        " planning_status and or web_tag_certainty",
    )
    # used to calculate the number of entries with user intervention required
    user_intervention_required = [
        "missing_coords",
    ]

    non_fatal_columns = [
        "inactive_entries",
        "missing_area",
        "contradictory_construction_planning_tag",
    ]

    autofix_columns = [
        "missing_site_ref",
        "all_invalid_existing_land_use_code",
        "all_invalid_proposed_land_use_code",
        "missing_gfa_or_dwellings_with_site_area",
        "missing_dist",
        "missing_years_with_webtag",
        "missing_gfa_or_dwellings_no_site_area",
        "missing_years_no_webtag",
    ]

    # filter for only entries with issues
    classified_data = classify_data(
        results_report,
        autofix_columns,
        user_intervention_required,
        non_fatal_columns,
    )

    # process data summary
    analysis_summary = results_report.analysis_summary
    analysis_summary_index_labels = (
        results_report.analysis_summary_index_labels
    )
    analysis_summary_notes = results_report.analysis_summary_notes

    analysis_summary.append(
        {
            "Residential": len(
                classified_data["non_fatal"]["residential"]
            ),
            "Employment": len(
                classified_data["non_fatal"]["employment"]
            ),
            "Mixed": len(classified_data["non_fatal"]["mixed"]),
        }
    )
    analysis_summary_index_labels.append("total_contradictory_entries")
    analysis_summary_notes.append(
        "total number of entries with non-fatal values,"
        " these entries do not require modification to be included"
    )

    analysis_summary.append(
        {
            "Residential": len(
                classified_data["auto_fixes"]["residential"]
            ),
            "Employment": len(
                classified_data["auto_fixes"]["employment"]
            ),
            "Mixed": len(classified_data["auto_fixes"]["mixed"]),
        }
    )
    analysis_summary_index_labels.append(
        "total_fixable_invalid_entries"
    )
    analysis_summary_notes.append(
        "total number of entries with invalid values that can"
        " be fixed automatically. Either syntax errors or recoverable from assumptions"
    )

    analysis_summary.append(
        {
            "Residential": len(
                classified_data["intervention_required"]["residential"]
            ),
            "Employment": len(
                classified_data["intervention_required"]["employment"]
            ),
            "Mixed": len(
                classified_data["intervention_required"]["mixed"]
            ),
        }
    )
    analysis_summary_index_labels.append(
        "total_invalid_entries_user_input_required"
    )
    analysis_summary_notes.append(
        "total number of entries with invalid values that require user"
        " intervention, E.G. missing critical values"
    )

    analysis_summary.append(
        {
            "Residential": len(classified_data["valid"]["residential"]),
            "Employment": len(classified_data["valid"]["employment"]),
            "Mixed": len(classified_data["valid"]["mixed"]),
        }
    )
    analysis_summary_index_labels.append("total_valid_entries")
    analysis_summary_notes.append(
        "total number of complete and valid entries"
    )

    summary = pd.DataFrame(
        analysis_summary, index=analysis_summary_index_labels
    )
    summary["Total"] = summary.sum(axis=1)
    summary["Notes"] = analysis_summary_notes
    # plot results
    if plot_maps:
        split_data = {
            "R invalid": gpd.GeoDataFrame(
                classified_data["invalid"]["residential"],
                geometry=gpd.points_from_xy(
                    classified_data["invalid"]["residential"][
                        "easting"
                    ],
                    classified_data["invalid"]["residential"][
                        "northing"
                    ],
                    crs=27700,
                ),
            ),
            "R No invalid": gpd.GeoDataFrame(
                classified_data["valid"]["residential"],
                geometry=gpd.points_from_xy(
                    classified_data["valid"]["residential"]["easting"],
                    classified_data["valid"]["residential"]["northing"],
                    crs=27700,
                ),
            ),
            "E invalid": gpd.GeoDataFrame(
                classified_data["invalid"]["employment"],
                geometry=gpd.points_from_xy(
                    classified_data["invalid"]["employment"]["easting"],
                    classified_data["invalid"]["employment"][
                        "northing"
                    ],
                    crs=27700,
                ),
            ),
            "E No invalid": gpd.GeoDataFrame(
                classified_data["valid"]["employment"],
                geometry=gpd.points_from_xy(
                    classified_data["valid"]["employment"]["easting"],
                    classified_data["valid"]["employment"]["northing"],
                    crs=27700,
                ),
            ),
            "M invalid": gpd.GeoDataFrame(
                classified_data["invalid"]["mixed"],
                geometry=gpd.points_from_xy(
                    classified_data["invalid"]["mixed"]["easting"],
                    classified_data["invalid"]["mixed"]["northing"],
                    crs=27700,
                ),
            ),
            "M No invalid": gpd.GeoDataFrame(
                classified_data["valid"]["mixed"],
                geometry=gpd.points_from_xy(
                    classified_data["valid"]["mixed"]["easting"],
                    classified_data["valid"]["mixed"]["northing"],
                    crs=27700,
                ),
            ),
        }
        # visualisation parameters
        plot_colours = {
            "R invalid": "red",
            "R No invalid": "black",
            "E invalid": "red",
            "E No invalid": "black",
            "M invalid": "red",
            "M No invalid": "black",
        }
        plot_markers = {
            "R invalid": "*",
            "R No invalid": "*",
            "E invalid": "^",
            "E No invalid": "^",
            "M invalid": "o",
            "M No invalid": "o",
        }
        plot_limits = {"x": [280000, 550000], "y": [325000, 670000]}
        LOG.info("Plotting results")
        plot_data(
            split_data,
            plot_colours,
            plot_markers,
            auxiliary_data.regions.to_crs("27700"),
            plot_limits,
            False,
            output_folder_path,
        )
    # output data report
    if write_report:
        utilities.write_to_excel(
            report_file_path,
            {
                "report_summary": summary,
                "Residential": classified_data["invalid"][
                    "residential"
                ],
                "Employment": classified_data["invalid"]["employment"],
                "Mixed": classified_data["invalid"]["mixed"],
            },
        )
    return global_classes.DLogData(
        None,
        results_report.data_filter["residential"],
        results_report.data_filter["employment"],
        results_report.data_filter["mixed"],
        dlog_data.lookup,
    )

def luc_ratio(
    data: dict[str, pd.DataFrame],
    auxiliary_data: global_classes.AuxiliaryData,
    column: str = "proposed_land_use", 
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
        for key, value in data.items():
            #do not use residential since they do not contain floorspace
            if key == "residential":
                continue

            code_in_entry = find_lucs(value, column, code)

            if code_in_entry is None:
                continue

            have_floorspace = pd.DataFrame([code_in_entry[
                "missing_gfa_or_dwellings_no_site_area"].reset_index(drop=True),
                    code_in_entry["missing_gfa_or_dwellings_with_site_area"].reset_index(drop=True)]).transpose()
            have_floorspace.index = code_in_entry.index
            have_floorspace = code_in_entry[~have_floorspace.any(axis=1)]

            have_floorspace.loc[:, "units_(floorspace)"] = have_floorspace[
                "units_(floorspace)"] / have_floorspace[column].apply(lambda x: len(x))
                
            total_floorspace = have_floorspace["units_(floorspace)"].sum()

            land_use_codes_count.loc[land_use_codes_count["land_use_codes"] == code, "count"
                ] = land_use_codes_count.loc[land_use_codes_count["land_use_codes"
                    ] == code, "count"] + len(have_floorspace)

            land_use_codes_count.loc[land_use_codes_count["land_use_codes"
                ]== code, "total_floorspace"] = land_use_codes_count.loc[land_use_codes_count[
                    "land_use_codes"] == code, "total_floorspace"] + total_floorspace

    land_use_codes_count["average_floorspace"] = land_use_codes_count["total_floorspace"] / \
        land_use_codes_count["count"]
    return land_use_codes_count

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


def classify_data(
    results_report: global_classes.ResultsReport,
    auto_fix_columns: list[str],
    intervention_required_columns: list[str],
    non_fatal_columns: list[str] = [],
) -> dict[str, dict[str, pd.DataFrame]]:
    """seperates data into invalid, valid, fixable, intervention required and contradictory

    determines the data status of every entry in the data report
    by the values of the filter columns

    Parameters
    ----------
    results_report : global_classes.ResultsReport
        data to be classified, must have bool columns corresponding
        to errors
    intervention_required_columns : list[str]
        a list of filter columns in the results report that cannot be
        fixed automatically
    non_fatal_columns : list[str]
        a list of filter columns that have contradictory/non-fatal information

    Returns
    -------
    dict[str, dict[str, pd.DataFrame]]
        the classified data (keys =  invalid, valid,
        intervention_required, auto_fixes)
    """
    filter_names = results_report.filter_columns
    invalid_output = {}
    intervention_required = {}
    auto_fixes = {}
    valid_output = {}
    non_fatal = {}
    for key, value in results_report.data_filter.items():
        invalid_output[key] = value[
            value.loc[:, filter_names].any(axis=1)
        ]
        valid_output[key] = value[
            ~value.loc[:, filter_names].any(axis=1)
        ]
        intervention_required[key] = value[
            value.loc[:, intervention_required_columns].any(axis=1)
        ]
        non_fatal[key] = value[
            value.loc[:, non_fatal_columns].any(axis=1)
        ]
        auto_fixes[key] = value[
            value.loc[:, auto_fix_columns].any(axis=1)
        ]

    return {
        "invalid": invalid_output,
        "valid": valid_output,
        "intervention_required": intervention_required,
        "non_fatal": non_fatal,
        "auto_fixes": auto_fixes,
    }


def find_missing_lpas(
    data: dict[str, pd.DataFrame], lpa_look_up: pd.DataFrame
) -> dict[str, pd.DataFrame]:
    """finds LPAs that do not appear in the DLOG

    determines whether an LPA in lpa_look_up has any entries

    Parameters
    ----------
    data : dict[str, pd.DataFrame]
        the dlog data to determine the missing lpas
    lpa_look_up : pd.DataFrame
        lpas look up table (id and name)

    Returns
    -------
    dict[str, pd.DataFrame]
        the missing LPAs, identical keys to inputted data
    """
    missing_lpas = {}
    for key, value in data.items():
        lpas_with_entries = (
            value["local_authority_id"].unique().tolist()
        )
        missing_lpas[key] = lpa_look_up.drop(index=lpas_with_entries)
    return missing_lpas


def contradictory_webtag_planning_status(
    data: dict[str, pd.DataFrame]
) -> dict[str, pd.DataFrame]:
    """determines whether enrty has contradictory web tag and planning status values

    returns values which have are not permissioned/not specified
    planning status ID = 1,0 yet have a near certain webtag
    status (ID = 1)

    Parameters
    ----------
    data : dict[str, pd.DataFrame]
        data top be analysed

    Returns
    -------
    dict[str, pd.DataFrame]
        contradictory values
    """
    contradictory = {}
    for key, value in data.items():
        # checks if planning permission is not specified or not permission,
        #  yet record is near certain
        planning_not_permissioned = value["planning_status_id"] == 1
        planning_not_specified = value["planning_status_id"] == 0
        planning_check = (
            pd.DataFrame(
                [planning_not_permissioned, planning_not_specified]
            )
            .transpose()
            .any(axis=1)
        )
        webtag_check = value["web_tag_certainty_id"] == 1
        contradictory[key] = value[
            pd.DataFrame([planning_check, webtag_check])
            .transpose()
            .all(axis=1)
        ]
    return contradictory


def invalid_land_use_report(
    data: dict[str, pd.DataFrame],
    auxiliary_data: global_classes.AuxiliaryData,
    input_results_report: global_classes.ResultsReport,
) -> global_classes.ResultsReport:
    """determines issues with land use codes

    find all invalid codes, incorrect format, out of date codes
    and incomplete codes for existing and proposed land use.

    adds new filter columns to data report for the findings
    Parameters
    ----------
    data : dict[str, pd.DataFrame]
        data to be analysed
    auxiliary_data : global_classes.AuxiliaryData
        axiliary data object
    input_results_report : global_classes.ResultsReport
        data report to be modified.

    Returns
    -------
    global_classes.ResultsReport
        data report with new filter columns added
    """
    # find incorrectly formatted land use codes
    res_data = data["residential"]
    emp_data = data["employment"]
    mix_data = data["mixed"]
    # ------------------------luc summary-----------------------
    # existing
    # residential - do not expect any land use to be given
    res_invalid_e_land_use = find_invalid_land_use_codes(
        res_data,
        auxiliary_data.allowed_codes["land_use_codes"],
        ["existing_land_use"],
    )

    emp_invalid_e_land_use = find_invalid_land_use_codes(
        emp_data,
        auxiliary_data.allowed_codes["land_use_codes"],
        ["existing_land_use"],
    )

    mix_invalid_e_land_use = find_invalid_land_use_codes(
        mix_data,
        auxiliary_data.allowed_codes["land_use_codes"],
        ["existing_land_use"],
    )
    # proposed
    emp_invalid_p_land_use = find_invalid_land_use_codes(
        emp_data,
        auxiliary_data.allowed_codes["land_use_codes"],
        ["proposed_land_use"],
    )

    mix_invalid_p_land_use = find_invalid_land_use_codes(
        mix_data,
        auxiliary_data.allowed_codes["land_use_codes"],
        ["proposed_land_use"],
    )

    # parse invalid land use code results
    results_report = parse_analysis_results(
        {
            "residential": res_invalid_e_land_use,
            "employment": emp_invalid_e_land_use,
            "mixed": mix_invalid_e_land_use,
        },
        input_results_report,
        "all_invalid_existing_land_use_code",
        "Entries which contain existing land use code(s) that is not"
        " in the land use code table in lookup",
    )
    results_report = parse_analysis_results(
        {
            "residential": pd.DataFrame(
                columns=res_data.columns
            ),  # empty df as place holder
            "employment": emp_invalid_p_land_use,
            "mixed": mix_invalid_p_land_use,
        },
        results_report,
        "all_invalid_proposed_land_use_code",
        "Entries which contain proposed land use code(s) that is not in the land"
        " use code table in lookup (residential proposed land use code ignored)",
    )
    # -----------------perform indepth luc analysis-----------------------
    eluc_analysis = analyse_invalid_luc(
        {
            "residential": res_invalid_e_land_use,
            "employment": emp_invalid_e_land_use,
            "mixed": mix_invalid_e_land_use,
        },
        {
            "residential": ["existing_land_use"],
            "employment": ["existing_land_use"],
            "mixed": ["existing_land_use"],
        },
        auxiliary_data.allowed_codes["land_use_codes"],
        auxiliary_data.out_of_date_luc[
            "out_of_date_land_use_codes"
        ].str.lower(),
        auxiliary_data.incomplete_luc[
            "incomplete_land_use_codes"
        ].str.lower(),
    )
    pluc_analysis = analyse_invalid_luc(
        {
            "employment": emp_invalid_p_land_use,
            "mixed": mix_invalid_p_land_use,
        },
        {
            "employment": ["proposed_land_use"],
            "mixed": ["proposed_land_use"],
        },
        auxiliary_data.allowed_codes["land_use_codes"],
        auxiliary_data.out_of_date_luc[
            "out_of_date_land_use_codes"
        ].str.lower(),
        auxiliary_data.incomplete_luc[
            "incomplete_land_use_codes"
        ].str.lower(),
    )
    # create empty df's since these columns exist in the data report
    # TODO alter code so this is not necessary
    pluc_analysis["wrong_format"]["residential"] = pd.DataFrame(
        columns=res_data.columns
    )
    pluc_analysis["incomplete"]["residential"] = pd.DataFrame(
        columns=res_data.columns
    )
    pluc_analysis["out_of_date"]["residential"] = pd.DataFrame(
        columns=res_data.columns
    )
    pluc_analysis["other_issues"]["residential"] = pd.DataFrame(
        columns=res_data.columns
    )
    # TODO put this in for loop
    # ------------------------parse eluc-------------------
    results_report = parse_luc_analysis(
        eluc_analysis,
        results_report,
        "wrong_format",
        "wrong_format_existing_land_use_code",
        "Entries which contain existing land use code with incorrect syntax e.g."
        " Egi instead of E(g)(i) *codes are case insensitive*",
    )

    results_report = parse_luc_analysis(
        eluc_analysis,
        results_report,
        "out_of_date",
        "out_of_date_existing_land_use_code",
        "Entries which contain existing land use code which have been revoked/replaced, eg. B1(a)",
    )

    results_report = parse_luc_analysis(
        eluc_analysis,
        results_report,
        "incomplete",
        "incomplete_existing_land_use_code",
        "Entries which contain existing land use code which are incomplete"
        " e.g. E(g) instead of E(g)(i)",
    )

    results_report = parse_luc_analysis(
        eluc_analysis,
        results_report,
        "other_issues",
        "other_issues_existing_land_use_code",
        "Entries which have a invalid existing land use code not defined above,"
        " These will require infilling",
    )
    # ------------------------------parse pluc-----------------------
    results_report = parse_luc_analysis(
        pluc_analysis,
        results_report,
        "wrong_format",
        "wrong_format_proposed_land_use_code",
        "Entries which contain proposed land use code with incorrect syntax e.g."
        " Egi instead of E(g)(i) *codes are case insensitive*",
    )

    results_report = parse_luc_analysis(
        pluc_analysis,
        results_report,
        "out_of_date",
        "out_of_date_proposed_land_use_code",
        "Entries which contain proposed land use code which have been revoked/replaced, eg. B1(a)",
    )
    results_report = parse_luc_analysis(
        pluc_analysis,
        results_report,
        "incomplete",
        "incomplete_proposed_land_use_code",
        "Entries which contain proposed land use code which are incomplete"
        " e.g. E(g) instead of E(g)(i)",
    )
    results_report = parse_luc_analysis(
        pluc_analysis,
        results_report,
        "other_issues",
        "other_issues_proposed_land_use_code",
        "Entries which have a invalid proposed land use code not defined"
        " above, These will require infilling",
    )
    return results_report


def parse_luc_analysis(
    results: dict[str, dict[str, pd.DataFrame]],
    results_report: global_classes.ResultsReport,
    key_name: str,
    column_name: str,
    notes: str,
) -> global_classes.ResultsReport:
    """adds a new filter column to the data report for land use codes only

    also updates the summary pages

    Parameters
    ----------
    results: dict[str, dict[str, pd.DataFrame]]
        filtered data set with invalid entries of the luc
    results_report : global_classes.ResultsReport
        report to append filter column to
    key_name : str
        name of the key of the luc analysis results to parse
    column_name : str
        new filter column name
    notes : str
        new filter column notes

    Returns
    -------
    global_classes.ResultsReport
        updated report
    """

    data_filter = add_multiple_filter_columns(
        results_report.data_filter, results[key_name], column_name,
    )
    analysis_summary = results_report.analysis_summary + [
        {
            "Residential": len(results[key_name]["residential"]),
            "Employment": len(results[key_name]["employment"]),
            "Mixed": len(results[key_name]["mixed"]),
        }
    ]

    analysis_summary_index_labels = (
        results_report.analysis_summary_index_labels + [column_name]
    )
    analysis_summary_notes = results_report.analysis_summary_notes + [
        notes
    ]

    filter_columns = results_report.filter_columns + [column_name]
    return global_classes.ResultsReport(
        data_filter,
        analysis_summary,
        analysis_summary_index_labels,
        analysis_summary_notes,
        filter_columns,
    )


def parse_analysis_results(
    results: dict[str, pd.DataFrame],
    results_report: global_classes.ResultsReport,
    index_name: str,
    notes: str,
) -> global_classes.ResultsReport:
    """adds a new filter column to the data report

    also updates the summary pages

    Parameters
    ----------
    results : dict[str, pd.DataFrame]
        filtered data set with invalid entries
    results_report : global_classes.ResultsReport
        report to append filter column to
    key_name : str
        name of the key of the luc analysis results to parse
    column_name : str
        new filter column name
    notes : str
        new filter column notes

    Returns
    -------
    global_classes.ResultsReport
        updated report
    """
    data_filter = add_multiple_filter_columns(
        results_report.data_filter, results, index_name,
    )
    analysis_summary = results_report.analysis_summary + [
        {
            "Residential": len(results["residential"]),
            "Employment": len(results["employment"]),
            "Mixed": len(results["mixed"]),
        }
    ]

    analysis_summary_index_labels = (
        results_report.analysis_summary_index_labels + [index_name]
    )
    analysis_summary_notes = results_report.analysis_summary_notes + [
        notes
    ]
    filter_columns = results_report.filter_columns + [index_name]
    return global_classes.ResultsReport(
        data_filter,
        analysis_summary,
        analysis_summary_index_labels,
        analysis_summary_notes,
        filter_columns,
    )


def plot_data(
    data: dict[str, gpd.GeoDataFrame],
    colour: dict[str, str],
    marker: dict[str, str],
    base: gpd.GeoDataFrame,
    limits: dict[str, list[int]],
    show_graph: bool,
    folder_path: pathlib.Path,
) -> None:
    """plots Geodataframes in data onto a base

    Parameters
    ----------
    data : dict[str, gpd.GeoDataFrame]
        data to be plotted
    colour : dict[str, str]
        colours to be used for point data
    marker : dict[str, str]
        markers to be used for point data
    base : gpd.GeoDataFrame
        base polygon to be used
    limits : dict[str, list[int]]
        limits to be used (OSGR)
    show_graphs : bool
        whether to show graphs while code is running
    folder_path : pathlib.Path
        folder path to save the plots
    """
    LOG.info("Producing site_locations explorer & plot")
    simplified_base = base.copy()
    simplified_base.loc[:, "geometry"] = base.simplify(
        SIMPLIFY_TOLERANCE
    )
    geo_explorer(
        "site_locations",
        folder_path,
        points=data,
        colour=colour,
        base=simplified_base,
    )

    geo_plotter(
        "site_locations",
        "Location of Development Sites",
        folder_path,
        limits=limits,
        points=data,
        marker=marker,
        show_figs=show_graph,
        colour=colour,
        base=base,
    )

    LOG.info("Performing calculations")
    total_dwellings = spatial_analysis(
        pd.concat(
            [
                data["R invalid"][["units_(dwellings)", "geometry"]],
                data["R No invalid"][["units_(dwellings)", "geometry"]],
                data["M invalid"][["units_(dwellings)", "geometry"]],
                data["M No invalid"][["units_(dwellings)", "geometry"]],
            ],
        ),
        base,
        "units_(dwellings)",
        "total_dwellings",
    )
    total_dwellings.loc[:, "geometry"] = total_dwellings.simplify(
        SIMPLIFY_TOLERANCE
    )
    total_floorspace = spatial_analysis(
        pd.concat(
            [
                data["E invalid"][["units_(floorspace)", "geometry"]],
                data["E No invalid"][
                    ["units_(floorspace)", "geometry"]
                ],
                data["E invalid"][["units_(floorspace)", "geometry"]],
                data["E No invalid"][
                    ["units_(floorspace)", "geometry"]
                ],
            ],
        ),
        base,
        "units_(floorspace)",
        "total_floorspace",
    )
    total_floorspace.loc[:, "geometry"] = total_floorspace.simplify(
        SIMPLIFY_TOLERANCE
    )

    total_dwellings.loc[:, "total_dwellings"] = (
        total_dwellings["total_dwellings"] / 1e3
    )
    total_dwellings.rename(
        columns={
            "total_dwellings": "total dwellings (units: thousand dwellings)"
        },
        inplace=True,
    )
    total_floorspace.loc[:, "total_floorspace"] = (
        total_floorspace["total_floorspace"] / 1e6
    )
    total_floorspace.rename(
        columns={
            "total_floorspace": "total_floorspace (units: million sq m)"
        },
        inplace=True,
    )
    invalid_ratio = spatial_invalid_ratio(
        pd.concat(
            [
                data["R No invalid"][["geometry"]],
                data["E No invalid"][["geometry"]],
                data["M No invalid"][["geometry"]],
            ],
        ),
        pd.concat(
            [
                data["R invalid"][["geometry"]],
                data["E invalid"][["geometry"]],
                data["M invalid"][["geometry"]],
            ],
        ),
        base,
        "region_invalid_percentage",
    )
    LOG.info("Producing total_dwellings explorer & plot")
    geo_explorer(
        "total_dwellings",
        folder_path,
        colour=colour,
        points=data,
        choropleth=total_dwellings,
        column="total dwellings (units: thousand dwellings)",
    )

    geo_plotter(
        "total_dwellings",
        r"Total Dwellings by LPA (Units: $10^3$ Dwellings)",
        folder_path,
        colour=colour,
        limits=limits,
        marker=marker,
        show_figs=show_graph,
        choropleth=total_dwellings,
        column="total dwellings (units: thousand dwellings)",
    )

    LOG.info("Producing total_floorspace explorer & plot")
    geo_explorer(
        "total_floorspace",
        folder_path,
        points=data,
        colour=colour,
        choropleth=total_floorspace,
        column="total_floorspace (units: million sq m)",
    )

    geo_plotter(
        "total_floorspace",
        r"Total Floorspace by LPA (Units: $10^6$ m$^2$)",
        folder_path,
        colour=colour,
        limits=limits,
        marker=marker,
        show_figs=show_graph,
        choropleth=total_floorspace,
        column="total_floorspace (units: million sq m)",
    )

    LOG.info("Producing invalid_ratio explorer & plot")
    geo_explorer(
        "invalid_ratio",
        folder_path,
        points=data,
        colour=colour,
        choropleth=invalid_ratio,
        column="region_invalid_percentage",
    )

    geo_plotter(
        "invalid_ratio",
        "Percentage of Invalid Entries by LPA",
        folder_path,
        colour=colour,
        limits=limits,
        marker=marker,
        show_figs=show_graph,
        choropleth=invalid_ratio,
        column="region_invalid_percentage",
    )


def geo_plotter(
    file_name: str,
    title: str,
    path: pathlib.Path,
    limits: dict[str, list[int]],
    points: Optional[dict[str, gpd.GeoDataFrame]] = None,
    marker: Optional[dict[str, str]] = None,
    colour: Optional[dict[str, str]] = None,
    choropleth: Optional[gpd.GeoDataFrame] = None,
    column: Optional[str] = None,
    base: Optional[gpd.GeoDataFrame] = None,
    show_figs: bool = True,
) -> None:
    """create a geographical plot from inputs

    base will just plot shape, choropleth wil produce a colour map based
    on regions and specified column value (a column must be specified when
    using choropleth)

    Parameters
    ----------
    title : str
        the file name without extension
    path : pathlib.Path
        path where the output should be saved
    points : Optional[dict[str, gpd.GeoDataFrame]], optional
        a dictionary of geodataframes with point geometery by default None
    marker : Optional[dict[str, str]], optional
        contains the markers for points, must have seem keys as points, by default None
    colour : Optional[dict[str, str]], optional
        contains the colours for points, must have seem keys as points, by default None
    choropleth : Optional[gpd.GeoDataFrame], optional
        ploygon with a column to be used as a heatmap, by default None
    column : Optional[str], optional
        column in choropleth to be used as heatmap, by default None
    base : Optional[gpd.GeoDataFrame], optional
        a ploygon to be plotted, by default None
    limits : dict[str, list[int]]
        graph limits in OSGR, by default None
    show_figs : bool, optional
        whether the figures should be shown, by default True

    Raises
    ------
    ValueError
        if a column is not defined and choropleth is, consider using base
        (no heatmap) or define a column within choropleth
    """
    fig, ax = plt.subplots()
    ax.set_aspect("equal")

    if base is not None:  # plot base
        base.plot(ax=ax, color="white", edgecolor="black")

    if choropleth is not None:
        if column is None:
            raise ValueError(
                "if a chloropleth is passesd, a column should be passed too"
            )
        choropleth.plot(
            ax=ax,
            column=column,
            legend=True,
            edgecolor="black",
            label=column,
        )

    if points is not None:  # plot points
        if marker is None or colour is None:
            for key, value in points.items():
                value.plot(ax=ax, markersize=5, label=key)
        else:
            for key, value in points.items():
                value.plot(
                    ax=ax,
                    marker=marker[key],
                    color=colour[key],
                    markersize=5,
                    label=key,
                )
        fig.legend()

    ax.set_xticks([])  # turn off axis labels
    ax.set_yticks([])
    plt.title(title)
    ax.text(
        0.01,
        0.01,
        s="Source: Office for National Statistics licensed under"
        " the Open Government Licence v.3.0\n Contains OS data ©"
        " Crown copyright and database right [2021]",
        transform=fig.transFigure,
    )
    if limits is not None:  # set limits
        ax.set_xlim(limits["x"][0], limits["x"][1])
        ax.set_ylim(limits["y"][0], limits["y"][1])
    fig.savefig(path / f"{file_name}.png")
    if show_figs:
        plt.show()
    plt.close()


def geo_explorer(
    title: str,
    path: pathlib.Path,
    points: Optional[dict[str, gpd.GeoDataFrame]] = None,
    colour: Optional[dict[str, gpd.GeoDataFrame]] = None,
    choropleth: Optional[gpd.GeoDataFrame] = None,
    column: Optional[str] = None,
    base: Optional[gpd.GeoDataFrame] = None,
) -> None:
    """create an interactive geographical plot from inputs in html format

    base will just plot shape, choropleth wil produce a colour map based
    on regions and specified column value (a column must be specified when
    using choropleth)

    Parameters
    ----------
    title : str
        the file name without extension
    path : pathlib.Path
        path where the output should be saved
    points : Optional[dict[str, gpd.GeoDataFrame]], optional
        a dictionary of geodataframes with point geometery by default None
    colour : Optional[dict[str, str]], optional
        contains the colours for points, must have seem keys as points, by default None
    choropleth : Optional[gpd.GeoDataFrame], optional
        ploygon with a column to be used as a heatmap, by default None
    column : Optional[str], optional
        column in choropleth to be used as heatmap, by default None
    base : Optional[gpd.GeoDataFrame], optional
        a ploygon to be plotted, by default None
    Raises
    ------
    ValueError
        if a column is not defined and choropleth is, consider using base
        (no heatmap) or define a column within choropleth
    """

    if choropleth is not None:
        base = None
    explorer = None
    if base is not None:  # plot base
        # TODO more robust CRS conversion required
        base = base.to_crs(epsg=4326)
        explorer = base.explore()

    if choropleth is not None:
        #TODO more robust CRS conversion
        choropleth = choropleth.to_crs(epsg=4326)
        if column is None:
            raise ValueError(
                "if a chloropleth is passesd, a column should be passed too"
            )
        filtered_choropleth = choropleth.loc[choropleth[column] > 0, :]
        if explorer is None:
            explorer = filtered_choropleth.explore(
                column, legend=True, name=column
            )
        else:
            explorer = filtered_choropleth.explore(
                column, m=explorer, legend=True, name=title
            )
    if points is not None:  # plot points
        if colour is None:
            raise ValueError(
                "colour must be given when points is provided"
            )
        if explorer is None:
            for key, value in points.items():
                # TODO more robust CRS conversion required
                temp = value[
                    ["site_reference_id", "geometry"]
                ].set_geometry("geometry")
                #TODO more robust CRS conversion
                temp = temp.to_crs(epsg=4326)
                explorer = temp.explore(
                    name=key, color=colour[key], legend=True, show=False
                )
        else:
            for key, value in points.items():
                temp = value[
                    ["site_reference_id", "geometry"]
                ].set_geometry("geometry")
                # TODO more robust CRS conversion required
                temp = temp.to_crs(epsg=4326)
                if len(temp) == 0:
                    continue
                explorer = temp.explore(
                    m=explorer,
                    color=colour[key],
                    name=key,
                    legend=True,
                    show=False,
                )

    if explorer is None:
        LOG.warning(f"you have not given any data to explore {title}")
    else:
        folium.LayerControl().add_to(explorer)
        explorer.save(path / f"{title}.html", default=str)


def spatial_invalid_ratio(
    not_invalid_data: gpd.GeoDataFrame,
    invalid_data: gpd.GeoDataFrame,
    base: gpd.GeoDataFrame,
    new_column_name: str,
) -> gpd.GeoDataFrame:
    """calculates the ratio of invalid data against all the data for each region

    ratio outputted as a percentage (100%: all data invalid) appended to base

    Parameters
    ----------
    not_invalid_data : gpd.GeoDataFrame
        geo data frame of valid data
    invalid_data : gpd.GeoDataFrame
        geo data frame of invalid data
    base : gpd.GeoDataFrame
        base polygon shape to use
    new_column_name : str
        column name for the result

    Returns
    -------
    gpd.GeoDataFrame
        base with the result appended as a column
    """
    not_invalid_joined_data = not_invalid_data.sjoin(
        base, how="left"
    ).drop(
        columns=[
            "LPA19CD",
            "LPA19NM",
            "BNG_E",
            "BNG_N",
            "LONG",
            "LAT",
            "Shape__Are",
            "Shape__Len",
            "geometry",
        ]  # only want ONJECTID
    )
    invalid_joined_data = invalid_data.sjoin(base, how="left").drop(
        columns=[
            "LPA19CD",
            "LPA19NM",
            "BNG_E",
            "BNG_N",
            "LONG",
            "LAT",
            "Shape__Are",
            "Shape__Len",
            "geometry",
        ]  # only want ONJECTID
    )
    region_invalid_percentage = []
    object_id = base["OBJECTID"].unique()
    for region_id in object_id:
        invalid_regional_data = invalid_joined_data[
            invalid_joined_data["OBJECTID"] == region_id
        ]
        not_invalid_region_data = not_invalid_joined_data[
            not_invalid_joined_data["OBJECTID"] == region_id
        ]
        if (
            len(invalid_regional_data) == 0
            and len(not_invalid_region_data) == 0
        ):
            invalid_percentage = 0.0
        else:
            invalid_percentage = (
                float(len(invalid_regional_data))
                / (
                    float(len(invalid_regional_data))
                    + float(len(not_invalid_region_data))
                )
                * 100
            )
        region_invalid_percentage.append(invalid_percentage)
    region_results = pd.DataFrame(
        region_invalid_percentage,
        index=object_id,
        columns=[new_column_name],
    )
    return base.join(region_results, on="OBJECTID", how="left")


def spatial_analysis(
    data: gpd.GeoDataFrame,
    base: gpd.GeoDataFrame,
    column: str,
    new_column_name,
) -> gpd.GeoDataFrame:
    """calculates the ratio of invalid data against all the data for each region

    ratio outputted as a percentage (100%: all data invalid)

    Parameters
    ----------
    data : gpd.GeoDataFrame
        geo data frame conatining data to be analysed
    base : gpd.GeoDataFrame
        base polygon shape to use
    column : str
        _description_

    Returns
    -------
    gpd.GeoDataFrame
        _description_
    """
    joined_data = data.sjoin(base, how="left").drop(
        columns=[
            "LPA19CD",
            "LPA19NM",
            "BNG_E",
            "BNG_N",
            "LONG",
            "LAT",
            "Shape__Are",
            "Shape__Len",
            "geometry",
        ]  # only want ONJECTID
    )
    region_total = []
    object_id = base["OBJECTID"].unique()
    for region_id in object_id:
        region_data = joined_data[joined_data["OBJECTID"] == region_id]
        if len(region_data) == 0:
            region_total.append(0)
        else:
            region_total.append(region_data[column].sum())
    region_results = pd.DataFrame(
        region_total, index=object_id, columns=[new_column_name]
    )
    return base.join(region_results, on="OBJECTID", how="left")


def add_multiple_filter_columns(
    original: dict[str, pd.DataFrame],
    subset: dict[str, pd.DataFrame],
    column_name: str,
) -> dict[str, pd.DataFrame]:
    """adds a new filter column for multiple arrays

    original and subset must have identical keys

    Parameters
    ----------
    original : dict[str,pd.DataFrame]
        a dictionary conataining the dataframe to have the filter column
    subset : dict[str,pd.DataFrame]
        a dictionary conataining the rows for which the filter column value should be true
    column_name : str
        name of the filter column

    Returns
    -------
    dict[str, pd.DataFrame]
        original which the filter columns added
    """
    filter_added = {}
    for key, value in original.items():
        filter_added[key] = add_filter_column(
            value, subset[key], column_name
        )
    return filter_added


def find_multiple_missing_values(
    data: dict[str, pd.DataFrame],
    test_columns: dict[str, list[str]],
    not_allowed: dict[str, list[str | int]],
) -> dict[str, pd.DataFrame]:
    """finds missing values in each datafram contained within a dictionary

    wrapper for find_missing_values

    Parameters
    ----------
    data : dict[str, pd.DataFrame]
        data to be analysed
    test_columns : dict[str, list[str]]
        column to be analysed needs idetical keys to data
    not_allowed : dict[str, list[str  |  int]]
        invalid values to be analysed needs idetical keys to data

    Returns
    -------
    dict[str, pd.DataFrame]
        the entries with missing values to be tested
    """
    missing_values = {}
    for key, value in data.items():
        missing_values[key] = find_missing_values(
            value, [test_columns[key]], not_allowed[key]
        )
    return missing_values


def add_filter_column(
    original_df: pd.DataFrame, subset_df: pd.DataFrame, filter_name: str
) -> pd.DataFrame:
    """if entry of original is also in subset, the filter column will be set to True

    default filter value = False.
    If subset is not a subset of original, a ValueError will be raised.

    Parameters
    ----------
    original_df : pd.DataFrame
        DataFrame have filter column added
    subset_df : pd.DataFrame
        the subset of the df for which fiter = True, others will = False
    filter_name : str
        name of the filter column

    Returns
    -------
    pd.DataFrame
        original_df with filter column added
    """
    # TODO implement subset check
    invalid_filter_df = original_df.copy()
    invalid_filter_df[filter_name] = False
    invalid_filter_df.loc[
        subset_df.index, filter_name
    ] = True  # TODO do on entire row
    return invalid_filter_df


def data_completeness_assessment(record: pd.DataFrame) -> pd.DataFrame:
    """outputs ratio of non NaN entries each column in a pandas DF as a %

    determines a record as incomplete if it is NaN, regardless of of the cell content
    e.g. "unknown" would still be classed as a complete record

    Parameters
    ----------
    record : pd.DataFrame
        any pd.DF to be assessed

    Returns
    -------
    pd.DataFrame
        the columns of the inputted DF and it's percentage completeness
    """
    completeness = []
    for column in record.columns:
        percent_complete = (
            record[column].count() / len(record[column]) * 100
        )
        LOG.debug(f"{column}: {percent_complete} % complete")
        completeness.append(
            {"column": column, "percent_complete": percent_complete}
        )
    return pd.DataFrame(completeness)


def find_missing_values(
    record: pd.DataFrame,
    columns: list[str],
    not_allowed: list[int | str],
) -> pd.DataFrame:
    """finds and returns missing values

    will return entries with values in the not_allowed input and values that are nan

    Parameters
    ----------
    record : pd.DataFrame
        record to check
    columns : list[str]
        columns to containing coordinates to check
    not_allowed : list[int | str]
        list of not allowed values

    Returns
    -------
    pd.DataFrame
        entries with missing values

    """

    missing_values_df = []
    for column in columns:

        missing_values = [record[column].isna()]

        for value in not_allowed:
            missing_values.append(record[column] == value)

        all_missing_values = pd.concat(missing_values, axis=1)
        missing_values_df.append(record[all_missing_values.any(axis=1)])

    if len(missing_values_df) == 1:
        return missing_values_df[0]
    else:
        return (
            pd.concat(missing_values_df)
            .drop_duplicates(subset=["site_reference_id", "site_name"])
            .sort_values(by="site_reference_id")
        )


def check_id_value_consistency(
    data: dict[str, pd.DataFrame],
    lookup_table: pd.DataFrame,
    id_name: str,
    value_name: str,
) -> dict[str, pd.DataFrame]:
    """checks whether the value_id is consistent with the value in the data

    returns any entries for which this is not true
    Parameters
    _extended_summary_

    Parameters
    ----------
    data : dict[str, pd.DataFrame]
        data to check
    lookup_table : pd.DataFrame
        DLog lookup table for the id value pair to be tested
    id_name : str
        id column name
    value_name : str
        value column name

    Returns
    -------
    dict[str, pd.DataFrame]
        values with contradictor id and value, same keys as data input
    """
    contra = {}
    for key, item in data.items():
        check_record = item.merge(
            lookup_table,
            left_on=id_name,
            right_on="id",
            how="left",
            suffixes=["", "_table"],
        )
        contra[key] = item[
            check_record[value_name]
            != check_record[f"{value_name}_table"]
        ]
    return contra


def find_invalid_land_use_codes(
    record: pd.DataFrame, land_use_codes: pd.Series, columns: list[str]
) -> pd.DataFrame:
    """finds invalid land use codes in the column provided

    returns the invaild entries (luc) not in land_use codes

    Parameters
    ----------
    record : pd.DataFrame
        data to be checked
    land_use_codes : pd.Series
        allowed land use codes to compare data against
    columns : list[str]
        columns with land use codes to check

    Returns
    -------
    pd.DataFrame
        entries with invalid land use codes

    Raises
    ------
    ValueError
        no columns found
    """
    invalid_land_use = []
    for column in columns:
        exploded_land_use_codes = (
            record[column].str.join(",").str.split(",", expand=True)
        )
        invalid_land_use.append(
            record.loc[
                ~exploded_land_use_codes.isin(
                    land_use_codes.tolist() + [None]
                ).all(axis=1)
            ]
        )
    if len(invalid_land_use) == 0:
        raise ValueError("No columns found")

    elif len(invalid_land_use) == 1:
        return invalid_land_use[0]

    else:
        return (
            pd.concat(invalid_land_use)
            .drop_duplicates(subset=["site_reference_id"])
            .sort_values("site_reference_id")
        )


def find_contradictory_tag_const_plan(
    data: dict[str, pd.DataFrame]
) -> dict[str, pd.DataFrame]:
    """returns contradictory tag certainty, construction and planning status

    checks for combinations that do not make sense e.g. work completed but
    planning permission not granted

    Parameters
    ----------
    data : dict[str, pd.DataFrame]
        data to analyse

    Returns
    -------
    dict[str, pd.DataFrame]
        contradictory rows
    """
    contra = {}
    for key, value in data.items():

        construction_started_completed = value[
            pd.DataFrame(
                [
                    value["construction_status_id"] == 2,
                    value["construction_status_id"] == 3,
                ]
            )
            .transpose()
            .any(axis=1)
        ]

        not_permissioned = value[value["planning_status_id"] == 1]
        near_certain = value[value["web_tag_certainty_id"] == 1]
        less_than_mtl = value[value["web_tag_certainty_id"] > 2]

        contra_constr_perm = construction_started_completed[
            construction_started_completed.index.isin(
                not_permissioned.index
            )
        ]
        contra_plan_perm = not_permissioned[
            not_permissioned.index.isin(near_certain.index)
        ]
        contra_constr_tag = construction_started_completed[
            construction_started_completed.index.isin(
                less_than_mtl.index
            )
        ]

        # can't drop duplicates of all column values as some columns are lists
        all_contra = pd.concat(
            [contra_constr_perm, contra_plan_perm, contra_constr_tag]
        )
        contra_values = all_contra.drop_duplicates(
            subset=["site_reference_id"]
        )

        contra[key] = contra_values
    return contra


def analyse_invalid_luc(
    invalid_luc: dict[str, pd.DataFrame],
    columns: dict[str, list[str]],
    land_use_codes: pd.Series,
    out_of_date_luc: pd.Series,
    incomplete_luc: pd.Series,
) -> dict[str, dict[str, pd.DataFrame]]:
    """determines why the land use code is invalid

    seperates the invalid land use code into  out_of_date,
    wrong_format, incomplete and other_issues. other issues
    contains any entries not classified by the other checks

    Parameters
    ----------
    invalid_luc : dict[str, pd.DataFrame]
        dictionary containing the invalid formats for each of the
        sheets that require analysis
    columns : dict[str, list[str]]
        columns that require analysis, same keys as invalid_luc
    land_use_codes : pd.Series
        allowed land use code lookup
    out_of_date_luc : pd.Series
        out of date land use code lookup
    incomplete_luc : pd.Series
        incomplete land use code lookup

    Returns
    -------
    dict[str, dict[str, pd.DataFrame]]
        the original data set seperated by the error type,
        indicated by the key

    Raises
    ------
    ValueError
        no columns found
    """
    out_of_date_output = {}
    wrong_format_output = {}
    incomplete_output = {}
    other_issues_output = {}

    wrong_format_check = [
        s for s in land_use_codes.tolist() if "(" in s or ")" in s
    ]
    wrong_format_check = [
        s.replace("(", "").replace(")", "") for s in wrong_format_check
    ]

    incomplete_luc_ = [
        s for s in incomplete_luc.tolist() if "(" in s or ")" in s
    ]
    incomplete_luc_ = incomplete_luc.tolist() + [
        s.replace("(", "").replace(")", "") for s in incomplete_luc_
    ]
    for key, value in invalid_luc.items():
        out_of_date = []
        incomplete = []
        formatting = []
        for column in columns[key]:
            exploded_land_use_codes = (
                value[column].str.join(",").str.split(",", expand=True)
            )
            out_of_date.append(
                value.loc[
                    exploded_land_use_codes.isin(
                        out_of_date_luc.tolist()
                    ).any(axis=1)
                ]
            )
            incomplete.append(
                value.loc[
                    exploded_land_use_codes.isin(incomplete_luc_).any(
                        axis=1
                    )
                ]
            )
            formatting.append(
                value.loc[
                    exploded_land_use_codes.isin(
                        wrong_format_check
                    ).any(axis=1)
                ]
            )

        if len(out_of_date) == 0:
            raise ValueError("No columns found")

        elif len(out_of_date) == 1:
            out_of_date_output[key] = out_of_date[0]

        else:
            out_of_date_output[key] = (
                pd.concat(out_of_date)
                .drop_duplicates(
                    subset=["site_reference_id", "site_name"]
                )
                .sort_values("site_reference_id")
            )
        if len(incomplete) == 0:
            raise ValueError("No columns specified")

        elif len(incomplete) == 1:
            incomplete_output[key] = incomplete[0]

        else:
            incomplete_output[key] = (
                pd.concat(incomplete)
                .drop_duplicates(subset=["site_reference_id"])
                .sort_values("site_reference_id")
            )

        if len(formatting) == 0:
            raise ValueError("No columns specified")

        elif len(formatting) == 1:
            wrong_format_output[key] = formatting[0]

        else:
            wrong_format_output[key] = (
                pd.concat(formatting)
                .drop_duplicates(subset=["site_reference_id"])
                .sort_values("site_reference_id")
            )
        temp = pd.concat(
            [
                wrong_format_output[key],
                out_of_date_output[key],
                incomplete_output[key],
            ]
        ).drop_duplicates(subset=["site_reference_id", "site_name"])
        other_issues_output[key] = value.drop(index=temp.index)
    return {
        "out_of_date": out_of_date_output,
        "wrong_format": wrong_format_output,
        "incomplete": incomplete_output,
        "other_issues": other_issues_output,
    }


def find_missing_ids(
    ids_l: pd.Series, ids_s: pd.Series
) -> Optional[pd.Series]:
    """finds the IDs in ids_l that do not exist in id_s

    This should be used for series where id_s is a subset of id_l,
    however this function will also check this assumption

    Parameters
    ----------
    ids_l : pd.Series
        a set of IDs
    ids_s : pd.Series
        a subset of id_l, for which the missing values will be
        returned

    Returns
    -------
    pd.Series
        IDs from id_l missing in id_s

    Raises
    ------
    ValueError
        find_missing_ids: ids_s is not a subset of id_l
    """
    if len(pd.merge(ids_s, ids_l, how="inner")) != len(ids_s):
        # checks if id_s is a subset of id_l and for duplicates
        raise ValueError(
            "find_missing_ids: ids_s is not a subset of id_l"
        )
    missing_ids = ids_l[~ids_l.isin(ids_s)]
    return missing_ids


def find_inactivate_entries(
    data: dict[str, pd.DataFrame]
) -> dict[str, pd.DataFrame]:
    """returns any values that do not have a "t" value in the active column

    Parameters
    ----------
    data : dict[str, pd.DataFrame]
        data to be tested

    Returns
    -------
    dict[str, pd.DataFrame]
        inactive entries, same keys as input
    """
    inactive = {}
    for key, value in data.items():
        inactive[key] = value[value["active"] != "t"]
    return inactive
