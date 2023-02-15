"""Automatically fixes and infills data where possible

    IN PROGRESS
"""
# standard imports
from __future__ import annotations

import logging
from typing import Optional
import pathlib
# third party imports
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

# local imports
from dlit_lu import global_classes, utilities, analyse, inputs

# constants
LOG = logging.getLogger(__name__)


def correct_inavlid_syntax(
    data: global_classes.DLogData,
    auxiliary_data: global_classes.AuxiliaryData,
) -> global_classes.DLogData:
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
    LOG.info("performing automatic syntax fixes")
    data_dict = {
        "residential": data.residential_data,
        "employment": data.employment_data,
        "mixed": data.mixed_data,
    }

    # define columns

    land_use_columns = {
        "residential": ["existing_land_use"],
        "employment": ["existing_land_use", "proposed_land_use"],
        "mixed": ["existing_land_use", "proposed_land_use"],
    }

    # TODO does not include dwelling units for mixed, currently fixed manually
    data_dict = utilities.to_dict(data)

    corrected_format = fix_site_ref_id(data_dict)

    corrected_format = incorrect_luc_formatting(
        corrected_format, land_use_columns, auxiliary_data
    )

    return global_classes.DLogData(
        None,
        corrected_format["residential"],
        corrected_format["employment"],
        corrected_format["mixed"],
        data.lookup,
    )


def infill_data(data: global_classes.DLogData,
                auxiliary_data: global_classes.AuxiliaryData,
                output_folder: pathlib.Path,
                config: inputs.DLitConfig) -> global_classes.DLogData:
    """Infills data for which assumptions are required

    infills missing areas, units, land use codes with multiple possible values

    Parameters
    ----------
    data : global_classes.DLogData
        data to infill
    auxiliary_data : global_classes.AuxiliaryData
        auxiliary data from parser

    Returns
    -------
    global_classes.DLogData
        infilled data
    """
    LOG.info("performing automatic infilling fixes")
    data_dict = {
        "residential": data.residential_data,
        "employment": data.employment_data,
        "mixed": data.mixed_data,
    }

    # define columns
    area_columns_list = {
        "residential": ["total_site_area_size_hectares"],
        "employment": ["site_area_ha"],
        "mixed": ["total_area_ha"],
    }

    area_columns = {
        "residential": "total_site_area_size_hectares",
        "employment": "site_area_ha",
        "mixed": "total_area_ha",
    }

    land_use_columns = {
        "residential": ["existing_land_use"],
        "employment": ["existing_land_use", "proposed_land_use"],
        "mixed": ["existing_land_use", "proposed_land_use"],
    }

    units_columnns = {
        "residential": ["units_(dwellings)", "total_units"],
        "employment": ["total_area_sqm", "units_(floorspace)"],
        "mixed": ["floorspace_sqm", "units_(floorspace)"],
    }

    # calculate ratios
    distribution_path = config.output_folder/"distribution_plots"
    distribution_path.mkdir(exist_ok=True)

    dwelling_area_ratio = unit_area_ratio(
        dict((k, data_dict[k]) for k in (["residential", "mixed"])),
        {"residential": "total_units", "mixed": "dwellings"},
        dict((k, area_columns[k]) for k in (["residential", "mixed"])),
        distribution_path / "dwelling_site_area_ratio_dist.png"
    )

    floorspace_area_ratio = unit_area_ratio(
        dict((k, data_dict[k]) for k in (["employment", "mixed"])),
        {"employment": "total_area_sqm", "mixed": "floorspace_sqm"},
        dict((k, area_columns[k]) for k in (["employment", "mixed"])),
        distribution_path / "GFA_site_area_ratio_dist.png"
    )

    average_area = calculate_average(
        data_dict, area_columns_list, distribution_path)

    inputs.InfillingAverages(
        average_res_area=average_area["residential"],
        average_emp_area=average_area["employment"],
        average_mix_area=average_area["mixed"],
        average_gfa_site_area_ratio=floorspace_area_ratio,
        average_dwelling_site_area_ratio=dwelling_area_ratio,
    ).save_yaml(output_folder/inputs.AVERAGE_INFILLING_VALUES_FILE)
    # infill values
    corrected_format = infill_missing_site_area(data_dict, area_columns_list,
                                                [0, "-"], average_area)
    corrected_format = infill_units(corrected_format, units_columnns,
                                    area_columns, ["-", 0],
                                    {"residential": dwelling_area_ratio,
                                     "employment": floorspace_area_ratio,
                                     "mixed": floorspace_area_ratio})

    corrected_format["mixed"] = infill_units({"mixed": corrected_format["mixed"]},
                                             {"mixed": ["dwellings", "units_(dwellings)"]}, {
        "mixed": "total_area_ha"}, ["-", 0],
        {"mixed": dwelling_area_ratio})["mixed"]

    corrected_format = old_incomplete_known_luc(
        corrected_format, land_use_columns, auxiliary_data
    )

    corrected_format = fix_missing_lucs(
        corrected_format,
        land_use_columns,
        ["unknown", "mixed"],
        auxiliary_data.allowed_codes["land_use_codes"].to_list(),
    )

    corrected_format = fix_undefined_invalid_luc(corrected_format, land_use_columns,
        auxiliary_data.allowed_codes["land_use_codes"].to_list(
        ),
        auxiliary_data,
        {"existing_land_use": "other_issues_existing_land_use_code",
        "proposed_land_use": "other_issues_proposed_land_use_code"})

    corrected_format = infill_missing_tag(corrected_format)

    corrected_format = infill_missing_years(
        corrected_format, data.lookup.webtag
    )

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
        s
        for s in auxiliary_data.allowed_codes["land_use_codes"].tolist()
        if "(" in s or ")" in s
    ]
    wrong_format_check = [
        s.replace("(", "").replace(")", "")
        for s in possible_error_codes
    ]
    format_lookup = pd.DataFrame(
        [possible_error_codes, wrong_format_check]
    ).transpose()
    format_lookup.columns = ["land_use_code", "incorrect_format"]
    format_lookup = format_lookup.append(
        {"land_use_code": "sg", "incorrect_format": "suigeneris"},
        ignore_index=True,
    )

    fixed_format = {}

    # no assumption required
    for key, value in data.items():
        # this ensures fixed format is a copy, not a pointer
        fixed_format[key] = value.copy()
        for column in columns[key]:
            fixed_format[key][column] = fixed_format[key][column].apply(
                find_and_replace_luc,
                lookup_table=format_lookup,
                find_column_name="incorrect_format",
                replace_column_name="land_use_code",
            )

    return fixed_format


def calc_average_years_webtag_certainty(
    data: dict[str, pd.DataFrame], webtag_lookup: pd.DataFrame,
) -> dict[int, list[int]]:
    """calculates the mode start and end year for each catergory of webtag certainty

    ignores "not specified", uses the webtag certainty id as the key
    in the output

    Parameters
    ----------
    data : pd.DataFrame
        data to analyse
    webtag_lookup : pd.DataFrame
        webtag lookup
    years_lookup : pd.DataFrame
        years lookup

    Returns
    -------
    dict[int, list[int]]
        average years
    """
    average_years = {}

    for id_ in webtag_lookup.index:
        if id_ == 0:
            continue
        all_start_years = pd.Series([])
        all_end_years = pd.Series([])
        for _, value in data.items():
            # filter df for each webtag status without missing years
            filtered_value = value[value["missing_years"] == False]
            filtered_value = filtered_value[
                value["web_tag_certainty_id"] == id_
            ]

            all_start_years = all_start_years.append(
                filtered_value["start_year_id"], ignore_index=True
            )
            all_end_years = all_end_years.append(
                filtered_value["end_year_id"], ignore_index=True
            )
        mode_start_year = all_start_years.mode().values[0]
        mode_end_year = all_end_years.mode().values[0]

        if mode_start_year > mode_end_year:
            LOG.warning(
                f"infilled years for TAG status {webtag_lookup[id_]} have end years"
                " that are before start years, setting end"
                " year equal to start year ({mode_start_year})"
            )

            mode_end_year = mode_start_year
        average_years[id_] = [mode_start_year, mode_end_year]

    return average_years


def infill_missing_tag(
    data: dict[str, pd.DataFrame]
) -> dict[str, pd.DataFrame]:
    """infills TAG certainty where it is not specified

    the infill value is determined from the planning and/or construction status

    Parameters
    ----------
    data : dict[str, pd.DataFrame]
        data to infill

    Returns
    -------
    dict[str, pd.DataFrame]
        infilled data
    """
    infilled_data = {}
    infill_lookup = {
        "permissioned": 2,
        "not_permissioned_no_years": 4,
        "not_permissioned_with_years": 3,
        "not_specified_in_construction": 1,
        "not_specified_not_started_specified": 4,
    }

    for key, value in data.items():

        to_be_infilled = value.copy()
        missing_tag = value[value["web_tag_certainty_id"] == 0]

        # permissioned
        missing_tag_permissioned = missing_tag.loc[
            missing_tag["planning_status_id"] == 2, :
        ]

        missing_tag_permissioned.loc[
            :, "web_tag_certainty_id"
        ] = infill_lookup["permissioned"]

        # not permissioned
        missing_tag_not_permissioned = missing_tag[
            missing_tag["planning_status_id"] == 1
        ]

        # without_years
        missing_tag_not_permissioned.loc[missing_tag_not_permissioned["missing_years"] ==
            True, "web_tag_certainty_id"] = infill_lookup["not_permissioned_no_years"]

        # with_years
        missing_tag_not_permissioned.loc[missing_tag_not_permissioned["missing_years"] ==
                False, "web_tag_certainty_id"] = infill_lookup["not_permissioned_with_years"]

        # not specified
        missing_tag_not_spec = missing_tag.loc[
            missing_tag["planning_status_id"] == 0, :
        ]

        # completed or undergoing constructiom
        completed_undergoing_constr = (
            pd.DataFrame(
                [
                    missing_tag_not_spec["construction_status_id"] == 2,
                    missing_tag_not_spec["construction_status_id"] == 3,
                ]
            )
            .transpose()
            .any(axis=1)
        )

        missing_tag_not_spec.loc[completed_undergoing_constr,
            "web_tag_certainty_id"] = infill_lookup["not_specified_in_construction"]

        # not started/not specified
        missing_tag_not_spec.loc[~completed_undergoing_constr,
            "web_tag_certainty_id"] = infill_lookup["not_specified_not_started_specified"]

        # infill
        to_be_infilled.loc[
            missing_tag_permissioned.index, :
        ] = missing_tag_permissioned
        to_be_infilled.loc[
            missing_tag_not_permissioned.index, :
        ] = missing_tag_not_permissioned
        to_be_infilled.loc[
            missing_tag_not_spec.index, :
        ] = missing_tag_not_spec

        infilled_data[key] = to_be_infilled

    return infilled_data


def infill_one_missing_year(
    data: dict[str, pd.DataFrame], average_years: dict[int, list[int]]
) -> dict[str, pd.DataFrame]:
    """infills start/end year when end/start year is present

    takes the average periods for each tag status and uses that to
    infill. if this will result in an invalid year, the missing year
    will be infilled with the existing year.

    Parameters
    ----------
    data : dict[str, pd.DataFrame]
        data to infill
    average_years : dict[int, list[int]]
        average year for each tag status

    Returns
    -------
    dict[str, pd.DataFrame]
        infilled years
    """

    missing_start_id = analyse.find_multiple_missing_values(data,
        dict((k, ["start_year_id"])
                for k in data.keys()),
        dict((k, [14, ""]) for k in data.keys()))

    missing_end_id = analyse.find_multiple_missing_values(data,
        dict((k, ["end_year_id"])
            for k in data.keys()),
        dict((k, [14, ""]) for k in data.keys()))

    fixed = {}

    for key, value in data.items():
        fixed_data = value.copy()
        for id_, average_year in average_years.items():
            #loop through average year for each webtag status
            to_be_fixed = fixed_data[
                fixed_data["web_tag_certainty_id"] == id_
            ].copy()
            #period is start year - end year
            period = average_year[1] - average_year[0] 
            #gets all entries with no start year
            no_start_index = missing_start_id[key][
                missing_start_id[key]["web_tag_certainty_id"] == id_
            ].index
            #gets all entries with no end year
            no_end_index = missing_end_id[key][
                missing_end_id[key]["web_tag_certainty_id"] == id_
            ].index
            #get all values that have only end year
            end_no_start = no_start_index[
                ~no_start_index.isin(no_end_index)
            ]
            end_no_start_values = to_be_fixed.loc[end_no_start]
            #get all values that have only start year
            start_no_end = no_end_index[
                ~no_end_index.isin(no_start_index)
            ]
            start_no_end_values = to_be_fixed.loc[start_no_end]

            #set start to end if applying period will set value out of bounds
            mask_end = end_no_start_values["end_year_id"] <= period
            end_no_start_values.loc[mask_end, "start_year_id"
                ] = end_no_start_values.loc[mask_end, "end_year_id"]
            #set start to end - period if result in bounds
            end_no_start_values.loc[~mask_end,"start_year_id"] = (
                end_no_start_values.loc[~mask_end, "end_year_id"] - period
            )

            mask_start = start_no_end_values["start_year_id"] + period >= 14
            #set end to start if result is out of bounds
            start_no_end_values.loc[mask_start, "end_year_id",
                ] = start_no_end_values.loc[mask_start, "start_year_id",]
            #set end to start + period if result is in bounds
            start_no_end_values.loc[~mask_start,"end_year_id"] = (
                start_no_end_values.loc[~mask_start,"start_year_id"]+ period)

            #integrate results in to data set
            to_be_fixed.loc[
                end_no_start, "start_year_id"
            ] = end_no_start_values["end_year_id"]
            to_be_fixed.loc[
                start_no_end, "end_year_id"
            ] = start_no_end_values["start_year_id"]
            fixed_data.loc[to_be_fixed.index] = to_be_fixed
        fixed[key] = fixed_data
    return fixed


def infill_missing_years(
    data: dict[str, pd.DataFrame],
    tag_lookup: pd.DataFrame
) -> dict[str, pd.DataFrame]:
    """infills missing years

    infills using the model start and end year for the tag certainty of the entry

    Parameters
    ----------
    data : dict[str, pd.DataFrame]
        data to infill
    tag_lookup : pd.DataFrame
        webtag lookup from dlog

    Returns
    -------
    dict[str, pd.DataFrame]
        infilled data
    """
    average_years = calc_average_years_webtag_certainty(
        data, tag_lookup
    )

    data = infill_one_missing_year(data, average_years)

    fixed_data = {}

    missing_year_id = analyse.find_multiple_missing_values(
        data,
        dict(
            (k, ["start_year_id", "end_year_id"]) for k in data.keys()
        ),
        dict((k, [14, ""]) for k in data.keys()),
    )

    for key, value in data.items():
        fixed_data[key] = value.copy()

        filtered_data = missing_year_id[key][
            missing_year_id[key]["web_tag_certainty_id"] != 0
        ]
        for id_ in average_years.keys():
            id_ = int(id_)
            filtered_data.loc[filtered_data["web_tag_certainty_id"]
                              == id_, "start_year_id"] = average_years[id_][0]
            filtered_data.loc[filtered_data["web_tag_certainty_id"]
                              == id_, "end_year_id"] = average_years[id_][1]
        fixed_data[key].loc[filtered_data.index, :] = filtered_data
    return fixed_data


def infill_units(
    data: dict[str, pd.DataFrame],
    unit_columns: dict[str, list[str]],
    area_columns: dict[str, str],
    missing_values: list[str | int],
    unit_to_area_ratio: dict[str, float],
) -> dict[str, pd.DataFrame]:
    """infills missing units

    infills missing units based on the site area and average unit area
    ratio. All dicts should have the same keys

    Parameters
    ----------
    data : dict[str, pd.DataFrame]
        data to infill
    unit_columns : dict[str, list[str]]
        columns to infill
    area_columns : dict[str, str]
        columns with the areas to calculate the unit
    missing_values : list[str | int]
        values that should be infilled
    unit_area_ratio : dict[str, float]
        unit area ratio to calculate the area

    Returns
    -------
    dict[str, pd.DataFrame]
        infilled data
    """
    filtered_data = analyse.find_multiple_missing_values(
        data,
        unit_columns,
        dict((k, missing_values) for k in data.keys()),
    )
    area_columns_list = dict(
        (k, [area_columns[k]]) for k in area_columns.keys()
    )
    filtered_data_missing_area = analyse.find_multiple_missing_values(
        filtered_data,
        area_columns_list,
        dict((k, missing_values) for k in data.keys()),
    )

    fixed_data = {}
    for key, value in data.items():
        fixed_data[key] = value.copy()
        filtered_data_with_area = filtered_data[key].drop(
            index=filtered_data_missing_area[key].index
        )

        fixed_data[key].loc[
            filtered_data_with_area.index, unit_columns[key]
        ] = (
            fixed_data[key].loc[
                filtered_data_with_area.index, area_columns[key]
            ]
            * unit_to_area_ratio[key]
        )

    return fixed_data


def infill_missing_site_area(
    data: dict[str, pd.DataFrame],
    area_columns: dict[str, list[str]],
    missing_values: list[str | int],
    infill_area: dict[str, float],
) -> dict[str, pd.DataFrame]:
    """infills missing site area with the a specific value

    Parameters
    ----------
    data : dict[str, pd.DataFrame]
        data to infll
    area_columns : dict[str, list[str]]
        columns to infill
    missing_values : list[str | int]
        values to infill
    infill_area : dict[str, int]
        infill value

    Returns
    -------
    dict[str, pd.DataFrame]
        _description_
    """
    missing_area = analyse.find_multiple_missing_values(
        data,
        area_columns,
        dict((k, missing_values) for k in data.keys()),
    )

    fixed_data = {}
    for key, value in data.items():
        fixed_data[key] = value.copy()
        fixed_data[key].loc[
            missing_area[key].index, area_columns[key]
        ] = infill_area[key]

    return fixed_data


def calculate_average(
    data: dict[str, pd.DataFrame],
    columns: dict[str, list[str]],
    output_path: pathlib.Path,
    ) -> dict[str, float]:
    """calculate the mean value

    will calculate the total average across all the columns

    Parameters
    ----------
    data : dict[str, pd.DataFrame]
        data to analyse
    columns : dict[str, list[str]]
        columns to include within the average

    Returns
    -------
    dict[str, float]
        mean values
    """
    mean_values = {}
    for key, value in data.items():
        for column in columns[key]:
            mean_values[key] = value.loc[value["missing_area"]
                                         == False, column].mean()
            distribution_plots(value.loc[value["missing_area"] == False, column].to_numpy(
            ), key + " Site Area Distribution", output_path / (key+"_site_area_dist.png"))
    return mean_values


def old_incomplete_known_luc(
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
    out_of_date_codes = auxiliary_data.out_of_date_luc
    incomplete_codes = auxiliary_data.incomplete_luc
    known_invalid_codes = auxiliary_data.known_invalid_luc
    fixed_format = {}

    # inferances required values correspond to > 1 new code
    for key, value in data.items():
        fixed_format[key] = value.copy()
        for column in columns[key]:
            fixed_format[key][column] = value[column].apply(
                find_and_replace_luc,
                lookup_table=known_invalid_codes,
                find_column_name="known_invalid_code",
                replace_column_name="corrected_code",
            )
            fixed_format[key][column] = value[column].apply(
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


def fix_missing_lucs(
    data: dict[str, pd.DataFrame],
    columns: dict[str, list[str]],
    missing_values: list[str],
    fill_value: list[str],
) -> dict[str, pd.DataFrame]:
    """fills missing luc values

    currently fills the same value regardless of how the empty value is defined

    Parameters
    ----------
    data : dict[str, pd.DataFrame]
        data to be infilled
    columns : dict[str, str]
        columns to be infilled in data
    missing_values : list[str]
        defines missing values (empty value is automatically infilled)
    fill_value : list
        value to infill missing values with

    Returns
    -------
    dict[str, pd.DataFrame]
        infilled data
    """
    missing_values_lookup = pd.DataFrame([missing_values]).transpose()
    missing_values_lookup.columns = ["missing_values"]
    missing_values_lookup.loc[:, "fill_value"] = (
        pd.Series([fill_value])
        .repeat(len(missing_values_lookup))
        .reset_index(drop=True)
    )
    fixed_codes = {}
    for key, value in data.items():
        fixed_codes[key] = value.copy()
        for column in columns[key]:
            fixed_codes[key][column] = fixed_codes[key][column].apply(
                find_and_replace_luc,
                lookup_table=missing_values_lookup,
                find_column_name="missing_values",
                replace_column_name="fill_value",
                fill_empty_value=fill_value,
            )

    return fixed_codes


def fix_undefined_invalid_luc(
    data: dict[str, pd.DataFrame],
    columns: dict[str, list[str]],
    fill_value: list[str],
    auxiliary_data: global_classes.AuxiliaryData,
    filter_column_lookup: dict[str, str],
) -> dict[str, pd.DataFrame]:
    """infills undefined land use codes


    Parameters
    ----------
    data : dict[str, pd.DataFrame]
        data to infill
    columns : dict[str, list[str]]
        columns to infill
    fill_value : list[str]
        value to infill
    auxiliary_data : global_classes.AuxiliaryData
        auxiliary data from parser
    filter_column_lookup : dict[str, str]
        key = column, value = filter column indicating whether value in column is valid

    Returns
    -------
    dict[str, pd.DataFrame]
        infilled data
    """
    fixed_codes = {}
    valid_codes = auxiliary_data.allowed_codes["land_use_codes"]
    for key, value in data.items():
        fixed_codes[key] = value.copy()
        for column in columns[key]:
            # finds values that have not been defined as empty, infills and gives a warning
            existing_entries_other_issues = fixed_codes[key][fixed_codes[key
                ][filter_column_lookup[column]] == True]
            not_fixed = existing_entries_other_issues.loc[
                existing_entries_other_issues[column].apply(
                    lambda x: x != fill_value
                ),
                :,
            ]

            if len(not_fixed) != 0:
                not_fixed = analyse.find_invalid_land_use_codes(
                    not_fixed, valid_codes, [column]
                )
                if len(not_fixed) == 0:
                    continue

                LOG.warning(f"{len(not_fixed)} undefined invalid land use codes"
                            f" found in {key}, {column}:\n{not_fixed[column].to_list()}\n"
                            "Infilling with average land use split.")
                replacement = pd.Series([fill_value]).repeat(len(not_fixed))
                replacement.index = not_fixed.index
                fixed_codes[key].loc[
                    not_fixed.index, column
                ] = replacement
    return fixed_codes


def unit_area_ratio(
    data: dict[str, pd.DataFrame],
    unit_columns: dict[str, str],
    area_columns: dict[str, str],
    plot_path: pathlib.Path,
) -> float:
    """calculate the ratio for unit to area

    Only give identical units i.e. all dwelling or all floorspace

    Parameters
    ----------
    data : dict[str, pd.DataFrame]
        data to be analysed
    unit_columns : dict[str, str]
        columns with unit (e.g. total_dwelling for residental)
        for each sheet, same keys as data
    area_columns : dict[str, str]
        columns with site area for each sheet, same keys as data

    Returns
    -------
    float
        _description_
    """
    all_ratios = np.array([])
    for key, value in data.items():
        data_subset = value.loc[value["missing_area"] == False, :]
        data_subset = data_subset.loc[
            data_subset["missing_gfa_or_dwellings_with_site_area"]
            == False,
            :,
        ]
        # data subset only contains entries with site area and dwelling/floorspace
        all_ratios = np.append(
            all_ratios,
            (
                data_subset[unit_columns[key]]
                / data_subset[area_columns[key]]
            ),
        )
    distribution_plots(all_ratios, "Unit-Site Area Ratio Plot", plot_path)
    return all_ratios.mean()


def distribution_plots(data: np.ndarray, title: str, save_as: pathlib.Path) -> None:
    """create a Kernel Distribution Estimation plot for data

    plots KDE line and mean for data

    Parameters
    ----------
    data : np.ndarray
        data to plot
    title : str
        title given to plot
    save_as : pathlib.Path
        path to save plot to
    """

    fig, ax = plt.subplots()
    ax.set_title(title)
    #KDE plot
    sns.kdeplot(data, ax=ax, label="Kerbel Distribution Estimation")
    #calculate and ploy mean
    kdeline = ax.lines[0]
    xs = kdeline.get_xdata()
    ys = kdeline.get_ydata()
    mean = data.mean()
    height = np.interp(mean, xs, ys)
    ax.vlines(mean, 0, height, ls="--", label="Mean")

    ax.legend()
    fig.savefig(save_as)
    plt.close()


def find_and_replace_luc(
    luc_entry: list[str],
    lookup_table: pd.DataFrame,
    find_column_name: str,
    replace_column_name: str,
    fill_empty_value: Optional[list[str]] = None,
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
            if fill_empty_value is not None:
                if isinstance(fill_empty_value, str):
                    return [fill_empty_value]
                elif isinstance(fill_empty_value, list):
                    return fill_empty_value
                else:
                    LOG.warning(
                        f"fill_empty_value is a {type(fill_empty_value)},"
                        " that is neither a str or list[str]"
                    )
            else:
                return []
    except TypeError:
        return []
    for find_code in lookup_table[find_column_name]:
        if find_code not in luc_entry:
            continue
        replacement_code = lookup_table.loc[
            lookup_table[find_column_name] == find_code,
            replace_column_name,
        ].values[0]
        # deals with multiple of the same code
        for _ in range(luc_entry.count(find_code)):
            luc_entry.remove(find_code)

            if isinstance(replacement_code, str):
                luc_entry.append(replacement_code)
            elif isinstance(replacement_code, list):
                luc_entry = luc_entry + replacement_code
            else:
                LOG.warning(
                    f"{replace_column_name} contians {type(replacement_code)},"
                    " that is neither a str or list[str]"
                )
    return luc_entry


def fix_site_ref_id(
    data: dict[str, pd.DataFrame]
) -> dict[str, pd.DataFrame]:
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
        max_id_value = value.loc[
            value["missing_site_ref"] == False, "site_reference_id"
        ].max()
        if max_id_value > overall_max_id_value:
            overall_max_id_value = max_id_value

    for key, value in data.items():
        fixed_ids[key] = value.copy()
        missing_ids = value[value["missing_site_ref"] == True]

        if len(missing_ids) == 0:
            continue
        # calculate new ids & reset max id value
        new_ids = overall_max_id_value + np.arange(
            1, len(missing_ids) + 1, dtype=int
        )
        overall_max_id_value = new_ids.max()

        fixed_ids[key].loc[
            value["missing_site_ref"] == True, "site_reference_id"
        ] = new_ids
    return fixed_ids


def infill_year_units(
    data: pd.DataFrame,
    distribution_column: str,
    unit_column: str,
    unit_year_column: list[str],
    years_lookup: pd.DataFrame,
) -> pd.DataFrame:
    """infills build out profile

    calculates and infills build out profile from start & end years,
    total units and distribution (asumes infill years are 5 years apart)

    Parameters
    ----------
    data : pd.DataFrame
        data to infill
    distribution_column : str
        column that contains the distribution id
    unit_column : str
        column that contains total units
    unit_year_column : list[str]
        columns to infill
    years_lookup : pd.DataFrame
        years lookup from unit

    Returns
    -------
    pd.DataFrame
        infilled data

    Raises
    ------
    ValueError
        if any values have distribution IDs of 0 (not specified) or
        1 (specified - unable to calculate build out from this)
    """

    period = 5

    not_specified = data[data[distribution_column] == 0]
    years_defined = data[data[distribution_column] == 1]

    if len(not_specified) != 0 or len(years_defined) != 0:
        raise ValueError(
            "distrubtion contains not specified or defined years values")

    flat = data[data[distribution_column] == 2]
    flat_years = strip_year(
        flat["start_year_id"], flat["end_year_id"], years_lookup)
    early = data[data[distribution_column] == 3]
    early_years = strip_year(
        early["start_year_id"], early["end_year_id"], years_lookup)
    late = data[data[distribution_column] == 4]
    late_years = strip_year(
        late["start_year_id"], late["end_year_id"], years_lookup)
    mid = data[data[distribution_column] == 5]
    mid_years = strip_year(mid["start_year_id"],
                           mid["end_year_id"], years_lookup)

    for column in unit_year_column:
        year = int(column.split("_")[2])

        flat.loc[:, column] = flat_distribution(
            flat[unit_column], flat_years["start_year"], flat_years["end_year"], year, period)
        early.loc[:, column] = early_distribution(
            early[unit_column], early_years["start_year"], early_years["end_year"], year, period)
        late.loc[:, column] = late_distribution(
            late[unit_column], late_years["start_year"], late_years["end_year"], year, period)
        mid.loc[:, column] = mid_distribution(
            mid[unit_column], mid_years["start_year"], mid_years["end_year"], year, period)

    data.update(flat)
    data.update(early)
    data.update(late)
    data.update(mid)
    return data


def strip_year(
    start_year_id: pd.Series,
    end_year_id: pd.Series,
    years_lookup: pd.DataFrame,
    ) -> pd.DataFrame:
    """strips the integer years from the string

    returns a data frame returning either the start or end year

    Parameters
    ----------
    str_year : pd.Series
        years in start_year-end_year format

    start: bool
        True if start years have been inputted
        False if end years have been inputted

    Returns
    -------
    pd.DataFrame
        start and end years as integers
    """
    years_lookup = years_lookup["years"].str.split("-", expand=True)
    years_lookup.columns = ["start_year", "end_year"]
    start_year = start_year_id.to_frame().merge(years_lookup, how="left", left_on="start_year_id",
        right_index=True, suffixes=["", "_"]).drop(columns=["end_year"])
    end_year = end_year_id.to_frame().merge(years_lookup, how="left", left_on="end_year_id",
        right_index=True, suffixes=["", "_"]).drop(columns=["start_year"])
    years = pd.DataFrame([start_year["start_year"].astype(
        int), end_year["end_year"].astype(int)]).transpose()
    years.columns = ["start_year", "end_year"]
    return years


def flat_distribution(
    unit: pd.Series,
    start_year: pd.Series,
    end_year: pd.Series,
    year: int,
    period: int,
) -> pd.Series:
    """calculate the build out for a specific year for a flat distribution

    Calculate the build out for a year for flat distribution using start and end years
    for the development the year to be calculated and the period between calculated years

    Parameters
    ----------
    unit : pd.Series
        value to disaggregate into build out profile
    start_year : pd.Series
        start year of developmnet
    end_year : pd.Series
        end year of development
    year : int
        year to calculate build out profile for
    period : int
        time step (in years) between years in build out profile
        e.g. build out profile for 2001,2006,2011... would have 
        a period = 5

    Returns
    -------
    pd.Series
        build out for year given
    """
    after_start = year >= start_year
    before_end = year <= end_year
    within_years = pd.DataFrame(
        [after_start, before_end]).transpose().all(axis=1)
    unit_years = pd.Series(np.zeros(len(unit)), index=unit.index)
    periods = (end_year - start_year+1)/period
    unit_years[within_years] = unit[within_years]/periods[within_years]
    return unit_years


def early_distribution(
    unit: pd.Series,
    start_year: pd.Series,
    end_year: pd.Series,
    year: int,
    period: int,
) -> pd.Series:
    """calculate the build out for a specific year for a early distribution

    Calculate the build out for a year for early distribution using start and end years
    for the development the year to be calculated and the period between calculated years

    Parameters
    ----------
    unit : pd.Series
        value to disaggregate into build out profile
    start_year : pd.Series
        start year of developmnet
    end_year : pd.Series
        end year of development
    year : int
        year to calculate build out profile for
    period : int
        time step (in years) between years in build out profile
        e.g. build out profile for 2001,2006,2011... would have 
        a period = 5

    Returns
    -------
    pd.Series
        build out for year given
    """

    after_start = year >= start_year
    before_end = year <= end_year
    within_years = pd.DataFrame(
        [after_start, before_end]).transpose().all(axis=1)
    unit_years = pd.Series(np.zeros(len(unit)), index=unit.index)

    periods = (end_year - start_year+1)/period

    unit_years[within_years] = unit[within_years]*((periods[within_years]-(
        ((year-start_year[within_years])/5)+1)).apply(two_to_pow
        )/(periods[within_years].apply(two_to_pow)-1))
    return unit_years


def late_distribution(
    unit: pd.Series,
    start_year: pd.Series,
    end_year: pd.Series,
    year: int,
    period: int,
) -> pd.Series:
    """calculate the build out for a specific year for a late distribution

    Calculate the build out for a year for late distribution using start and end years
    for the development the year to be calculated and the period between calculated years

    Parameters
    ----------
    unit : pd.Series
        value to disaggregate into build out profile
    start_year : pd.Series
        start year of developmnet
    end_year : pd.Series
        end year of development
    year : int
        year to calculate build out profile for
    period : int
        time step (in years) between years in build out profile
        e.g. build out profile for 2001,2006,2011... would have
        a period = 5

    Returns
    -------
    pd.Series
        build out for year given
    """

    after_start = year >= start_year
    before_end = year <= end_year
    within_years = pd.DataFrame(
        [after_start, before_end]).transpose().all(axis=1)
    unit_years = pd.Series(np.zeros(len(unit)), index=unit.index)

    periods = (end_year - start_year+1)/period

    unit_years[within_years] = unit[within_years]*((periods[within_years]-(
        ((end_year[within_years]-(year+4))/5)+1))).apply(two_to_pow
        )/(periods[within_years].apply(two_to_pow)-1)
    return unit_years


def mid_distribution(
    unit: pd.Series,
    start_year: pd.Series,
    end_year: pd.Series,
    year: int,
    period: int,
) -> pd.Series:
    """calculate the build out for a specific year for a mid distribution

    Calculate the build out for a year for mid distribution using start and end years
    for the development the year to be calculated and the period between calculated years

    Parameters
    ----------
    unit : pd.Series
        value to disaggregate into build out profile
    start_year : pd.Series
        start year of developmnet
    end_year : pd.Series
        end year of development
    year : int
        year to calculate build out profile for
    period : int
        time step (in years) between years in build out profile
        e.g. build out profile for 2001,2006,2011... would have
        a period = 5

    Returns
    -------
    pd.Series
        build out for year given
    """

    after_start = year >= start_year
    before_end = year <= end_year
    within_years = pd.DataFrame(
        [after_start, before_end]).transpose().all(axis=1)
    unit_years = pd.Series(np.zeros(len(unit)), index=unit.index)

    periods = (end_year - start_year+1)/period

    determinator = 1 + (year - start_year)/period
    less_than_bool = determinator <= (periods+1)/2
    more_than_bool = ~less_than_bool

    less_than_bool = pd.DataFrame([less_than_bool.reset_index(
        drop=True), within_years.reset_index(drop=True)]).transpose().all(axis=1)
    more_than_bool = pd.DataFrame([more_than_bool.reset_index(
        drop=True), within_years.reset_index(drop=True)]).transpose().all(axis=1)
    less_than_bool.index = unit.index
    more_than_bool.index = unit.index

    unit_years[less_than_bool] = unit[
        less_than_bool]*((year-start_year[less_than_bool])/5).apply(two_to_pow)/(
            (((periods[less_than_bool]+1)/2).apply(np.floor).apply(two_to_pow)-1)+(
                ((periods[less_than_bool])/2).apply(np.floor).apply(two_to_pow)-1))

    unit_years[more_than_bool] = unit[more_than_bool]*((periods[more_than_bool]-(
        (year-start_year[more_than_bool])/5+1)).apply(two_to_pow)/(
            (((periods[more_than_bool]+1)/2).apply(np.floor).apply(two_to_pow)-1)+(
                ((periods[more_than_bool])/2).apply(np.floor).apply(two_to_pow)-1)))
    return unit_years


def two_to_pow(x: float):
    return 2**x
