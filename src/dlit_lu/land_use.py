"""Performs the conversion from D-Log formatting to the input to the Trip-Ends Module.
Conversion process involves disagregating by:
    land use code,
    aggregating by LSOA
    
    Residential:
        disagregate by dwelling type and convert to population
    Employment:
        convert to jobs and SIC codes

"""

# standard imports
import logging
import pathlib

# third party imports
import pandas as pd
import geopandas as gpd
import numpy as np

# local imports
from dlit_lu import summary, utilities, global_classes, parser, inputs, data_repair

# constants
LOG = logging.getLogger(__name__)


def run(input_data: global_classes.DLogData, config: inputs.DLitConfig):
    """runs process for converting DLOG to site and LSOA build out profiles

    disaggregaes mixed into employment and residential and land use codes
    applys dwelling types using land use split by LSOA
    rebases to LSOA build-out profiles

    Parameters
    ----------
    input_data : global_classes.DLogData
        data to perform conversion
    config : inputs.DLitConfig
        config file
    """
    if config.land_use is None:
        raise ValueError("cannot run land use without any land use parameters")

    LOG.info("Initialising Land Use Module")

    config.output_folder.mkdir(exist_ok=True)

    average_infill_values = inputs.InfillingAverages.load_yaml(
        config.output_folder / inputs.AVERAGE_INFILLING_VALUES_FILE
    )

    lsoa_hh_pop_column_names = [
        "lsoa2021_id",
        "dwelling_type",
        "household",
        "population",
        "pop_per_dwelling",
    ]
    lsoa_pop_by_tt_column_names = [
        "lsoa2021_id",
        "tt",
        "population",
    ]

    lsoa_jobs_column_names = [
        "sic_2d",
        "soc",
        "lsoa2021_id",
        "jobs",
    ]

    by_lsoa_hh_pop = pd.read_csv(
        config.land_use.lsoa_dwelling_pop_path, names=lsoa_hh_pop_column_names, header=0
    )
    by_lsoa_pop_by_tt = pd.read_csv(
        config.land_use.lsoa_traveller_type_path,
        names=lsoa_pop_by_tt_column_names,
        header=0,
    )
    by_lsoa_jobs = pd.read_csv(
        config.land_use.lsoa_jobs_path, names=lsoa_jobs_column_names, header=0
    )

    traveller_type_factor = gb_traveller_type_distribution(by_lsoa_pop_by_tt)
    lsoa_dwelling_ratio = calc_lsoa_proportion(by_lsoa_hh_pop)
    ratio_soc_over_sic = gb_soc_over_sic_distribution(by_lsoa_jobs)

    by_lsoa_data = tot_by_pop_dwel_emp(
        by_lsoa_hh_pop,
        by_lsoa_pop_by_tt,
        by_lsoa_jobs,
    )

    by_lsoa_data_file_name = "tot_by_data_out.csv"
    utilities.write_to_csv(config.output_folder / by_lsoa_data_file_name, by_lsoa_data)

    lsoa = parser.parse_zone(config.land_use.lsoa_shapefile_path)
    # msoa = parser.parse_msoa(config.land_use.msoa_shapefile_path)
    LOG.info("Disaggregating mixed into residential and employment")
    data = disagg_mixed(utilities.to_dict(input_data))

    build_out_columns = np.arange(2000, 2067, 1).tolist()
    build_out_columns = [str(year) for year in build_out_columns]

    LOG.info("Calulating build out profile for all years")

    data["residential"] = add_all_year_units(
        data["residential"],
        "res_distribution",
        "units_(dwellings)",
        build_out_columns,
        input_data.lookup.years,
    )
    data["employment"] = add_all_year_units(
        data["employment"],
        "emp_distribution",
        "units_(floorspace)",
        build_out_columns,
        input_data.lookup.years,
    )

    emp_redundant_columns = list(
        filter(
            lambda x: x.startswith("emp_year_"), data["employment"].columns.to_list()
        )
    )
    res_redundant_columns = list(
        filter(
            lambda x: x.startswith("res_year_"), data["residential"].columns.to_list()
        )
    )

    data["residential"].drop(columns=res_redundant_columns, inplace=True)
    data["employment"].drop(columns=emp_redundant_columns, inplace=True)

    LOG.info("Disaggregating employment proposed LUCs")
    construction_land_use_data = data.copy()
    construction_land_use_data["employment"] = disagg_land_use_codes(
        construction_land_use_data["employment"],
        "proposed_land_use",
        build_out_columns,
        input_data.proposed_land_use_split,
    )

    msg = "Demolitions calculated with dampener = %.2f"
    if config.land_use.demolition_dampener == 1:
        msg += ", i.e. no damping"
    elif config.land_use.demolition_dampener == 0:
        msg += ", i.e. no demolitions"
    LOG.info(msg, config.land_use.demolition_dampener)

    demolition_land_use_data: dict[str, pd.DataFrame] = {}
    for key, df in data.items():
        negative_sites = (df.loc[:, build_out_columns] < 0).any(axis=1).sum()
        if negative_sites > 0:
            LOG.info(
                "Explicit demolitions found on %s sites in %s data", negative_sites, key
            )

        demos = df.copy()
        demos.loc[:, build_out_columns] = demos.loc[:, build_out_columns].multiply(
            config.land_use.demolition_dampener
        )
        demolition_land_use_data[key] = demos

    demolition_land_use_data["residential"] = convert_to_gfa(
        demolition_land_use_data["residential"],
        "total_site_area_size_hectares",
        "units_(dwellings)",
        build_out_columns,
        average_infill_values.average_gfa_site_area_ratio,
    )

    LOG.info("Disaggregating employment existing LUCs")

    demolition_land_use_data["employment"] = disagg_land_use_codes(
        demolition_land_use_data["employment"],
        "existing_land_use",
        build_out_columns,
        input_data.existing_land_use_split,
    )
    LOG.info("Disaggregating residential existing LUCs")
    demolition_land_use_data["residential"] = disagg_land_use_codes(
        demolition_land_use_data["residential"],
        "existing_land_use",
        build_out_columns,
        input_data.existing_land_use_split,
    )

    demolition_land_use_data["residential"].loc[:, build_out_columns] = (
        -demolition_land_use_data["residential"].loc[:, build_out_columns]
    )
    demolition_land_use_data["employment"].loc[:, build_out_columns] = (
        -demolition_land_use_data["employment"].loc[:, build_out_columns]
    )

    demolition_land_use_data["residential"].columns = demolition_land_use_data[
        "employment"
    ].columns

    construction_land_use_data["employment"]["land_use"] = construction_land_use_data[
        "employment"
    ]["proposed_land_use"]
    demolition_land_use_data["employment"]["land_use"] = demolition_land_use_data[
        "employment"
    ]["existing_land_use"]
    demolition_land_use_data["residential"]["land_use"] = demolition_land_use_data[
        "residential"
    ]["existing_land_use"]

    residential_build_out = construction_land_use_data["residential"]
    employment_build_out = pd.concat(
        [
            construction_land_use_data["employment"],
            demolition_land_use_data["employment"],
            demolition_land_use_data["residential"],
        ],
        ignore_index=True,
    )

    # # Site to zone
    # LOG.info("performing zone geospatial lookup")
    # res_zone_sites = zone_site_geospatial_lookup(residential_build_out, zone)
    # emp_zone_sites = zone_site_geospatial_lookup(employment_build_out, zone)

    res_sites = residential_build_out.loc[
        :,
        ["site_reference_id", "web_tag_certainty", "easting", "northing"]
        + build_out_columns,
    ]

    emp_sites = employment_build_out.loc[
        :,
        build_out_columns
        + ["site_reference_id", "web_tag_certainty", "land_use", "easting", "northing"],
    ]

    LOG.info("performing LSOA geospatial lookup")
    res_lsoa_sites = lsoa_site_geospatial_lookup(residential_build_out, lsoa)
    res_lsoa_sites = res_lsoa_sites.loc[
        :,
        build_out_columns + ["site_reference_id", "easting", "northing", "LSOA21CD"],
    ]
    emp_lsoa_sites = lsoa_site_geospatial_lookup(employment_build_out, lsoa)
    emp_lsoa_sites = emp_lsoa_sites.loc[
        :,
        build_out_columns
        + ["site_reference_id", "easting", "northing", "land_use", "LSOA21CD"],
    ]
    # compare_existing_proposed_dwellings_zone(
    #     zone_data,
    #     res_zone_sites,
    #     build_out_columns,
    #     comparison_path / "existing_proposed_dwelling_comparison_zone.csv",
    # )

    LOG.info("Export site level dwelling and floorspace data")

    res_sites_uncertainty = res_sites.groupby(
        ["site_reference_id", "web_tag_certainty", "easting", "northing"]
    ).sum()

    res_sites_tot = res_sites.groupby(
        ["site_reference_id", "easting", "northing"]
    ).sum()

    emp_sites_uncerntainty = emp_sites.groupby(
        ["site_reference_id", "web_tag_certainty", "land_use", "easting", "northing"]
    ).sum()
    res_sites_tot_file = (
        "dwelling_sites_build_out.csv"  # inputs needed by module dev_pattern
    )
    res_sites_uncertainty_file = "dwelling_sites_uncertainty_build_out.csv"
    emp_sites_uncertainty_file = "floorspace_sites_uncerntainty_build_out.csv"

    utilities.write_to_csv(config.output_folder / res_sites_tot_file, res_sites_tot)

    # Files needed by LUTI
    luti_output_path = config.output_folder / "LUTI_outputs"
    luti_output_path.mkdir(exist_ok=True)
    utilities.write_to_csv(
        luti_output_path / res_sites_uncertainty_file, res_sites_uncertainty
    )
    utilities.write_to_csv(
        luti_output_path / emp_sites_uncertainty_file, emp_sites_uncerntainty
    )

    LOG.info("Convert site development to jobs")
    emp_lsoa_sites = emp_lsoa_sites.rename(
        columns={"LSOA21CD": "lsoa2021_id"}, inplace=True
    )
    emp_lsoa_sites.set_index(
        ["site_reference_id", "easting", "northing", "lsoa2021_id", "land_use"],
        inplace=True,
    )
    emp_lsoa_sites = convert_gfa_to_jobs_site(
        emp_lsoa_sites,
        config.land_use.employment_density_matrix_path,
        build_out_columns,
    )
    emp_lsoa_sites = convert_luc_to_sic_site(
        emp_lsoa_sites, config.land_use.luc_sic_conversion_path
    )
    emp_lsoa_sites = emp_lsoa_sites.rename(columns={"sic_code": "sic_2d"}, inplace=True)
    emp_lsoa_sites = apply_soc_over_sic_ratio(
        emp_lsoa_sites, build_out_columns, ratio_soc_over_sic
    )

    # LOG.info("Compare total new jobs to existing")
    # compare_existing_proposed_jobs_zone(
    #     zone_data,
    #     emp_zone_sites,
    #     build_out_columns,
    #     comparison_path / "existing_proposed_jobs_comparison_zone.csv",
    # )

    LOG.info("Export site level job data")
    emp_lsoa_sites_segmented = emp_lsoa_sites.groupby(
        ["site_reference_id", "easting", "northing", "lsoa2021_id", "sic_2d", "soc"]
    ).sum()
    emp_sites_tot = emp_lsoa_sites.groupby(
        ["site_reference_id", "easting", "northing"]
    ).sum()

    emp_lsoa_sites_segmented_file = "jobs_segmented_sites_lsoa_build_out.csv.bz2"
    emp_sites_tot_file = "jobs_sites_build_out.csv"

    utilities.write_to_csv(
        config.output_folder / emp_lsoa_sites_segmented_file, emp_lsoa_sites_segmented
    )  # inputs needed by module dev_pattern
    utilities.write_to_csv(
        config.output_folder / emp_sites_tot_file, emp_sites_tot
    )  # inputs needed by module dev_pattern

    LOG.info("Disaggregating dwellings into population by dwelling type")
    res_lsoa_sites_pop = disagg_dwelling(
        res_lsoa_sites,
        lsoa_dwelling_ratio,
        build_out_columns,
    )
    res_lsoa_sites_pop = res_lsoa_sites_pop.loc[
        :,
        build_out_columns
        + ["site_reference_id", "easting", "northing", "lsoa2021_id", "dwelling_type"],
    ]

    LOG.info("Disaggregating site level population furhter by traveller type")
    res_lsoa_sites_pop = res_lsoa_sites_pop.groupby(
        ["site_reference_id", "easting", "northing", "lsoa2021_id"]
    ).sum()

    res_lsoa_sites_pop_segmented = apply_pop_land_use(
        res_lsoa_sites_pop, build_out_columns, traveller_type_factor
    )
    res_lsoa_sites_pop_segmented = res_lsoa_sites_pop_segmented.groupby(
        ["site_reference_id", "easting", "northing", "lsoa2021_id", "tt"]
    ).sum()

    LOG.info("Export site level population data")
    res_lsoa_sites_pop_segmented_file = (
        "population_segmented_sites_lsoa_build_out.csv.bz2"
    )
    res_lsoa_sites_pop_tot_file = "population_sites_build_out.csv.bz2"
    utilities.write_to_csv(
        config.output_folder / res_lsoa_sites_pop_segmented_file,
        res_lsoa_sites_pop_segmented,
    )  # inputs needed by module dev_pattern
    utilities.write_to_csv(
        config.output_folder / res_lsoa_sites_pop_tot_file, res_lsoa_sites_pop
    )

    # LOG.info("Compare total units of new dwelling to existing")
    # comparison_path = config.output_folder / "existing_proposed_development_comparison"

    # comparison_path.mkdir(exist_ok=True)

    # compare_existing_proposed_dwellings(
    #     lsoa_dwelling_ratio,
    #     res_lsoa_sites,
    #     build_out_columns,
    #     comparison_path / "existing_proposed_dwelling_comparison.csv",
    # )

    # res_lsoa_sites_pop = res_lsoa_sites_pop.loc[
    #     :, build_out_columns + ["LSOA21CD", "dwelling_type"]
    # ]
    # emp_lsoa_sites = emp_lsoa_sites.loc[:, build_out_columns + ["LSOA21CD", "land_use"]]

    # LOG.info("Rebasing to LSOA and Land use")

    # # res_lsoa_base = res_lsoa_sites.groupby(["LSOA21CD", "dwelling_type"]).sum()
    # res_lsoa_base = res_lsoa_sites_pop.groupby(["LSOA21CD"]).sum()
    # emp_lsoa_base = emp_lsoa_sites.groupby(["LSOA21CD", "land_use"]).sum()

    # LOG.info("Disaggregating by traveller type")
    # res_lsoa_base = apply_pop_land_use(
    #     res_lsoa_base, build_out_columns, traveller_type_factor
    # )

    LOG.info("Check total yearly dwelling and population")

    year_tot_dwel = res_sites_tot.sum(axis=0)

    year_tot_pop = res_lsoa_sites_pop.sum(axis=0)

    year_tot_df = pd.DataFrame(
        {"year_tot_dwel": year_tot_dwel, "year_tot_pop": year_tot_pop}
    )
    year_tot_df_file_name = "year_totals.csv"
    utilities.write_to_csv(config.output_folder / year_tot_df_file_name, year_tot_df)

    # LOG.info("Rename LSOA column id")
    # # rename columns
    # res_lsoa_base.reset_index(drop=False, inplace=True)
    # emp_lsoa_base.reset_index(drop=False, inplace=True)
    # res_lsoa_base.rename(
    #     columns={"LSOA21CD": "lsoa2021_id"}, inplace=True
    # )  # lsoa_zone_id
    # emp_lsoa_base.rename(
    #     columns={"LSOA21CD": "lsoa2021_id"}, inplace=True
    # )  # lsoa_zone_id
    # res_lsoa_base.set_index(["lsoa2021_id", "tt"], inplace=True)
    # emp_lsoa_base.set_index(["lsoa2021_id", "land_use"], inplace=True)

    # LOG.info("Converting GFA to jobs")
    # emp_lsoa_base = convert_gfa_to_jobs(
    #     emp_lsoa_base, config.land_use.employment_density_matrix_path, build_out_columns
    # )
    # emp_lsoa_base = convert_luc_to_sic(
    #     emp_lsoa_base, config.land_use.luc_sic_conversion_path
    # )

    # compare_existing_proposed_jobs(
    #     lsoa_jobs,
    #     emp_lsoa_base,
    #     build_out_columns,
    #     comparison_path / "existing_proposed_jobs_comparison.csv",
    # )

    # LOG.info("Writing Land Use disaggregation and geospatial lookup results")

    # res_file_name = "residential_lsoa_build_out.csv.bz2"
    # emp_file_name = "employment_lsoa_build_out.csv.bz2"

    # utilities.write_to_csv(config.output_folder / res_file_name, res_lsoa_base)
    # utilities.write_to_csv(config.output_folder / emp_file_name, emp_lsoa_base)

    # print("population data generated by d-log:", res_lsoa_base)
    # print("employment data generated by d-log:", emp_lsoa_base)
    # if config.land_use.summary_data is not None:
    #     summary.summarise_landuse(
    #         res_msoa_base,
    #         emp_msoa_base,
    #         config.land_use.summary_data,
    #         config.output_folder / "land_use_summaries",
    #     )

    LOG.info("Ending Land Use Module")


def compare_existing_proposed_jobs(
    existing_data: pd.DataFrame,
    proposed_data: pd.DataFrame,
    build_out_profile_cols: list[str],
    file_path: pathlib.Path,
) -> None:
    """compares the number of existing jobsto the number of proposed jobs

    outputs a comparion the number of existing jobs from an external input,
    to the number of proposed jobs infered from the D-Log

    Parameters
    ----------
    existing_data : pd.DataFrame
        data containing the number of existing jobs (TfN land use data)
    proposed_data : pd.DataFrame
        data containing the number of proposed jobs (from D-Log)
    build_out_profile_cols : list[str]
        build-out profiles columns
    file_path : pathlib.Path
        path to save comparison output
    """
    existing_jobs = (existing_data.groupby("lsoa21_id")["jobs"].sum()).to_frame(
        name="total_existing_jobs"
    )
    proposed_data["total_proposed_jobs"] = proposed_data[build_out_profile_cols].sum(
        axis=1
    )
    proposed_jobs = proposed_data.groupby("lsoa2021_id")["total_proposed_jobs"].sum()
    comparison = existing_jobs.merge(
        proposed_jobs, how="outer", left_index=True, right_index=True
    )
    comparison["ratio (percentage)"] = (
        100 * comparison["total_proposed_jobs"] / comparison["total_existing_jobs"]
    )
    utilities.write_to_csv(file_path, comparison)


def compare_existing_proposed_jobs_zone(
    existing_data: pd.DataFrame,
    proposed_data: pd.DataFrame,
    build_out_profile_cols: list[str],
    file_path: pathlib.Path,
) -> None:
    """compares the number of existing jobsto the number of proposed jobs

    outputs a comparion the number of existing jobs from an external input,
    to the number of proposed jobs infered from the D-Log

    Parameters
    ----------
    existing_data : pd.DataFrame
        data containing the number of existing jobs (TfN land use data)
    proposed_data : pd.DataFrame
        data containing the number of proposed jobs (from D-Log)
    build_out_profile_cols : list[str]
        build-out profiles columns
    file_path : pathlib.Path
        path to save comparison output
    """

    existing_jobs = existing_data.rename({"emp": "total_existing_jobs"}, axis=1).drop(
        columns=["pop", "households"]
    )

    proposed_data["total_proposed_jobs"] = proposed_data[build_out_profile_cols].sum(
        axis=1
    )
    proposed_jobs = proposed_data.groupby("normits_id")["total_proposed_jobs"].sum()
    comparison = existing_jobs.merge(proposed_jobs, how="outer", on="normits_id")
    comparison["ratio (percentage)"] = (
        100 * comparison["total_proposed_jobs"] / comparison["total_existing_jobs"]
    )
    utilities.write_to_csv(file_path, comparison)


def gb_traveller_type_distribution(data: pd.DataFrame) -> pd.DataFrame:
    """calculates the factors for each traveller type

    aggregates across all zones and dwelling types

    Parameters
    ----------
    data: pd.DataFrame
        TfN base year population land use

    Returns
    -------
    pd.DataFrame
        contains factors for lsoa traveller type
    """
    agg_zones = data.groupby("tt").sum()
    ratios = (agg_zones["population"] / agg_zones["population"].sum()).reset_index(
        drop=False
    )
    lsoa_ratios = []
    for id_ in data["lsoa2021_id"].unique():
        temp = ratios.copy()
        temp["lsoa2021_id"] = (
            pd.Series([id_]).repeat(len(ratios)).reset_index(drop=True)
        )
        lsoa_ratios.append(temp)
    all_lsoa_ratios = pd.concat(lsoa_ratios, axis=0).set_index(["lsoa2021_id", "tt"])
    all_lsoa_ratios.columns = ["ratios"]
    return all_lsoa_ratios


# def lad_traveller_type_distribution(file_path: pathlib.Path, lookup_path: pathlib.Path) -> pd.DataFrame:
#     """calculates the factors for each traveller type

#     aggregates across all zones and dwelling types

#     Parameters
#     ----------
#     file_path : pathlib.Path
#         file path to TfN population land use

#     Returns
#     -------
#     pd.DataFrame
#         contains factors for lsoa traveller type
#     """
#     data = pd.read_csv(file_path)
#     lookup = pd.read_csv(lookup_path)
#     data = data.merge(lookup, left_on="LSOA", right_on="lsoa2021_id", how="left")
#     data =
#     agg_zones = data.groupby("tt").sum()
#     ratios = (agg_zones["pop"] / agg_zones["pop"].sum()).reset_index(drop=False)
#     lsoa_ratios = []
#     for id_ in data["LSOA"].unique():
#         temp = ratios.copy()
#         temp["LSOA"] = pd.Series([id_]).repeat(len(ratios)).reset_index(drop=True)
#         lsoa_ratios.append(temp)
#     all_lsoa_ratios = pd.concat(lsoa_ratios, axis=0).set_index(["LSOA", "tt"])
#     all_lsoa_ratios.columns = ["ratios"]
#     return all_lsoa_ratios


def gb_soc_over_sic_distribution(data: pd.DataFrame) -> pd.DataFrame:
    """calculates the factors for each traveller type

    aggregates across all zones and dwelling types

    Parameters
    ----------
    data: pd.DataFrame
        TfN base year employment data
    Returns
    -------
    pd.DataFrame
        contains factors for lsoa traveller type
    """
    jobs_sic_soc = (
        data.groupby(["sic_2d", "soc"], as_index=False)
        .sum()
        .rename(columns={"jobs": "jobs_sic_soc"})
    )
    jobs_sic = (
        data.groupby("sic_2d", as_index=False)
        .sum()
        .rename(columns={"jobs": "jobs_sic"})
    )
    ratios = jobs_sic_soc.merge(jobs_sic, on="sic_2d", how="left")
    ratios["ratio_soc_over_sic"] = ratios["jobs_sic_soc"] / ratios["jobs_sic"]

    ratios = ratios[["sic_2d", "soc", "ratio_soc_over_sic"]]

    lsoa_sic_soc = data[
        ["lsoa2021_id", "sic_2d", "soc"]
    ].drop_duplicates()  # Unique (lsoa, sic_2d, soc) combinations
    lsoa_ratios = lsoa_sic_soc.merge(
        ratios, on=["sic_2d", "soc"], how="left"
    )  # Assign correct ratios
    # lsoa_ratios = []
    # for id_ in data["lsoa2021_id"].unique():
    #     temp = ratios.copy()
    #     temp["lsoa2021_id"] = (
    #         pd.Series([id_]).repeat(len(ratios)).reset_index(drop=True)
    #     )
    #     lsoa_ratios.append(temp)
    # all_lsoa_ratios = pd.concat(lsoa_ratios, axis=0).set_index(
    #     ["lsoa2021_id", "sic_2d", "soc"]
    # )
    # all_lsoa_ratios.columns = ["ratio_soc_over_sic"]
    return lsoa_ratios


def apply_soc_over_sic_ratio(
    data: pd.DataFrame,
    unit_columns: list[str],
    ratio_soc_over_sic: pd.DataFrame,
) -> pd.DataFrame:
    """applies TfN population land use factors to data

    Parameters
    ----------
    data : pd.DataFrame
        data containing the unit values
    unit_columns : list[str]
        list of column names in data that contain the unit values to be updated
    ratio_soc_over_sic : pd.DataFrame
        dataframe containing the TfN employment factors

    Returns
    -------
    pd.DataFrame

    """
    data_ratios = (
        data.reset_index(drop=False)
        .merge(
            ratio_soc_over_sic.reset_index(drop=False),
            on=["lsoa2021_id", "sic_2d"],
        )
        .set_index(
            ["site_reference_id", "easting", "northing", "lsoa2021_id", "sic_2d", "soc"]
        )
    )
    data_ratios = data_ratios.loc[:, unit_columns].multiply(
        data_ratios["ratio_soc_over_sic"], axis=0
    )
    return data_ratios


def apply_pop_land_use(
    data: pd.DataFrame,
    unit_columns: list[str],
    tt_factors: pd.DataFrame,
) -> pd.DataFrame:
    """applies TfN population land use factors to data

    Parameters
    ----------
    data : pd.DataFrame
        data containing the unit values
    unit_columns : list[str]
        list of column names in data that contain the unit values to be updated
    tt_factors : pd.DataFrame
        dataframe containing the TfN population land use factors

    Returns
    -------
    pd.DataFrame
        dataframe with updated unit values, indexed by lsoa11cd,
        dwelling_type, and tfn_traveller_type
    """

    data_ratios = (
        data.reset_index(drop=False)
        .merge(
            tt_factors.reset_index(drop=False),
            on="lsoa2021_id",
        )
        .set_index(["site_reference_id", "easting", "northing", "lsoa2021_id", "tt"])
    )
    data_ratios = data_ratios.loc[:, unit_columns].multiply(
        data_ratios["ratios"], axis=0
    )
    return data_ratios


def tot_by_pop_dwel_emp(
    hh_pop_data: pd.DataFrame,
    pop_data: pd.DataFrame,
    emp_data: pd.DataFrame,
) -> pd.DataFrame:
    """
    Processes household, population, and employment data, then get lsoa total.

    Parameters
    ----------
    hh_pop_data: pd.DataFrame,
        the household and population dataset.
    pop_data: pd.DataFrame,
        the population dataset.
    emp_data: pd.DataFrame
        the employment dataset.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing aggregated household, population, and employment data,
        merged on the LSOA zone identifier.
    """

    hh_pop_data = hh_pop_data.groupby("lsoa2021_id", as_index=False).agg(
        {
            "household": "sum",
            # "population": "sum"
        }
    )

    pop_data = pop_data.groupby("lsoa2021_id", as_index=False).agg(
        {"population": "sum"}
    )

    emp_data = emp_data.groupby("lsoa2021_id", as_index=False).agg({"jobs": "sum"})

    # Merge datasets
    all_data = hh_pop_data.merge(pop_data, on="lsoa2021_id", how="left")
    all_data = all_data.merge(emp_data, on="lsoa2021_id", how="left")
    all_data.set_index(["lsoa2021_id"], inplace=True)
    return all_data


def compare_existing_proposed_dwellings(
    exisiting_data: pd.DataFrame,
    proposed_data: pd.DataFrame,
    build_out_profile_cols: list[str],
    file_path: pathlib.Path,
) -> None:
    """produces a comparison of existing and proposed dwelling types

    outputs a csvfile at a defined location with the total existing and proposed
    dwellings by lsoa. existing jobs are taken from a defined external data
    source (TfN landuse)

    Parameters
    ----------
    exisiting_data : pd.DataFrame
        TfN land use data contain the number of dwellings by lsoa
    proposed_data : pd.DataFrame
        Dlog data
    build_out_profile_cols : list[str]
        build-out profile data in proposed data
    file_path : pathlib.Path
        path to save outputted csv
    """
    existing_dwellings = (
        exisiting_data.groupby("lsoa2021_id")["household"].sum()
    ).to_frame(name="total_existing_dwellings")
    proposed_data["total_proposed_dwellings"] = proposed_data[
        build_out_profile_cols
    ].sum(axis=1)
    proposed_dwellings = proposed_data.groupby("LSOA21CD")[
        "total_proposed_dwellings"
    ].sum()
    comparison = existing_dwellings.merge(
        proposed_dwellings, how="outer", left_index=True, right_index=True
    )
    comparison["ratio (percentage)"] = (
        100
        * comparison["total_proposed_dwellings"]
        / comparison["total_existing_dwellings"]
    )
    utilities.write_to_csv(file_path, comparison)


# create zonal comparison
def compare_existing_proposed_dwellings_zone(
    existing_data: pd.DataFrame,
    proposed_data: pd.DataFrame,
    build_out_profile_cols: list[str],
    file_path: pathlib.Path,
) -> None:
    """produces a comparison of existing and proposed dwelling types

    outputs a csvfile at a defined location with the total existing and proposed
    dwellings by zone. existing jobs are taken from a defined external data
    source (TfN landuse)

    Parameters
    ----------
    existing_data : pd.DataFrame
        TfN land use data contain the number of dwellings by zone
    proposed_data : pd.DataFrame
        Dlog data
    build_out_profile_cols : list[str]
        build-out profile data in proposed data
    file_path : pathlib.Path
        path to save outputted csv
    """
    existing_dwellings = existing_data.rename(
        {"households": "total_existing_dwellings"}, axis=1
    ).drop(columns=["pop", "emp"])

    proposed_data["total_proposed_dwellings"] = proposed_data[
        build_out_profile_cols
    ].sum(axis=1)
    proposed_dwellings = proposed_data.groupby("normits_id")[
        "total_proposed_dwellings"
    ].sum()
    comparison = existing_dwellings.merge(
        proposed_dwellings, how="outer", on="normits_id"
    )
    comparison["ratio (percentage)"] = (
        100
        * comparison["total_proposed_dwellings"]
        / comparison["total_existing_dwellings"]
    )

    utilities.write_to_csv(file_path, comparison)


def convert_to_gfa(
    data: pd.DataFrame,
    area_col: str,
    unit_col: str,
    unit_year_columns: list[str],
    factor: float,
) -> pd.DataFrame:
    """Converts dwellings to GFA

    uses site area and factor to calculate total GFA and
    distributes the build out profile in the orginal ratio

    Parameters
    ----------
    data : pd.DataFrame
        DataFrame containing the data to be converted
    area_col : str
        Column name for the area data
    unit_col : str
        Column name for the unit data
    unit_year_columns : list[str]
        List of column names for the year unit data
    factor : float
        Conversion factor to use in converting the data

    Returns
    -------
    pd.DataFrame
        DataFrame with the converted data
    """
    data_to_gfa = data.copy()
    data_to_gfa.loc[:, unit_col] = data_to_gfa[area_col] * factor
    data_to_gfa.loc[:, unit_year_columns] = (
        data.loc[:, unit_year_columns]
        .divide(data.loc[:, unit_col], axis=0)
        .multiply(data_to_gfa[unit_col], axis=0)
    )
    return data_to_gfa


def convert_gfa_to_jobs(
    data: pd.DataFrame, matrix_path: pathlib.Path, unit_cols
) -> pd.DataFrame:
    """Converts GFA build-out profile to jobs

    Parameters
    ----------
    data : pd.DataFrame
        DataFrame with GFA build out profiles
    matrix_path : pathlib.Path
        Path to the job density matrix
    unit_cols : list[str]
        Columns in the data that contain build-out profile data

    Returns
    -------
    pd.DataFrame
        DataFrame containing job build-out profiles
    """
    matrix = pd.read_csv(matrix_path).loc[:, ["land_use_code", "fte_floorspace"]]
    matrix.loc[:, "land_use_code"] = matrix["land_use_code"].str.lower()
    data_jobs = data.reset_index().merge(
        matrix, how="left", left_on="land_use", right_on="land_use_code"
    )
    data_jobs.loc[:, unit_cols] = data_jobs.loc[:, unit_cols].divide(
        data_jobs.loc[:, "fte_floorspace"], axis=0
    )
    data_jobs.loc[data_jobs["fte_floorspace"].isnull(), unit_cols] = 0
    has_jobs = (
        ~pd.DataFrame([data_jobs[col] == 0 for col in unit_cols])
        .transpose()
        .all(axis=1)
    )
    data_jobs.drop(columns=["fte_floorspace", "land_use_code"], inplace=True)
    data_jobs = data_jobs[has_jobs]
    data_jobs.set_index(["lsoa2021_id", "land_use"], inplace=True)
    return data_jobs


def convert_gfa_to_jobs_site(
    data: pd.DataFrame, matrix_path: pathlib.Path, unit_cols
) -> pd.DataFrame:
    """Converts GFA build-out profile to jobs

    Parameters
    ----------
    data : pd.DataFrame
        DataFrame with GFA build out profiles
    matrix_path : pathlib.Path
        Path to the job density matrix
    unit_cols : list[str]
        Columns in the data that contain build-out profile data

    Returns
    -------
    pd.DataFrame
        DataFrame containing job build-out profiles
    """
    matrix = pd.read_csv(matrix_path).loc[:, ["land_use_code", "fte_floorspace"]]
    matrix.loc[:, "land_use_code"] = matrix["land_use_code"].str.lower()
    data_jobs = data.reset_index().merge(
        matrix, how="left", left_on="land_use", right_on="land_use_code"
    )
    data_jobs.loc[:, unit_cols] = data_jobs.loc[:, unit_cols].divide(
        data_jobs.loc[:, "fte_floorspace"], axis=0
    )
    data_jobs.loc[data_jobs["fte_floorspace"].isnull(), unit_cols] = 0
    has_jobs = (
        ~pd.DataFrame([data_jobs[col] == 0 for col in unit_cols])
        .transpose()
        .all(axis=1)
    )
    data_jobs.drop(columns=["fte_floorspace", "land_use_code"], inplace=True)
    data_jobs = data_jobs[has_jobs]
    data_jobs.set_index(
        ["site_reference_id", "easting", "northing", "lsoa2021_id", "land_use"],
        inplace=True,
    )
    return data_jobs


def convert_luc_to_sic(
    data: pd.DataFrame, conversion_path: pathlib.Path
) -> pd.DataFrame:
    """Convert the land use codes (LUC) to standard industrial classification (SIC) codes.

    Parameters
    ----------
    data : pd.DataFrame
        The DataFrame containing the land use codes.
    conversion_path : pathlib.Path
        The path to a csv file that maps the LUC codes to the SIC codes.

    Returns
    -------
    pd.DataFrame
        The DataFrame with the SIC codes

    """
    conversion = pd.read_csv(conversion_path).loc[:, ["land_use_code", "sic_code"]]
    conversion["land_use_code"] = conversion["land_use_code"].str.lower()
    data_sic_code = data.reset_index(drop=False).merge(
        conversion,
        how="left",
        left_on="land_use",
        right_on="land_use_code",
    )
    data_sic_code.drop(columns=["land_use_code", "land_use"], inplace=True)
    data_sic_code.set_index(["lsoa2021_id", "sic_code"], inplace=True)
    return data_sic_code


def convert_luc_to_sic_site(
    data: pd.DataFrame, conversion_path: pathlib.Path
) -> pd.DataFrame:
    """Convert the land use codes (LUC) to standard industrial classification (SIC) codes.

    Parameters
    ----------
    data : pd.DataFrame
        The DataFrame containing the land use codes.
    conversion_path : pathlib.Path
        The path to a csv file that maps the LUC codes to the SIC codes.

    Returns
    -------
    pd.DataFrame
        The DataFrame with the SIC codes

    """
    conversion = pd.read_csv(conversion_path).loc[:, ["land_use_code", "sic_code"]]
    conversion["land_use_code"] = conversion["land_use_code"].str.lower()
    data_sic_code = data.reset_index(drop=False).merge(
        conversion,
        how="left",
        left_on="land_use",
        right_on="land_use_code",
    )
    data_sic_code.drop(columns=["land_use_code", "land_use"], inplace=True)
    data_sic_code.set_index(
        ["site_reference_id", "easting", "northing", "lsoa2021_id", "sic_code"],
        inplace=True,
    )
    return data_sic_code


def add_all_year_units(
    data: pd.DataFrame,
    distribution_column: str,
    unit_column: str,
    unit_year_column: list[str],
    years_lookup: pd.DataFrame,
) -> pd.DataFrame:
    """create a build out profile for any consecutive years

    calculates a build out profile for consecutive years defined in unit year columns.
    adds them as new columns to the inputted data. assunes infill years are 1 year apart.

    Parameters
    ----------
    data : pd.DataFrame
        data to produce build out profile
    distribution_column : str
        column that contains the distribution ID
    unit_column : str
        column that contains unit to disagregate build out profile
    unit_year_column : list[int]
        columns to produce build out year. must be in 4 digit year format and
        passed as strings
    years_lookup : pd.DataFrame
        years lookup table from DLog

    Returns
    -------
    pd.DataFrame:
        data with new build out profile calculated

    Raises
    ------
    ValueError
        if any values have distribution IDs of 0 (not specified) or
        1 (specified - unable to calculate build out from this)
    """

    period = 1

    not_specified = data[data[distribution_column] == 0]
    years_defined = data[data[distribution_column] == 1]

    if len(not_specified) != 0 or len(years_defined) != 0:
        raise ValueError("distrubtion contains not specified or defined years values")

    flat = data[data[distribution_column] == 2]
    flat_years = data_repair.strip_year(
        flat["start_year_id"], flat["end_year_id"], years_lookup
    )
    early = data[data[distribution_column] == 3]
    early_years = data_repair.strip_year(
        early["start_year_id"], early["end_year_id"], years_lookup
    )
    late = data[data[distribution_column] == 4]
    late_years = data_repair.strip_year(
        late["start_year_id"], late["end_year_id"], years_lookup
    )
    mid = data[data[distribution_column] == 5]
    mid_years = data_repair.strip_year(
        mid["start_year_id"], mid["end_year_id"], years_lookup
    )

    for column in unit_year_column:
        year = int(column)
        flat.loc[:, column] = data_repair.flat_distribution(
            flat[unit_column],
            flat_years["start_year"],
            flat_years["end_year"],
            year,
            period,
        )
        early.loc[:, column] = data_repair.early_distribution(
            early[unit_column],
            early_years["start_year"],
            early_years["end_year"],
            year,
            period,
        )
        late.loc[:, column] = data_repair.late_distribution(
            late[unit_column],
            late_years["start_year"],
            late_years["end_year"],
            year,
            period,
        )
        mid.loc[:, column] = data_repair.mid_distribution(
            mid[unit_column],
            mid_years["start_year"],
            mid_years["end_year"],
            year,
            period,
        )

    updated_data = pd.concat([flat, early, late, mid])

    return updated_data


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

    mix_res.loc[:, "total_site_area_size_hectares"] = mix_res["total_area_ha"]
    # this does not feel right, it should be the other way around
    # mix_emp.loc[:, "total_area_ha"] = mix_emp["site_area_ha"]
    mix_emp.loc[:, "site_area_ha"] = mix_emp["total_area_ha"]

    res_new = pd.concat([res, mix_res], ignore_index=True)
    emp_new = pd.concat([emp, mix_emp], ignore_index=True)

    return {"residential": res_new, "employment": emp_new}


def disagg_dwelling(
    data: pd.DataFrame,
    lsoa_ratio: pd.DataFrame,
    unit_columns: list[str],
) -> pd.DataFrame:
    """_summary_

    _extended_summary_

    Parameters
    ----------
    data : pd.DataFrame
        DataFrame containing residential dwelling data
    lsoa_ratio : pd.DataFrame
        contains the existing ratio of each type of dwelling and average occupancy for each LSOA
    unit_columns : list[str]
        columns names for dwelling column to be disaggregated

    Returns
    -------
    pd.DataFrame
        Path to the population data file
    """

    lsoa_ratio.reset_index("dwelling_type", inplace=True)

    data = data.merge(
        lsoa_ratio, how="left", left_on="LSOA21CD", right_on="lsoa2021_id"
    )
    # data.rename(columns={"LSOA21CD": "lsoa21cd"}, inplace=True)

    for column in unit_columns:
        data.loc[:, column] = (
            data[column] * data["dwelling_ratio"] * data["pop_per_dwelling"]
        )

    return data


def lsoa_site_geospatial_lookup(
    data: pd.DataFrame,
    lsoa: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """spatially joins LSOA shapefile to DLOG sites


    Parameters
    ----------
    data : pd.DataFrame
        data to join to lsoa
    lsoa : gpd.GeoDataFrame
        lsoa data

    Returns
    -------
    gpd.GeoDataFrame
        spatially joined data
    """

    dlog_geom = gpd.GeoDataFrame(
        data, geometry=gpd.points_from_xy(data["easting"], data["northing"])
    )
    dlog_lsoa = gpd.sjoin(dlog_geom, lsoa, how="left")
    return dlog_lsoa


def calc_lsoa_proportion(lsoa_hh: pd.DataFrame) -> pd.DataFrame:
    """calculates the lsoa population by dwelling type


    Parameters
    ----------
    lsoa_hh: pd.DataFrame
        TfN population land use


    Returns
    -------
    pd.DataFrame
        population and ratio of dwellings by dwelling type
    """
    lsoa_hh.set_index(["lsoa2021_id", "dwelling_type"], inplace=True)
    lsoa_hh["dwelling_ratio"] = (
        lsoa_hh["household"] / lsoa_hh["household"].groupby(level="lsoa2021_id").sum()
    )

    return lsoa_hh


def disagg_land_use_codes(
    data: pd.DataFrame,
    luc_column: str,
    unit_columns: list[str],
    land_use_split: pd.DataFrame,
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
    ratio_demonitator = (
        site_luc.groupby(["site_reference_id"])["total_floorspace"]
        .sum()
        .rename({"total_floorspace": "denom"})
    )
    site_luc = site_luc.merge(
        ratio_demonitator,
        how="left",
        left_on="site_reference_id",
        right_index=True,
        suffixes=["", "_denom"],
    )
    ratio = site_luc["total_floorspace"] / site_luc["total_floorspace_denom"]
    ratio.index = disagg.index
    disagg.loc[:, unit_columns] = disagg.loc[:, unit_columns].multiply(ratio, axis=0)
    return disagg
