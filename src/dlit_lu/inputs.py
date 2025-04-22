"""handles reading config file"""

# standard imports
from __future__ import annotations

import enum
import pathlib
from typing import Any, Optional, Dict

# third party imports
import pydantic
from pydantic import dataclasses, model_validator
import caf.toolkit

AVERAGE_INFILLING_VALUES_FILE = "infilling_average_values.yml"
MEAN_INFILLING_VALUES_FILE = "infilling_mean_values.yml"
MEDIAN_INFILLING_VALUES_FILE = "infilling_median_values.yml"


class GFAInfillMethod(enum.Enum):
    """Method for infilling the GFA from the site area."""

    MEAN = "mean"
    MEDIAN = "median"
    GLBAVERAGE = "glbaverage"
    REGRESSION = "regression"
    REGRESSION_NO_NEGATIVES = "regression_no_negatives"

    @classmethod
    def regression_methods(cls) -> list[GFAInfillMethod]:
        """List of methods which use HistGradientBoostingRegressor."""
        return [cls.REGRESSION, cls.REGRESSION_NO_NEGATIVES]


class GeoBoundary(enum.Enum):
    """Geography boundary options for processing site data."""

    LSOA = "lsoa"
    NORMITS = "normits"
    NOHAM = "noham"
    NORMS = "norms"
    MSOA = "msoa"


class Sector(enum.Enum):
    REGION = "region"
    COMBINED_LAD = "combined_lad"
    LAD = "lad"


# Define sector-specific metadata at the module level
SECTOR_INFO_MAP: Dict[Sector, Dict[str, Any]] = {
    Sector.REGION: {
        "lookup_path": None,  # Path to the translation file from config.constraint or config.tripend
        "zone_id": "lad2013_id",  # Static value (column name) in translation file
        "sector_id": "ntem_region_id",  # Static value (column name) in translation file
        "zone_to_sector_prop_col": "lad2013_to_ntem_region",  # Static value (column name) in translation file
        "sector_name": None,  # Key to fetch from config.constraint
    },
    Sector.LAD: {
        "lookup_path": None,  # Path to the translation file from config.constraint or config.tripend
        "zone_id": None,  # Will be dynamically assigned, column name in translation file
        "sector_id": None,  # Will be dynamically assigned, column name in translation file
        "zone_to_sector_prop_col": None,  # Will be dynamically assigned, column name in translation file
        "sector_name": None,  # Key to fetch from config.constraint
    },
}


@dataclasses.dataclass
class SummaryInputs:
    """Lookup file and shapefile for creating output summaries."""

    # summary_lad: str
    lsoa_to_lad_file: pydantic.FilePath
    msoa_to_lad_file: pydantic.FilePath
    norms_to_lad_file: pydantic.FilePath
    noham_to_lad_file: pydantic.FilePath
    normits_to_lad_file: pydantic.FilePath
    lad_shapefile: pydantic.FilePath
    shapefile_id_column: str
    geometry_simplify_tolerance: int | None = None


@dataclasses.dataclass
class InfillConfig:
    """Manages reading / writing the tool's config file.


    Parameters
    ----------
    user_infill: bool
        whether to run user infilling functionality
    dlog_column_names_path: pathlib.Path
        path to column names in the dlog. contains column names for each
        sheet and column names to drop for all sheets
    user_input_path: pathlib.Path
        Path to file when user inpjut file with but read/written
    valid_luc_path: pathlib.Path
        path to valid land_use codes file
    out_of_date_luc_path: pathlib.Path
        path to out of date land use codes file
    incomplete_luc_path: pathlib.Path
        path to incomplete land use codes file
    known_invalid_luc_path: pathlib.Path
        path to known invalid land use codes and their replacements
    regions_shapefiles_path: pathlib.Path
        path to LPA regions shapefile
    gfa_infill_method : GFAInfillMethod
        Method to use when infilling the site area and GFA columns.
    """

    user_infill: bool
    dlog_column_names_path: pydantic.FilePath
    user_input_path: pathlib.Path
    valid_luc_path: pydantic.FilePath
    out_of_date_luc_path: pydantic.FilePath
    incomplete_luc_path: pydantic.FilePath
    known_invalid_luc_path: pydantic.FilePath
    regions_shapefiles_path: pydantic.FilePath
    gfa_infill_method: GFAInfillMethod


@dataclasses.dataclass
class LandUseConfig:
    """Manages reading / writing the tool's config file.

    Attributes
    ----------
    lsoa_shapefile_path : pydantic.FilePath
        Path to the LSOA shapefile.
    lsoa_hh_types_path: pydantic.FilePath
        Path to the LSOA household segmented by type file.
    lsoa_dwelling_pop_path : pydantic.FilePath
        Path to the LSOA dwelling population file.
    lsoa_traveller_type_path : pydantic.FilePath
        Path to the LSOA traveller type split file.
    lsoa_jobs_path : pydantic.FilePath
        Path to the LSOA jobs data file.
    luti_zone_shapefile_path : pydantic.FilePath
        Path to the LUTI (Land Use and Transport Interaction) zone shapefile.
    employment_density_matrix_path : pydantic.FilePath
        Path to the employment density matrix file.
    luc_sic_conversion_path : pydantic.FilePath
        Path to the land use code to SIC (Standard Industrial Classification) code conversion matrix.
    luti : bool
        Flag indicating whether the LUTI module is enabled.
    land_use_input : Optional[pathlib.Path], default=None
        Path to the land use input file (output of the infill process). This is not required
        if the infilling module is not being used.
    demolition_dampener : pydantic.types.confloat(ge=0, le=1, allow_inf_nan=False), default=1.0
        Factor applied when calculating the number of demolitions.
        A value of 0 means no demolitions, while 1 represents the maximum demolitions allowed.
    """

    lsoa_shapefile_path: pydantic.FilePath
    lsoa_hh_types_path: pydantic.FilePath
    lsoa_dwelling_pop_path: pydantic.FilePath
    lsoa_traveller_type_path: pydantic.FilePath
    lsoa_jobs_path: pydantic.FilePath
    luti_zone_shapefile_path: pydantic.FilePath
    employment_density_matrix_path: pydantic.FilePath
    luc_sic_conversion_path: pydantic.FilePath
    web_tag_certainty_path: pydantic.FilePath
    luti: bool
    # land_use_input: Optional[pydantic.FilePath] = None
    # change from Optional[pydantic.FilePath]  to Optional[pathlib.Path]
    # as pydantic.FilePath immediately validates whether the file exists when parsing the YAML
    # while using pathlib.Path (or str), the validation will only happen inside the custom validator,
    # which properly checks run_land_use before verifying the file path
    land_use_input: Optional[pathlib.Path] = None
    demolition_dampener: pydantic.types.confloat(ge=0, le=1, allow_inf_nan=False) = 1


@dataclasses.dataclass
class DevPatnConfig:
    """Manages reading / writing the tool's config file.

    Attributes
    ----------
    base_year : str
        The base year for the model.
    end_year : str
        The end year of the Dlog data.
    geo_boundary : GeoBoundary
        Specifies the model zone boundary.
    viz_distribution : bool
        Flag indicating whether to visualize distribution.
    index_weights_path : pydantic.FilePath
        Path to the weights file, defining variables for the final weighted index to determine large sites.
    normits_shapefile_path : pydantic.FilePath
        Path to the Normits zone shapefile.
    noham_shapefile_path : pydantic.FilePath
        Path to the Noham zone shapefile.
    norms_shapefile_path : pydantic.FilePath
        Path to the Norms zone shapefile.
    msoa_shapefile_path : pydantic.FilePath
        Path to the MSOA (Middle Layer Super Output Area) shapefile.
    lsoa_hh_centroids : pydantic.FilePath
        Path to LSOA (Lower Layer Super Output Area) household centroids.
    lsoa_emp_centroids : pydantic.FilePath
        Path to LSOA employment centroids.
    lsoa_pop_centroids : pydantic.FilePath
        Path to LSOA population centroids.
    normits_hh_centroids : pydantic.FilePath
        Path to Normits household centroids.
    normits_emp_centroids : pydantic.FilePath
        Path to Normits employment centroids.
    normits_pop_centroids : pydantic.FilePath
        Path to Normits population centroids.
    noham_hh_centroids : pydantic.FilePath
        Path to Noham household centroids.
    noham_emp_centroids : pydantic.FilePath
        Path to Noham employment centroids.
    noham_pop_centroids : pydantic.FilePath
        Path to Noham population centroids.
    norms_hh_centroids : pydantic.FilePath
        Path to Norms household centroids.
    norms_emp_centroids : pydantic.FilePath
        Path to Norms employment centroids.
    norms_pop_centroids : pydantic.FilePath
        Path to Norms population centroids.
    msoa_hh_centroids : pydantic.FilePath
        Path to MSOA household centroids.
    msoa_emp_centroids : pydantic.FilePath
        Path to MSOA employment centroids.
    msoa_pop_centroids : pydantic.FilePath
        Path to MSOA population centroids.
    lsoa_to_normits : pydantic.FilePath
        Path to the translation file from LSOA to Normits.
    lsoa_to_noham : pydantic.FilePath
        Path to the translation file from LSOA to Noham.
    lsoa_to_norms : pydantic.FilePath
        Path to the translation file from LSOA to Norms.
    lsoa_to_msoa : pydantic.FilePath
        Path to the translation file from LSOA to MSOA.
    tfn_tt : pydantic.FilePath
        Path to Transport for the North (TfN) travel time data.
    normits_hh_car : pydantic.FilePath
        Path to Normits household data segmented by car ownership.
    normits_pop_car : pydantic.FilePath
        Path to Normits population data segmented by car ownership.
    normits_pop_age : pydantic.FilePath
        Path to Normits population data segmented by age group.
    lad_hh_car : pydantic.FilePath
        Path to LAD (Local Authority District) household data segmented by car ownership.
    lad_pop_car : pydantic.FilePath
        Path to LAD population data segmented by car ownership.
    lad_pop_age : pydantic.FilePath
        Path to LAD population data segmented by age group.
    lsoa_data_path : Optional[pathlib.Path], default=None
        Path to the zonal totals file containing population, dwelling, and employment data.
    assessment_input : Optional[pathlib.Path], default=None
        Path to the site assessment input file, used to determine if land use values are estimated.
    emp_site_data : Optional[pathlib.Path], default=None
        Path to employment site data.
    res_site_data : Optional[pathlib.Path], default=None
        Path to residential site data.
    hh_type_site_data : Optional[pathlib.Path], default=None
        Path to household data segmented by household type.
    pop_tt_site_data : Optional[pathlib.Path], default=None
        Path to population data segmented by travel type.
    emp_sic_soc_site_data : Optional[pathlib.Path], default=None
        Path to job data segmented by SIC (Standard Industrial Classification) and SOC (Standard Occupational Classification).
    summary_data : SummaryInputs | None, optional
        Lookup file and shapefile for creating output summaries at different zone levels.
    """

    base_year: str
    end_year: str
    geo_boundary: GeoBoundary
    viz_distribution: bool
    index_weights_path: pydantic.FilePath
    normits_shapefile_path: pydantic.FilePath
    noham_shapefile_path: pydantic.FilePath
    norms_shapefile_path: pydantic.FilePath
    msoa_shapefile_path: pydantic.FilePath
    lsoa_hh_centroids: pydantic.FilePath
    lsoa_emp_centroids: pydantic.FilePath
    lsoa_pop_centroids: pydantic.FilePath
    normits_hh_centroids: pydantic.FilePath
    normits_emp_centroids: pydantic.FilePath
    normits_pop_centroids: pydantic.FilePath
    noham_hh_centroids: pydantic.FilePath
    noham_emp_centroids: pydantic.FilePath
    noham_pop_centroids: pydantic.FilePath
    norms_hh_centroids: pydantic.FilePath
    norms_emp_centroids: pydantic.FilePath
    norms_pop_centroids: pydantic.FilePath
    msoa_hh_centroids: pydantic.FilePath
    msoa_emp_centroids: pydantic.FilePath
    msoa_pop_centroids: pydantic.FilePath
    lsoa_to_normits: pydantic.FilePath
    lsoa_to_noham: pydantic.FilePath
    lsoa_to_norms: pydantic.FilePath
    lsoa_to_msoa: pydantic.FilePath
    tfn_tt: pydantic.FilePath
    normits_hh_car: pydantic.FilePath
    normits_pop_car: pydantic.FilePath
    normits_pop_age: pydantic.FilePath
    lad_hh_car: pydantic.FilePath
    lad_pop_car: pydantic.FilePath
    lad_pop_age: pydantic.FilePath
    lsoa_data_path: Optional[pathlib.Path] = None
    assessment_input: Optional[pathlib.Path] = None
    emp_site_data: Optional[pathlib.Path] = None
    res_site_data: Optional[pathlib.Path] = None
    pop_site_data: Optional[pathlib.Path] = None
    emp_sic_site_data: Optional[pathlib.Path] = None
    hh_type_site_data: Optional[pathlib.Path] = None
    pop_tt_site_data: Optional[pathlib.Path] = None
    emp_sic_soc_site_data: Optional[pathlib.Path] = None
    summary_data: SummaryInputs | None = None


@dataclasses.dataclass
class ConstraintConfig:
    """Configuration for defining constraints related to household, employment, and population data.

    This class holds the configuration for sector-based constraints, including paths to various
    data sources for household, employment, and population, as well as files for region and Local
    Authority District (LAD) mappings. The configuration also allows for optional DLOG data related
    to household, employment, and population.

    Attributes
    ----------
    sector : Sector
        The sector for which constraints are applied.
    lad_to_region_file : pydantic.FilePath
        Path to the mapping file that translates LAD (Local Authority District) data to regions.
    lad_name : pydantic.FilePath
        Path to the file containing LAD names.
    region_name : pydantic.FilePath
        Path to the file containing region names.
    ddg_pop : pydantic.FilePath
        Path to the DDG (Demand Distribution Generator) population data.
    ddg_emp : pydantic.FilePath
        Path to the DDG employment data.
    ntem_hh : pydantic.FilePath
        Path to the NTEM (National Trip End Model) household data.
    ntem_pop : pydantic.FilePath
        Path to the NTEM population data.
    ntem_emp : pydantic.FilePath
        Path to the NTEM employment data.
    dlog_hh : Optional[pathlib.Path], default=None
        Path to the DLOG household data, if available.
    dlog_emp : Optional[pathlib.Path], default=None
        Path to the DLOG employment data, if available.
    dlog_pop : Optional[pathlib.Path], default=None
        Path to the DLOG population data, if available.
    """

    sector: Sector
    lad_to_region_file: pydantic.FilePath
    lad_name: pydantic.FilePath
    region_name: pydantic.FilePath
    ddg_pop: pydantic.FilePath
    ddg_emp: pydantic.FilePath
    ntem_hh: pydantic.FilePath
    ntem_pop: pydantic.FilePath
    ntem_emp: pydantic.FilePath
    cap_ratio: float
    dlog_hh: Optional[pathlib.Path] = None
    dlog_emp: Optional[pathlib.Path] = None
    dlog_pop: Optional[pathlib.Path] = None


@dataclasses.dataclass
class TripendsConfig:
    """Configuration for defining trip ends and their associated data.

    This class holds the configuration for trip-end data, including paths to various files
    that describe trip production and attraction for different zones and regions, as well as
    data for households, employment, and populations. Additionally, it includes options for
    exporting data for visualization and optional DLOG data related to trip ends.

    Attributes
    ----------
    sector : Sector
        The sector for which trip-end data is being defined.
    future_years : list[int]
        List of future years for which trip-end data is required.
    by_fr_hb : pydantic.FilePath
        Path to the file containing "by" trip ends from households (HB).
    by_to_hb : pydantic.FilePath
        Path to the file containing "to" trip ends from households (HB).
    by_nhb : pydantic.FilePath
        Path to the file containing "by" trip ends from non-households (NHB).
    ntem_zone_hb_prod : pydantic.FilePath
        Path to the NTEM zone-level production data for households (HB).
    ntem_zone_hb_attr : pydantic.FilePath
        Path to the NTEM zone-level attraction data for households (HB).
    ntem_zone_nhb_prod : pydantic.FilePath
        Path to the NTEM zone-level production data for non-households (NHB).
    ntem_zone_nhb_attr : pydantic.FilePath
        Path to the NTEM zone-level attraction data for non-households (NHB).
    ntem_lad_hb_prod : pydantic.FilePath
        Path to the NTEM LAD-level production data for households (HB).
    ntem_lad_hb_attr : pydantic.FilePath
        Path to the NTEM LAD-level attraction data for households (HB).
    ntem_lad_nhb_prod : pydantic.FilePath
        Path to the NTEM LAD-level production data for non-households (NHB).
    ntem_lad_nhb_attr : pydantic.FilePath
        Path to the NTEM LAD-level attraction data for non-households (NHB).
    export_for_viz : bool
        Flag indicating whether to export trip-end data for visualization.
    dlog_hb_prod : Optional[pathlib.Path], default=None
        Path to the DLOG household production data, if available.
    dlog_hb_attr : Optional[pathlib.Path], default=None
        Path to the DLOG household attraction data, if available.
    dlog_nhb_prod : Optional[pathlib.Path], default=None
        Path to the DLOG non-household production data, if available.
    dlog_nhb_attr : Optional[pathlib.Path], default=None
        Path to the DLOG non-household attraction data, if available.
    """

    sector: Sector
    future_years: list[int]
    # pop2023: pydantic.FilePath
    by_fr_hb: pydantic.FilePath
    by_to_hb: pydantic.FilePath
    by_nhb: pydantic.FilePath
    ntem_zone_hb_prod: pydantic.FilePath
    ntem_zone_hb_attr: pydantic.FilePath
    ntem_zone_nhb_prod: pydantic.FilePath
    ntem_zone_nhb_attr: pydantic.FilePath
    ntem_lad_hb_prod: pydantic.FilePath
    ntem_lad_hb_attr: pydantic.FilePath
    ntem_lad_nhb_prod: pydantic.FilePath
    ntem_lad_nhb_attr: pydantic.FilePath
    export_for_viz: bool
    fy_fr_hb: Optional[pathlib.Path] = None
    fy_to_hb: Optional[pathlib.Path] = None
    fy_nhb: Optional[pathlib.Path] = None
    # dlog_hb_prod: Optional[pathlib.Path] = None
    # dlog_hb_attr: Optional[pathlib.Path] = None
    # dlog_nhb_prod: Optional[pathlib.Path] = None
    # dlog_nhb_attr: Optional[pathlib.Path] = None


class DLitConfig(caf.toolkit.BaseConfig):
    """Manages reading / writing the tool's config file."""

    run_infill: bool
    run_land_use: bool
    run_dev_pattern: bool
    run_constraint: bool
    run_tripend: bool

    output_folder: pathlib.Path
    proposed_luc_split_path: pathlib.Path
    existing_luc_split_path: pathlib.Path
    dlog_input_file: pydantic.FilePath
    lookups_sheet_name: str

    infill: Optional[InfillConfig] = None
    land_use: Optional[LandUseConfig] = None
    dev_pattern: Optional[DevPatnConfig] = None
    constraint: Optional[ConstraintConfig] = None
    tripend: Optional[TripendsConfig] = None

    @pydantic.validator("infill")
    def check_running_infill(  # pylint: disable=no-self-argument
        cls, value: InfillConfig | None, values: dict[str, Any]
    ) -> dict[str, Any]:
        """Check infill parameters are given if running module."""
        if not values["run_infill"] and value is None:
            raise ValueError("infill is required if run_infill is true")

        return value

    @pydantic.validator("land_use")
    def land_use_input_check(  # pylint: disable=no-self-argument
        cls, value: LandUseConfig | None, values: dict[str, Any]
    ) -> LandUseConfig:
        """Check land use is given if running module."""
        if not values["run_land_use"]:
            # Don't need to check if we aren't running land use module
            return value

        if value is None:
            raise ValueError("land_use required if run_land_use is true")

        if not values["run_infill"] and value.land_use_input is None:
            # Need land use input path if not running infill module
            raise ValueError("land_use_input required if not running land_use")

        return value

    @pydantic.validator("dev_pattern")
    def dev_pattern_input_check(  # pylint: disable=no-self-argument
        cls, value: DevPatnConfig | None, values: dict[str, Any]
    ) -> DevPatnConfig:
        """Check dev pattern is given if running module."""
        if not values["run_dev_pattern"]:
            # Don't need to check if we aren't running dev_pattern module
            return value

        if value is None:
            raise ValueError("dev_pattern is required if run_dev_pattern is true")

        if not values.get("run_land_use") and not all(
            [value.lsoa_data_path, value.emp_site_data, value.res_site_data]
        ):
            raise ValueError(
                "lsoa_data_path, emp_site_data, and res_site_data are required if not running infill module"
            )

        return value

    @pydantic.validator("constraint")
    def constraint_input_check(  # pylint: disable=no-self-argument
        cls, value: ConstraintConfig | None, values: dict[str, Any]
    ) -> ConstraintConfig:
        """Check contraints is given if running module."""
        if not values["run_constraint"]:
            # Don't need to check if we aren't running constraints module
            return value

        if value is None:
            raise ValueError("constraint is required if run_constraint is true")

        if not values.get("run_dev_pattern") and not all(
            [value.dlog_hh, value.dlog_emp, value.dlog_pop]
        ):
            raise ValueError(
                "dlog_household, dlog_employment, and dlog_population at LAD level are required if not running dev_pattern module"
            )

        return value

    @pydantic.validator("tripend")
    def tripend_input_check(  # pylint: disable=no-self-argument
        cls, value: TripendsConfig | None, values: dict[str, Any]
    ) -> TripendsConfig:
        """Check contraints is given if running module."""
        if not values["run_tripend"]:
            # Don't need to check if we aren't running constraints module
            return value

        if value is None:
            raise ValueError("tripend is required if run_tripend is true")

        if not values.get("run_constraint") and not all(
            [value.fy_fr_hb, value.fy_nhb]
            # [value.dlog_hb_prod, value.dlog_hb_attr]
        ):
            raise ValueError(
                "dlog_hb_prod, dlog_hb_attr are required if not running constraint module"
            )

        return value

    @model_validator(mode="after")
    def check_running(cls, instance: "DLitConfig") -> "DLitConfig":
        """Ensure at least one module is set to run."""
        if not any(
            [
                instance.run_infill,
                instance.run_land_use,
                instance.run_dev_pattern,
                instance.run_constraint,
                instance.run_tripend,
            ]
        ):
            raise ValueError(
                "At least one of run_infill, run_land_use, "
                "run_dev_pattern, run_constraint, or run_tripend must be set to True"
            )

        return instance


class InfillingAverages(caf.toolkit.BaseConfig):
    """Averages calculated for use in glbaverage infill method."""

    average_res_area: float
    average_emp_area: float
    average_mix_area: float
    average_gfa_site_area_ratio: float
    average_dwelling_site_area_ratio: float


class InfillingMeans(caf.toolkit.BaseConfig):
    """Means calculated for use in MEAN infill method."""

    mean_res_area: float
    mean_emp_area: float
    mean_mix_area: float
    mean_gfa_site_area_ratio: float
    mean_dwelling_site_area_ratio: float


class InfillingMedians(caf.toolkit.BaseConfig):
    """Medians calculated for use in medians infill method."""

    median_res_area: float
    median_emp_area: float
    median_mix_area: float
    median_gfa_site_area_ratio: float
    median_dwelling_site_area_ratio: float
