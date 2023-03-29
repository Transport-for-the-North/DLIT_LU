"""contains NamedTuple subclasses and other classes used globally
"""  # docstring
# standard imports
from __future__ import annotations

from typing import NamedTuple, Optional

# third party imports
import pandas as pd
import geopandas as gpd


class DLogValueLookup(NamedTuple):
    """contains all the lookup tables from the DLog lookup sheet

    all tables are stored with the ID (int) as index and value as str
    except for existing_landuse (pd.Series) which is not strictly a
    lookup table but a list of possible vales

    Parameters
    ----------
    site_type: pd.DataFrame
        site stype ID look up table
    construction_status: pd.DataFrame
        construction status ID look up table
    planning_status: pd.DataFrame
        planning status ID look up tables
    webtag: pd.DataFrame
        webtag status ID look up table
    development_type: pd.DataFrame
        development type ID lookup table
    years: pd.DataFrame
        years ID look up table
    distribution_profile: pd.DataFrame
        distribution profile look up table
    land_use_codes: pd.Series
        a list of allowed land use codes, though to incomplete for the
        2021 set, so a complete list is read in in auxiliary data
    adoption_status: pd.DataFrame
        adoption status look up table
    local_authority: pd.DataFrame
        local authority id look up table
    """

    site_type: pd.DataFrame
    construction_status: pd.DataFrame
    planning_status: pd.DataFrame
    webtag: pd.DataFrame
    development_type: pd.DataFrame
    years: pd.DataFrame
    distribution_profile: pd.DataFrame
    land_use_codes: pd.Series
    adoption_status: pd.DataFrame
    local_authority: pd.DataFrame


class DLogData(NamedTuple):
    """used to store and pass the read DLOg data set

    note that the DataFrames do not have identical column names
    Parameters
    ----------
    residential_data: pd.DataFrame
        the residential data set
    employment_data: pd.DataFrame
        the employment data set
    mixed_data: Optional[pd.DataFrame]
        sites which have residential and employment developments
    lookup: Lookup
        a look up table for the IDs used in the data
    combined_data: Optional[pd.DataFrame] = None
        the combined data set
    proposed_land_use_split: Optional[pd.DataFrame] = None
        the split of proposed land use codes that appears in the dlog
    existing_land_use_split: Optional[pd.DataFrame] = None
        the split of existing land use codes that appears in the dlog
    """

    combined_data: Optional[pd.DataFrame]
    residential_data: pd.DataFrame
    employment_data: pd.DataFrame
    mixed_data: pd.DataFrame
    lookup: DLogValueLookup
    proposed_land_use_split: Optional[pd.DataFrame] = None
    existing_land_use_split: Optional[pd.DataFrame] = None

    def data_dict(self) -> dict[str, pd.DataFrame]:
        """Convert to D-Log data dictionary.

        Dictionary contains keys "residential", "employment"
        and "mixed".
        """
        return {
            "residential": self.residential_data,
            "employment": self.employment_data,
            "mixed": self.mixed_data,
        }

    @staticmethod
    def from_data_dict(
        data: dict[str, pd.DataFrame],
        lookup: DLogValueLookup,
        combined_data: Optional[pd.DataFrame] = None,
    ) -> DLogData:
        """Create DLogData from data dictionary.

        Parameters
        ----------
        data : dict[str, pd.DataFrame]
            D-Log data with keys "residential", "employment"
            and "mixed".
        lookup : DLogValueLookup
            D-Log lookup data.
        combined_data : pd.DataFrame, optional
            Combined D-Log data.

        Returns
        -------
        DLogData
            New instance of DLogData with `data`.
        """
        return DLogData(
            combined_data=combined_data,
            **{f"{k}_data": v for k, v in data.items()},
            lookup=lookup,
        )

    def copy(self) -> DLogData:
        """Shallow copy of the attributes within the data.

        Copies dataframes but uses the same `lookup`.
        """

        def optional_copy(data: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
            if data is None:
                return None
            return data.copy()

        return DLogData(
            combined_data=optional_copy(self.combined_data),
            residential_data=self.residential_data.copy(),
            employment_data=self.employment_data.copy(),
            mixed_data=self.mixed_data.copy(),
            lookup=self.lookup,
            proposed_land_use_split=optional_copy(self.proposed_land_use_split),
            existing_land_use_split=optional_copy(self.existing_land_use_split),
        )


class AuxiliaryData(NamedTuple):
    """stores data not contained in the DLog required for processing

    contains allowed and not allowed land use codes and lpa region
    shape data

    Parameters
    ----------
    allowed_codes:pd.DataFrame

    out_of_date_luc: pd.DataFrame
        out of date landuse codes lookup
    known_invalid_lucs:pd.DataFrame
        known issues landuse code lookup
    incomplete_luc: pd.DataFrame
        incomplete land use codes lookup
    regions: gpd.GeoDataFrame
        regions shape file
    """

    allowed_codes: pd.DataFrame
    known_invalid_luc: pd.DataFrame
    out_of_date_luc: pd.DataFrame
    incomplete_luc: pd.DataFrame
    regions: gpd.GeoDataFrame


class ResultsReport:
    """stores the results report

    Parameters
    ----------
    data_filter: dict[str, pd.DataFrame]
        data with the filter column added
    analysis_summary: list[dict[str, int]]
        the number of invalid values in each filter column
    analysis_summary_index_labels: list[str]
        labels each of the indexs in the summary
    analysis_summary_notes: list[str]
        notes for each of the filter column displayed in the summary sheet
    filter_columns:list[str]
        list of the filter column names
    """

    data_filter: dict[str, pd.DataFrame]
    analysis_summary: list[dict[str, int]]
    analysis_summary_index_labels: list[str]
    analysis_summary_notes: list[str]
    filter_columns: list[str]

    def __init__(
        self,
        data_filter: dict[str, pd.DataFrame],
        column_name: list[str],
        index_name: list[str],
        notes: list[str],
    ) -> None:
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

        self.data_filter = data_filter

        self.analysis_summary = [
            {
                "Residential": len(data_filter["residential"]),
                "Employment": len(data_filter["employment"]),
                "Mixed": len(data_filter["mixed"]),
            }
        ]

        self.analysis_summary_index_labels = index_name

        self.analysis_summary_notes = notes
        self.filter_columns = column_name

    def append_analysis_results(
        self,
        results: dict[str, pd.DataFrame],
        column_name: str,
        notes: str,
    ) -> None:
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
        for key, value in results.items():
            self.data_filter[key][column_name] = False
            self.data_filter[key].loc[value.index, column_name] = True

        self.analysis_summary = self.analysis_summary + [
            {
                "Residential": len(results["residential"]),
                "Employment": len(results["employment"]),
                "Mixed": len(results["mixed"]),
            }
        ]

        self.analysis_summary_index_labels = self.analysis_summary_index_labels + [
            column_name
        ]
        self.analysis_summary_notes = self.analysis_summary_notes + [notes]
        self.filter_columns = self.filter_columns + [column_name]


class ResultsReport_(NamedTuple):
    """stores the results report

    Parameters
    ----------
    data_filter: dict[str, pd.DataFrame]
        data with the filter column added
    analysis_summary: list[dict[str, int]]
        the number of invalid values in each filter column
    analysis_summary_index_labels: list[str]
        labels each of the indexs in the summary
    analysis_summary_notes: list[str]
        notes for each of the filter column displayed in the summary sheet
    filter_columns:list[str]
        list of the filter column names
    """

    data_filter: dict[str, pd.DataFrame]
    analysis_summary: list[dict[str, int]]
    analysis_summary_index_labels: list[str]
    analysis_summary_notes: list[str]
    filter_columns: list[str]

    def append_analysis_results(
        self,
        results: dict[str, pd.DataFrame],
        column_name: str,
        notes: str,
    ) -> None:
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
        for key, value in results.items():
            self.data_filter[key][column_name] = False
            self.data_filter[key].loc[value.index, column_name] = True

        self.analysis_summary = self.analysis_summary + [
            {
                "Residential": len(results["residential"]),
                "Employment": len(results["employment"]),
                "Mixed": len(results["mixed"]),
            }
        ]

        self.analysis_summary_index_labels = self.analysis_summary_index_labels + [
            column_name
        ]
        self.analysis_summary_notes = self.analysis_summary_notes + [notes]
        self.filter_columns = self.filter_columns + [column_name]
