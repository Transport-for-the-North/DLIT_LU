# standard imports
import logging

# third party
import pandas as pd

LOG = logging.getLogger(__name__)
REGION_COLUMN = "Region"


def constrain_to_forecast(
    data: pd.DataFrame,
    forecast: pd.DataFrame,
    zone_to_constraint_region_lookup: pd.DataFrame,
    data_unit_col: str,
    data_zone_col: str,
    forecast_unit_col: str,
    region_column: str = REGION_COLUMN,
) -> pd.DataFrame:
    
    if data_unit_col not in data.columns:
        raise ValueError("unit columns are not in data")
    if forecast_unit_col not in forecast.columns:
        raise ValueError("columns are not in forecast")
    index_cols = data.index.names
    data.reset_index(inplace=True)

    data_region = data.merge(zone_to_constraint_region_lookup, on=data_zone_col)
    data_region_sums = (
        data_region.loc[:, [data_unit_col, region_column]].groupby(region_column).sum()
    )

    forecast_region = forecast.merge(zone_to_constraint_region_lookup, on=data_zone_col)
    forecast_region_sums = (
        forecast_region.loc[:, [forecast_unit_col, region_column]]
        .groupby(region_column)
        .sum()
    )

    for region, sum in data_region_sums.iterrows():
        if sum[0] > forecast_region_sums.loc[region, forecast_unit_col]:
            data.loc[data_region[REGION_COLUMN] == region, data_unit_col] = data_above_forecast(
                data.loc[data_region[REGION_COLUMN] == region],
                forecast_region_sums.loc[region, forecast_unit_col],
                0.5,
                data_unit_col,
            )
        else:
            print("do sommat else")
    data.set_index(index_cols, inplace = True)
    return data


def data_above_forecast(
    data: pd.DataFrame, forecast: float, max_displacement: pd.DataFrame, value_col: str
) -> pd.Series:
    
    total_displacement = data[value_col].sum() - forecast
    disaggregate_displacement = (data[value_col]*total_displacement)/data[value_col].sum()
    disaggregate_displacement.loc[disaggregate_displacement>data[value_col]*max_displacement] = data[value_col]*max_displacement
    displaced_data = data[value_col] - disaggregate_displacement
    if displaced_data.sum() > forecast:
        factor = forecast/displaced_data.sum()
        displaced_data = displaced_data*factor
    return displaced_data

def data_below_forecast(
    data: pd.DataFrame, forecast: float, value_col: str
) -> pd.Series:
        factor = forecast/data[value_col].sum()
        displaced_data = data[value_col]*factor
        return displaced_data
