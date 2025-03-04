import os
import plotly.graph_objects as go
import pandas as pd

class GrowthRateVisualizer:
    def __init__(self, output_dir):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def generate_visualizations(self, combined_datasets):
        """
        Generate interactive visualizations for each dataset and save them as HTML.

        Args:
            combined_datasets (dict): Dictionary containing combined datasets.
        """
        for category, data_dict in combined_datasets.items():
            self.create_combined_plot(data_dict, category)

    def create_combined_plot(self, data_dict, category):
        """
        Create and save an interactive plot for a given category.

        Args:
            data_dict (dict): Dictionary of DataFrames for DDG and DLOG.
            category (str): The category of the data (e.g., LAD_Population).
        """
        fig = go.Figure()
        trace_names = []

        for sheet_name, data in data_dict.items():
            # Extract year columns
            year_columns = [col for col in data.columns if col.startswith('CAGR_')]
            new_column_names = {col: col.split('_')[1] for col in year_columns}
            data.rename(columns=new_column_names, inplace=True)

            # Determine id_vars based on the presence of REGIONNM
            if 'LAD' in category:
                id_var = 'LADNM'
                id_vars = [id_var, 'REGIONNM', 'Source']
            else:
                id_var = 'REGIONNM'
                id_vars = [id_var, 'Source']

            # Reshape data
            data_melted = data.melt(id_vars=id_vars,
                                    value_vars=new_column_names.values(), var_name='Year', value_name='CAGR')

            # Determine zones and regions
            if 'LAD' in category:
                regions = sorted(data_melted['REGIONNM'].unique())
                zones = sorted(data_melted['LADNM'].unique())
            else:
                zones = sorted(data_melted['REGIONNM'].unique())

            for zone in zones:
                zone_data = data_melted[data_melted[id_var] == zone]
                trace = go.Scatter(
                    x=zone_data['Year'], y=zone_data['CAGR'], mode='lines+markers',
                    name=f"{zone} - {sheet_name}", visible=False, hovertemplate='%{y:.2f}%')
                fig.add_trace(trace)
                trace_names.append((zone, sheet_name))

        if 'LAD' in category:
            # Create dropdowns for regions and LADs
            region_buttons = []
            lad_buttons_dict = {region: [] for region in regions}

            for region in regions:
                region_visibility = [(trace[0] in data_melted[data_melted['REGIONNM'] == region]['LADNM'].values) for trace in trace_names]
                region_buttons.append(dict(method='update', label=region,
                                           args=[{'visible': region_visibility},
                                                 {'title': f"{category} - {region}"}]))

                # Create LAD buttons for each region
                region_lads = data_melted[data_melted['REGIONNM'] == region]['LADNM'].unique()
                for lad in region_lads:
                    lad_visibility = [(trace[0] == lad) for trace in trace_names]
                    lad_buttons_dict[region].append(dict(method='update', label=lad,
                                                         args=[{'visible': lad_visibility},
                                                               {'title': f"{category} - {lad}"}]))

            # Default LAD buttons (all visible)
            default_lad_buttons = [
                dict(method='update', label='All LADs',
                     args=[{'visible': [True] * len(fig.data)},
                           {'title': f"{category} - All LADs"}])
            ]

            fig.update_layout(
                updatemenus=[
                    {'buttons': region_buttons, 'direction': 'down',
                     'showactive': True, 'x': 0.17, 'xanchor': 'left',
                     'y': 1.15, 'yanchor': 'top', 'pad': {'r': 10, 't': 10}},
                    {'buttons': default_lad_buttons, 'direction': 'down',
                     'showactive': True, 'x': 0.37, 'xanchor': 'left',
                     'y': 1.15, 'yanchor': 'top', 'pad': {'r': 10, 't': 10}}
                ],
                title=f"{category}", xaxis_title="Year",
                yaxis_title="CAGR (%)", yaxis_tickformat=",d"
            )

            # Add LAD buttons for each region
            for region, lad_buttons in lad_buttons_dict.items():
                fig.update_layout(
                    updatemenus=list(fig.layout.updatemenus) + [
                        {'buttons': lad_buttons, 'direction': 'down',
                         'showactive': True, 'x': 0.37, 'xanchor': 'left',
                         'y': 1.15, 'yanchor': 'top', 'pad': {'r': 10, 't': 10}}
                    ]
                )
        else:
            # Single dropdown for Region graphs
            buttons = [
                dict(method='update', label='All Regions',
                     args=[{'visible': [True] * len(fig.data)},
                           {'title': f"{category} - All Regions"}])
            ]
            for zone in zones:
                visibility = [(trace[0] == zone) for trace in trace_names]
                buttons.append(dict(method='update', label=zone,
                                    args=[{'visible': visibility},
                                          {'title': f"{category} - {zone}"}]))

            fig.update_layout(
                updatemenus=[
                    {'buttons': buttons, 'direction': 'down',
                     'showactive': True, 'x': 0.17, 'xanchor': 'left',
                     'y': 1.15, 'yanchor': 'top', 'pad': {'r': 10, 't': 10}}
                ],
                title=f"{category}", xaxis_title="Year",
                yaxis_title="CAGR (%)", yaxis_tickformat=",d"
            )

        output_file_html = os.path.join(self.output_dir, f"{category}_Growth_Trend_v1.html")
        fig.write_html(output_file_html)
        print(f"Visualization saved as: {output_file_html}")