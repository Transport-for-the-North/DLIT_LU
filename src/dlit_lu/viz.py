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
        specific_lads = [
            "Bury", "Manchester", "Oldham", "Rochdale",
            "Salford", "Stockport", "Tameside", "Trafford"
        ]

        fig = go.Figure()
        trace_names = []

        for sheet_name, data in data_dict.items():
            # Extract year columns
            year_columns = [col for col in data.columns if col.startswith('CAGR_')]
            new_column_names = {col: col.split('_')[1] for col in year_columns}
            data.rename(columns=new_column_names, inplace=True)

            # Reshape data
            id_var = 'LADNM' if 'LAD' in category else 'REGIONNM'
            data_melted = data.melt(id_vars=[id_var, 'Source'],
                                    value_vars=new_column_names.values(), var_name='Year', value_name='CAGR')

            # Filter data to include only specific LADs
            if 'LAD' in category:
                data_melted = data_melted[data_melted[id_var].isin(specific_lads)]

            zones = data_melted[id_var].unique()

            for zone in zones:
                zone_data = data_melted[data_melted[id_var] == zone]
                trace = go.Scatter(
                    x=zone_data['Year'], y=zone_data['CAGR'], mode='lines+markers',
                    name=f"{zone} - {sheet_name}", visible=(sheet_name == 'DDG'), hovertemplate='%{y:.2f}%')
                fig.add_trace(trace)
                trace_names.append(f"{zone} - {sheet_name}")

        # Dropdown options
        buttons = [
            dict(method='update', label='All',
                 args=[{'visible': [True] * len(fig.data)},
                       {'title': f"{category} - All Zones"}])
        ]
        for zone in zones:
            visibility = [(trace.startswith(zone)) for trace in trace_names]
            buttons.append(dict(method='update', label=zone,
                                args=[{'visible': visibility},
                                      {'title': f"{category} - {zone}"}]))

        fig.update_layout(
            updatemenus=[{'buttons': buttons, 'direction': 'down',
                          'showactive': True, 'x': 1, 'xanchor': 'left',
                          'y': 1.10, 'yanchor': 'top'}],
            title=f"{category}", xaxis_title="Year",
            yaxis_title="CAGR (%)", yaxis_tickformat=",d"
        )

        output_file_html = os.path.join(self.output_dir, f"{category}_Growth_Trend.html")
        fig.write_html(output_file_html)
        print(f"Visualization saved as: {output_file_html}")