import os
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd

class GrowthRateVisualizer:
    def __init__(self, output_dir):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        self.app = dash.Dash(__name__)
        self.data_melted = None

    def generate_visualizations(self, combined_datasets):
        self.data_melted = pd.concat([
            self.prepare_data(category, sheet_name, data)
            for category, data_dict in combined_datasets.items()
            for sheet_name, data in data_dict.items()
        ], ignore_index=True)

        self.app.layout = html.Div([
            html.H1("Growth Rate Visualizer"),
            dcc.Dropdown(
                id='category-dropdown',
                options=[{'label': category, 'value': category} for category in combined_datasets.keys()],
                placeholder="Select Category",
                multi=False
            ),
            dcc.Dropdown(
                id='region-dropdown',
                placeholder="Select Region",
                multi=False
            ),
            dcc.Dropdown(
                id='lad-dropdown',
                placeholder="Select LADs",
                multi=True
            ),
            dcc.Graph(id='scatter-plot', style={'height': '80vh'}),
            html.Button("Save as HTML", id="save-button")
        ])

        self.setup_callbacks()
        self.app.run_server(debug=False)

    def prepare_data(self, category, sheet_name, data):
        year_columns = [col for col in data.columns if col.isdigit()]

        if 'Region' in category and '2023' in year_columns:
            year_columns.remove('2023')
    
        data['Sheet'] = sheet_name

        id_var = 'LADNM' if 'LAD' in category else 'REGIONNM'
        if 'LAD' in category:
            melted = data.melt(id_vars=[id_var, 'Source', 'REGIONNM', 'Sheet'],
                           value_vars=year_columns, var_name='Year', value_name='Value')
        else:
            melted = data.melt(id_vars=[id_var, 'Source', 'Sheet'],
                           value_vars=year_columns, var_name='Year', value_name='Value')

        melted['Category'] = category
        return melted

    def setup_callbacks(self):
        @self.app.callback(
            Output('region-dropdown', 'options'),
            [Input('category-dropdown', 'value')]
        )
        def set_region_options(selected_category):
            if selected_category:
                regions = self.data_melted[self.data_melted['Category'] == selected_category]['REGIONNM'].dropna().unique()
                return [{'label': region, 'value': region} for region in regions]
            return []

        @self.app.callback(
            Output('lad-dropdown', 'options'),
            [Input('region-dropdown', 'value'),
             Input('category-dropdown', 'value')]
        )
        def set_lad_options(selected_region, selected_category):
            if selected_region and 'LAD' in selected_category:
                lads = self.data_melted[(self.data_melted['Category'] == selected_category) & (self.data_melted['REGIONNM'] == selected_region)]['LADNM'].dropna().unique()
                return [{'label': lad, 'value': lad} for lad in lads]
            return []

        @self.app.callback(
            Output('scatter-plot', 'figure'),
            [Input('category-dropdown', 'value'),
             Input('region-dropdown', 'value'),
             Input('lad-dropdown', 'value')]
        )
        def update_figure(selected_category, selected_region, selected_lads):
            filtered_df = self.data_melted[self.data_melted['Category'] == selected_category]

            if selected_region:
                filtered_df = filtered_df[filtered_df['REGIONNM'] == selected_region]

            if selected_lads:
                filtered_df = filtered_df[filtered_df['LADNM'].isin(selected_lads)]

            color_column = 'LADNM' if 'LAD' in selected_category else 'Sheet'
            filtered_df['LineID'] = filtered_df[color_column] + '_' + filtered_df['Sheet']
            fig = px.line(filtered_df, x='Year', y='Value', color='LineID', title=f'{selected_category} Growth Rate')
            return fig

        @self.app.callback(
            Output('save-button', 'n_clicks'),
            [Input('save-button', 'n_clicks'),
            Input('category-dropdown', 'value')]
        )
        def save_plot_as_html(n_clicks, selected_category):
            if n_clicks is not None and selected_category:
                filtered_df = self.data_melted[self.data_melted['Category'] == selected_category]
                color_column = 'LADNM' if 'LAD' in selected_category else 'Sheet'
                filtered_df['LineID'] = filtered_df[color_column] + '_' + filtered_df['Sheet']
                fig = px.line(filtered_df, x='Year', y='Value', color='LineID', title=f'{selected_category} Growth Rate')
                output_file_html = os.path.join(self.output_dir, f'{selected_category}_scatter_plot.html')
                fig.write_html(output_file_html)
            return n_clicks
            
if __name__ == "__main__":

    output_directory = r"I:\Data\D-Log\DLIT\Outputs\test12\07_constraint_test"

    combined_datasets_raw = {
        "LAD_Population": {
            "ddg": pd.read_excel(r"I:\Data\D-Log\DLIT\Outputs\LUTI_Dlog24_regression_no_negatives\06_constraint\output_for_viz\lad_YearTotal_pop.xlsx", sheet_name="DDG"),
            "dlog": pd.read_excel(r"I:\Data\D-Log\DLIT\Outputs\LUTI_Dlog24_regression_no_negatives\06_constraint\output_for_viz\lad_YearTotal_pop.xlsx", sheet_name="DLOG"),
            "ntem": pd.read_excel(r"I:\Data\D-Log\DLIT\Outputs\LUTI_Dlog24_regression_no_negatives\06_constraint\output_for_viz\lad_YearTotal_pop.xlsx", sheet_name="NTEM"),
        },
        "Region_Population": {
            "ddg": pd.read_excel(r"I:\Data\D-Log\DLIT\Outputs\LUTI_Dlog24_regression_no_negatives\06_constraint\output_for_viz\region_YearTotal_pop.xlsx", sheet_name="DDG"),
            "dlog": pd.read_excel(r"I:\Data\D-Log\DLIT\Outputs\LUTI_Dlog24_regression_no_negatives\06_constraint\output_for_viz\region_YearTotal_pop.xlsx", sheet_name="DLOG"),
            "ntem": pd.read_excel(r"I:\Data\D-Log\DLIT\Outputs\LUTI_Dlog24_regression_no_negatives\06_constraint\output_for_viz\region_YearTotal_pop.xlsx", sheet_name="NTEM"),
        },
        "LAD_Employment": {
            "ddg": pd.read_excel(r"I:\Data\D-Log\DLIT\Outputs\LUTI_Dlog24_regression_no_negatives\06_constraint\output_for_viz\lad_YearTotal_emp.xlsx", sheet_name="DDG"),
            "dlog": pd.read_excel(r"I:\Data\D-Log\DLIT\Outputs\LUTI_Dlog24_regression_no_negatives\06_constraint\output_for_viz\lad_YearTotal_emp.xlsx", sheet_name="DLOG"),
            "ntem": pd.read_excel(r"I:\Data\D-Log\DLIT\Outputs\LUTI_Dlog24_regression_no_negatives\06_constraint\output_for_viz\lad_YearTotal_emp.xlsx", sheet_name="NTEM"),
        },
        "Region_Employment": {
            "ddg": pd.read_excel(r"I:\Data\D-Log\DLIT\Outputs\LUTI_Dlog24_regression_no_negatives\06_constraint\output_for_viz\region_YearTotal_emp.xlsx", sheet_name="DDG"),
            "dlog": pd.read_excel(r"I:\Data\D-Log\DLIT\Outputs\LUTI_Dlog24_regression_no_negatives\06_constraint\output_for_viz\region_YearTotal_emp.xlsx", sheet_name="DLOG"),
            "ntem": pd.read_excel(r"I:\Data\D-Log\DLIT\Outputs\LUTI_Dlog24_regression_no_negatives\06_constraint\output_for_viz\region_YearTotal_emp.xlsx", sheet_name="NTEM"),
        }
    }

    combined_datasets_gr = {
        "LAD_Population_GR": {
            "ddg": pd.read_excel(r"I:\Data\D-Log\DLIT\Outputs\LUTI_Dlog24_regression_no_negatives\06_constraint\output_for_viz\lad_GrowthRate_pop.xlsx", sheet_name="DDG"),
            "dlog": pd.read_excel(r"I:\Data\D-Log\DLIT\Outputs\LUTI_Dlog24_regression_no_negatives\06_constraint\output_for_viz\lad_GrowthRate_pop.xlsx", sheet_name="DLOG"),
            "ntem": pd.read_excel(r"I:\Data\D-Log\DLIT\Outputs\LUTI_Dlog24_regression_no_negatives\06_constraint\output_for_viz\lad_GrowthRate_pop.xlsx", sheet_name="NTEM"),
        },
        "Region_Population_GR": {
            "ddg": pd.read_excel(r"I:\Data\D-Log\DLIT\Outputs\LUTI_Dlog24_regression_no_negatives\06_constraint\output_for_viz\region_GrowthRate_pop.xlsx", sheet_name="DDG"),
            "dlog": pd.read_excel(r"I:\Data\D-Log\DLIT\Outputs\LUTI_Dlog24_regression_no_negatives\06_constraint\output_for_viz\region_GrowthRate_pop.xlsx", sheet_name="DLOG"),
            "ntem": pd.read_excel(r"I:\Data\D-Log\DLIT\Outputs\LUTI_Dlog24_regression_no_negatives\06_constraint\output_for_viz\region_GrowthRate_pop.xlsx", sheet_name="NTEM"),
        },
        "LAD_Employment_GR": {
            "ddg": pd.read_excel(r"I:\Data\D-Log\DLIT\Outputs\LUTI_Dlog24_regression_no_negatives\06_constraint\output_for_viz\lad_GrowthRate_emp.xlsx", sheet_name="DDG"),
            "dlog": pd.read_excel(r"I:\Data\D-Log\DLIT\Outputs\LUTI_Dlog24_regression_no_negatives\06_constraint\output_for_viz\lad_GrowthRate_emp.xlsx", sheet_name="DLOG"),
            "ntem": pd.read_excel(r"I:\Data\D-Log\DLIT\Outputs\LUTI_Dlog24_regression_no_negatives\06_constraint\output_for_viz\lad_GrowthRate_emp.xlsx", sheet_name="NTEM"),
        },
        "Region_Employment_GR": {
            "ddg": pd.read_excel(r"I:\Data\D-Log\DLIT\Outputs\LUTI_Dlog24_regression_no_negatives\06_constraint\output_for_viz\region_GrowthRate_emp.xlsx", sheet_name="DDG"),
            "dlog": pd.read_excel(r"I:\Data\D-Log\DLIT\Outputs\LUTI_Dlog24_regression_no_negatives\06_constraint\output_for_viz\region_GrowthRate_emp.xlsx", sheet_name="DLOG"),
            "ntem": pd.read_excel(r"I:\Data\D-Log\DLIT\Outputs\LUTI_Dlog24_regression_no_negatives\06_constraint\output_for_viz\region_GrowthRate_emp.xlsx", sheet_name="NTEM"),
        }
    }

    visualizer = GrowthRateVisualizer(output_directory)
    visualizer.generate_visualizations(combined_datasets_gr)
    # visualizer.generate_visualizations(combined_datasets_raw)