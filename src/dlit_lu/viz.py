import os
import plotly.graph_objects as go
import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class Visualizer:
    def __init__(self, output_dir):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def generate_visualizations(self, input_folder):
        excel_files = list(Path(input_folder).glob("*.xlsx"))
        for file in excel_files:
            data = pd.read_excel(file, sheet_name=None)
            if "lad" in file.stem.lower():
                self.create_combined_lad_plot(data, file.stem)
            elif "region" in file.stem.lower():
                self.create_combined_region_plot(data, file.stem)

    def create_combined_region_plot(self, data, file_name):
        fig = go.Figure()
        first_sheet = next(iter(data.values()))
        regions = first_sheet["REGIONNM"].unique()

        for i, (sheet_name, df) in enumerate(data.items()):
            for region in regions:
                region_data = df[df["REGIONNM"] == region]
                year_columns = [col for col in region_data.columns if col.isdigit()]
                trace = go.Scatter(
                    x=year_columns,
                    y=region_data.iloc[0][year_columns],
                    mode="lines+markers",
                    name=f"{region} - {sheet_name}",
                    hovertemplate="%{y:.2f}",
                )
                fig.add_trace(trace)

        fig.update_layout(
            title=f"{file_name} - Combined Sheets",
            xaxis_title="Year",
            yaxis_title="Value",
            yaxis_tickformat=".2f",
            legend=dict(title="Region Selections"),
        )

        self.add_dropdown(fig, regions, file_name, "REGIONNM")
        self.save_plot(fig, file_name, "Region")

    def create_combined_lad_plot(self, data, file_name):
        first_sheet = next(iter(data.values()))
        regions = first_sheet["REGIONNM"].unique()

        for region in regions:
            fig = go.Figure()
            for sheet_name, df in data.items():
                region_data = df[df["REGIONNM"] == region]
                lads = region_data["LADNM"].unique()
                for lad in lads:
                    lad_data = region_data[region_data["LADNM"] == lad]
                    year_columns = [col for col in lad_data.columns if col.isdigit()]
                    trace = go.Scatter(
                        x=year_columns,
                        y=lad_data.iloc[0][year_columns],
                        mode="lines+markers",
                        name=f"{lad} - {sheet_name}",
                        hovertemplate="%{y:.2f}",
                    )
                    fig.add_trace(trace)

            fig.update_layout(
                title=f"{file_name} - {region}",
                xaxis_title="Year",
                yaxis_title="Value",
                yaxis_tickformat=".2f",
                legend=dict(title="LAD Selections"),
            )

            self.add_dropdown(fig, lads, f"{file_name}_{region}", "LADNM")
            self.save_plot(fig, f"{file_name}_{region}", "LAD")

    def add_dropdown(self, fig, items, category, label):
        sorted_items = sorted(items)

        buttons = [
            dict(
                method="update",
                label="All",
                args=[
                    {"visible": [True] * len(fig.data)},
                    {"title": f"{category} - All {label}"},
                ],
            )
        ]

        for item in sorted_items:
            visibility = [trace.name.startswith(item) for trace in fig.data]
            buttons.append(
                dict(
                    method="update",
                    label=item,
                    args=[{"visible": visibility}, {"title": f"{category} - {item}"}],
                )
            )

        fig.update_layout(
            updatemenus=[
                {
                    "buttons": buttons,
                    "direction": "down",
                    "showactive": True,
                    "x": 1,
                    "xanchor": "left",
                    "y": 1.10,
                    "yanchor": "top",
                }
            ],
            title=f"{category}",
            xaxis_title="Year",
            yaxis_title="Value",
            yaxis_tickformat=".2f",
        )

    def save_plot(self, fig, category, sheet_name):
        # Determine the subfolder based on the category
        if "GrowthRate" in category:
            subfolder = "GrowthRate"
        elif "AnnualGRate" in category:
            subfolder = "AnnualGRate"
        elif "AbsoluteGrowth" in category:
            subfolder = "AbsoluteGrowth"
        elif "YearTotal" in category:
            subfolder = "YearTotal"
        elif "GrowthRatio" in category:
            subfolder = "GrowthRatio"
        else:
            subfolder = "Other"

        # Create the subfolder if it doesn't exist
        subfolder_path = os.path.join(self.output_dir, subfolder)
        os.makedirs(subfolder_path, exist_ok=True)

        # Save the plot in the appropriate subfolder
        output_file_html = os.path.join(
            subfolder_path, f"{category}_{sheet_name}_Trend.html"
        )
        fig.write_html(output_file_html)
        logging.info(f"Visualization saved as: {output_file_html}")


if __name__ == "__main__":
    input_folder = Path(
        r"I:\Data\D-Log\DLIT\Outputs\test22_v0.21\M5_constraint\output_for_viz"
    )
    output_dir = Path(
        r"I:\Data\D-Log\DLIT\Outputs\test22_v0.21\M5_constraint\output_for_viz\html_files"
    )

    visualizer = Visualizer(output_dir=output_dir)
    visualizer.generate_visualizations(input_folder)
