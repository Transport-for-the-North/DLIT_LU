"""
runs the DLIT_LU tool
"""

import sys
import argparse
import yaml

if "src" not in sys.path:
    sys.path.append("src")

from dlit_lu import main

parser = argparse.ArgumentParser(description="Process some integers.")
parser.add_argument(
    "-c", "--config", help="Config file path", default="d_lit-config_v0.1.yml", type=str
)
parser.add_argument(
    "-m",
    "--maps",
    help="Whether the tool should plot maps displaying"
    " the results of the data reports",
    type=bool,
    default=False,
)
parser.add_argument(
    "-i",
    "--initial_report",
    help="Whether the tool should output a data report" " on the inputted DLog",
    type=bool,
    default=True,
)

parser.add_argument(
    "-o",
    "--output_folder",
    help="Specify output folder dynamically",
    type=str,
    default=r"I:\Data\D-Log\DLIT\Outputs\Test4",  # Keep the default, remove required=True
)

args = parser.parse_args()

# Load YAML Config and Replace Placeholders
with open(args.config, "r") as f:
    config = yaml.safe_load(f)


def replace_placeholders(obj, placeholder, value):
    """Recursively replace placeholder values in YAML config."""
    if isinstance(obj, dict):
        return {k: replace_placeholders(v, placeholder, value) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [replace_placeholders(v, placeholder, value) for v in obj]
    elif isinstance(obj, str):
        return obj.replace(placeholder, value)
    return obj


# Replace the output folder placeholder
config = replace_placeholders(config, "__OUTPUT_FOLDER__", args.output_folder)

if __name__ == "__main__":
    main.run(args)
