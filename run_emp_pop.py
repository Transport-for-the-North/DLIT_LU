"""
runs the DLIT_LU tool
"""
import sys
import argparse

if "src" not in sys.path:
    sys.path.append("src")

from dlit_lu import pop_jobs_processing

parser = argparse.ArgumentParser(description="Process some integers.")
parser.add_argument(
    "-c", "--config", help="Config file path", default="jobs_population_config.yml", type=str
)

args = parser.parse_args()

if __name__ == "__main__":
    pop_jobs_processing.run(args)
