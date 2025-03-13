import pandas as pd
import logging
from typing import Dict, Any
import numpy as np
from pathlib import Path

# Local imports
from dlit_lu import inputs, utilities

LOG = logging.getLogger(__name__)

class Tripends():
    def __init__(self):
        pass

def run(config: inputs.DLitConfig):
    if config.tripend is None:
        raise ValueError("Cannot run tripend module without any tripend parameters")

    LOG.info("Initialising Tripend Module")

    config.output_folder.mkdir(exist_ok=True)

    key_constraint_path = config.output_folder / "07_tripend"
    key_constraint_path.mkdir(exist_ok=True)

    soc_sic = pd.read_csv(config.tripend.zone_soc_sic_emp)
    tt = pd.read_csv(config.tripend.zone_tt_pop)

    # Export the loaded data to CSV files in the 07_tripend folder
    soc_sic.to_csv(key_constraint_path / "zone_soc_sic_emp.csv", index=False)
    tt.to_csv(key_constraint_path / "zone_tt_pop.csv", index=False)

    LOG.info("Data exported to 07_tripend folder")

    tripend = Tripends()

if __name__ == "__main__":
    
    tripend_config = inputs.TripendsConfig(
        zone_soc_sic_emp=r"I:\Data\D-Log\DLIT\Outputs\Test14_DLog24_regression_no_negatives\05_normits\normits_zonal_new_job_by_sic_soc.csv.bz2",
        zone_tt_pop=r"I:\Data\D-Log\DLIT\Outputs\Test14_DLog24_regression_no_negatives\05_normits\normits_zonal_new_population_by_tt.csv.bz2"
    )
    dlit_config = inputs.DLitConfig(
        tripend=tripend_config,
        output_folder = r"I:\Data\D-Log\DLIT\Outputs\Test14_DLog24_regression_no_negatives\07_tripend"
    )

    run(dlit_config)