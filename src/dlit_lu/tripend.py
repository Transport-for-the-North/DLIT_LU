import pandas as pd
import logging
from typing import Dict, Any
import numpy as np

# Local imports
from dlit_lu import inputs, utilities

LOG = logging.getLogger(__name__)

class Tripends():

    def load_data(self, config: inputs.TripendsConfig): 
        soc_sic = pd.read_csv(config.zone_soc_sic_emp)
        print(soc_sic)
        tt = pd.read_csv(config.zone_tt_pop)
        print(tt)
        
        return soc_sic, tt  

def run(config: inputs.DLitConfig):

    if config.run_tripend is None:
        raise ValueError("Cannot run tripend module without any tripend parameters")

    LOG.info("Initialising Tripend Module")

    config.output_folder.mkdir(exist_ok=True)

    key_constraint_path = config.output_folder / "07_tripend"
    key_constraint_path.mkdir(exist_ok=True)

    tripend = Tripends()
    soc_sic, tt = tripend.load_data(config.tripend)  

    # Export the loaded data to CSV files in the 07_tripend folder
    soc_sic.to_csv(key_constraint_path / "zone_soc_sic_emp.csv", index=False)
    tt.to_csv(key_constraint_path / "zone_tt_pop.csv", index=False)

    LOG.info("Data exported to 07_tripend folder")