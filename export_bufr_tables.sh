#!/bin/bash
local_tables=/home/juri/bufrtables/dwd

conda env config vars set METVIEW_EXTRA_GRIB_DEFINITION_PATH=${local_tables} ECCODES_DEFINITION_PATH=${local_tables}:${CONDA_PREFIX}/share/eccodes/definitions ECMWFLIBS_ECCODES_DEFINITION_PATH=${local_tables}:${CONDA_PREFIX}/share/eccodes/definitions && source /home/juri/miniconda3/etc/profile.d/conda.sh && conda activate $CONDA_DEFAULT_ENV

export METVIEW_EXTRA_GRIB_DEFINITION_PATH=${local_tables}
export ECCODES_DEFINITION_PATH=${local_tables}:${CONDA_PREFIX}/share/eccodes/definitions
export ECMWFLIBS_ECCODES_DEFINITION_PATH=${local_tables}:${CONDA_PREFIX}/share/eccodes/definitions
codes_info -d; echo
