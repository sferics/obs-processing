#!/bin/bash
local_tables=/bufr_tables/dwd

export METVIEW_EXTRA_GRIB_DEFINITION_PATH=${local_tables}
export ECCODES_DEFINITION_PATH=`pwd`${local_tables}:${CONDA_PREFIX}/share/eccodes/definitions
codes_info -d; echo
