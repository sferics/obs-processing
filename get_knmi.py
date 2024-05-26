#!/usr/bin/env python
# Retrieving the file from one hour ago and logging deprecation

import os
import sys
from datetime import datetime as dt, timedelta as td
import requests
import global_functions as gf
from config import ConfigClass


def main(dataset_name, dataset_version, filename):
    
    # Parameters
    log.debug(f"Dataset file to download: {filename}")

    endpoint = f"{api_url}/{api_ver}/datasets/{dataset_name}/versions/{dataset_version}/files/{filename}/url"

    get_file_response = requests.get(endpoint, headers={"Authorization": api_key})

    if get_file_response.status_code != 200:
        log.error("Unable to retrieve download url for file")
        log.error(get_file_response.text)
        sys.exit(1)

    log.info(f"Successfully retrieved temporary download URL for dataset file {filename}")

    download_url = get_file_response.json().get("temporaryDownloadUrl")
    # Check logging for deprecation
    if "X-KNMI-Deprecation" in get_file_response.headers:
        deprecation_message = get_file_response.headers.get("X-KNMI-Deprecation")
        log.warning(f"Deprecation message: {deprecation_message}")

    gf.create_dir(output_dir)

    download_file_from_temporary_download_url(download_url, filename)


def download_file_from_temporary_download_url(download_url, filename):
    try:
        with requests.get(download_url, stream=True) as r:
            r.raise_for_status()
            with open(output_dir+"/"+filename, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
    except Exception:
        log.exception("Unable to download file using download URL")
        sys.exit(1)
    
    log.info(f"Successfully downloaded dataset file to {filename}")


if __name__ == "__main__":

    script_name = gf.get_script_name(__file__)
    log         = gf.get_logger(script_name)
    flags       = ("l","v","d","m","o","p","r","a")
    info        = "Download the latest BUFR and NetCDF files from KNMI Open Data using their API."
    cf          = ConfigClass(script_name, flags=flags, info=info, sources=True)
    knmi_cf     = cf.sources["KNMI"]
    api_cf      = knmi_cf["api"]
    api_url     = api_cf["url"]
    api_ver     = api_cf["ver"]
    api_key     = api_cf["key"]

    output_dir  = cf.script["download_dir"]
    parse       = cf.script["parse"]
    verbose     = cf.script["verbose"]
    redo        = cf.script["redo"]
    approach    = cf.script["approach"]

    # Use get file to retrieve a file from one hour ago.
    date    = dt.utcnow()
    YY      = date.year
    MM      = date.month
    DD      = date.day
    hh      = date.hour
    mm      = date.minute // 10 * 10
    
    try:
        main("knmi_synop_bufr", 1, f"SYNOP_BUFR_{DD:02d}{hh:02d}.bufr")
    except:
        hh -= 1
        main("knmi_synop_bufr", 1, f"SYNOP_BUFR_{DD:02d}{hh:02d}.bufr")

    try:
        main("Actuele10mindataKNMIstations", 2, f"KMDS__OPER_P___10M_OBS_L2_{YY}{MM:02d}{DD:02d}{hh:02d}{mm:02d}.nc")
    except:
        mm -= 10
        try:
            main("Actuele10mindataKNMIstations", 2, f"KMDS__OPER_P___10M_OBS_L2_{YY}{MM:02d}{DD:02d}{hh:02d}{mm:02d}.nc")
        except:
            mm -= 10
            main("Actuele10mindataKNMIstations", 2, f"KMDS__OPER_P___10M_OBS_L2_{YY}{MM:02d}{DD:02d}{hh:02d}{mm:02d}.nc")

    if parse:
        import subprocess
        call = ["python", "decode_bufr.py", "KNMI"]
        if redo:
            call.append("-r")
        if verbose:
            call.append("-v")
        if approach:
            call += ["-a", approach]
        subprocess.run(call)
