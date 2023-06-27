# Retrieving the file from one hour ago and logging deprecation

import logging
import os
import sys
from datetime import datetime as dt, timedelta as td

import requests

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))

api_url = "https://api.dataplatform.knmi.nl/open-data"
#api_url = "https://api.dataplatform.knmi.nl/open-data/v1/datasets/knmi_synop_bufr/versions/1/files"
api_version = "v1"


def main(dataset_name, dataset_version, filename):
    # Parameters
    api_key = "eyJvcmciOiI1ZTU1NGUxOTI3NGE5NjAwMDEyYTNlYjEiLCJpZCI6IjA2MTU4MDU3YmM2MDRkMjA5OTExN2ZjOWJkY2IyZThiIiwiaCI6Im11cm11cjEyOCJ9"
    logger.debug(f"Dataset file to download: {filename}")

    endpoint = f"{api_url}/{api_version}/datasets/{dataset_name}/versions/{dataset_version}/files/{filename}/url"
    get_file_response = requests.get(endpoint, headers={"Authorization": api_key})

    if get_file_response.status_code != 200:
        logger.error("Unable to retrieve download url for file")
        logger.error(get_file_response.text)
        sys.exit(1)

    logger.info(f"Successfully retrieved temporary download URL for dataset file {filename}")

    download_url = get_file_response.json().get("temporaryDownloadUrl")
    # Check logging for deprecation
    if "X-KNMI-Deprecation" in get_file_response.headers:
        deprecation_message = get_file_response.headers.get("X-KNMI-Deprecation")
        logger.warning(f"Deprecation message: {deprecation_message}")

    download_file_from_temporary_download_url(download_url, filename)


def download_file_from_temporary_download_url(download_url, filename):
    try:
        with requests.get(download_url, stream=True) as r:
            r.raise_for_status()
            with open("KNMI/"+filename, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
    except Exception:
        logger.exception("Unable to download file using download URL")
        sys.exit(1)

    logger.info(f"Successfully downloaded dataset file to {filename}")


if __name__ == "__main__":
    # Use get file to retrieve a file from one hour ago.
    date = dt.utcnow()
    YY =  date.year; MM = date.month; DD = date.day; hh = date.hour; mm = int( date.minute / 10 ) * 10
    main("knmi_synop_bufr", 1, f"SYNOP_BUFR_{DD:02d}{hh:02d}.bufr")
    try:
        main("Actuele10mindataKNMIstations", 2, f"KMDS__OPER_P___10M_OBS_L2_{YY}{MM:02d}{DD:02d}{hh:02d}{mm:02d}.nc")
    except:
        mm -= 10
        main("Actuele10mindataKNMIstations", 2, f"KMDS__OPER_P___10M_OBS_L2_{YY}{MM:02d}{DD:02d}{hh:02d}{mm:02d}.nc")
