#!/bin/bash

#https://dataplatform.knmi.nl/dataset/access/knmi-synop-bufr-1

API_KEY=eyJvcmciOiI1ZTU1NGUxOTI3NGE5NjAwMDEyYTNlYjEiLCJpZCI6IjA2MTU4MDU3YmM2MDRkMjA5OTExN2ZjOWJkY2IyZThiIiwiaCI6Im11cm11cjEyOCJ9

curl --location --request GET -G "https://api.dataplatform.knmi.nl/open-data/v1/datasets/knmi_synop_bufr/versions/1/files" -d maxKeys=10 --header "Authorization: ${API_KEY}"
