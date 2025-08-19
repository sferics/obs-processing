#!/bin/bash

mkdir -p AEMET

API_KEY="eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJqdXJpQG1zd3IuZGUiLCJqdGkiOiJhYzkyNWFlNC1lODdhLTQzYjktOWExMC1lYjRmNzI2MzhjMzkiLCJpc3MiOiJBRU1FVCIsImlhdCI6MTc0ODAzMzgxMSwidXNlcklkIjoiYWM5MjVhZTQtZTg3YS00M2I5LTlhMTAtZWI0ZjcyNjM4YzM5Iiwicm9sZSI6IiJ9.JGrwNK7N0_rxCEc1rycQc8HNTlZEjXFbaS9iKs2y-8o"

wget -q -O AEMET/observations.json curl -X 'GET' \
  "https://opendata.aemet.es/opendata/api/observacion/convencional/mensajes/tipomensaje/synop?api_key=${API_KEY}" \
  -H 'accept: application/json'


