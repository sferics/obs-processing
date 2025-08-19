#!/bin/bash

set -euo pipefail

# Zielverzeichnis
TARGET_DIR="/home/juri/obs-processing/NOAA"
mkdir -p "$TARGET_DIR"
cd "$TARGET_DIR" || exit 1

# Liste der Verzeichnisse und Erweiterungen
declare -A SOURCES
SOURCES["https://tgftp.nws.noaa.gov/SL.us008001/DF.bf/DC.intl/"]=".bin"
SOURCES["https://tgftp.nws.noaa.gov/SL.us008001/DF.an/DC.sflnd/DS.synop/"]=".txt"

# Temp-Verzeichnis für Downloads
TMP_DIR=$(mktemp -d)

# HTML-Linkparser (greift echte Dateinamen heraus)
extract_links() {
    local url=$1
    local ext=$2
    curl -s "$url" | grep -oP 'href="\K[^"]+' | grep "$ext" | while read -r filename; do
        echo "$url$filename"
    done
}

# Hauptlogik
for URL in "${!SOURCES[@]}"; do
    EXT="${SOURCES[$URL]}"
    echo "Verarbeite $URL"

    # Für jede Datei in diesem Verzeichnis
    extract_links "$URL" "$EXT" | while read -r FILE_URL; do
        FILENAME=$(basename "$FILE_URL")
        NAME="${FILENAME%.*}"
        EXT="${FILENAME##*.}"

        TMP_FILE="$TMP_DIR/$FILENAME"
        curl -s -o "$TMP_FILE" "$FILE_URL"

        # Prüfen, ob leer
        if [[ ! -s "$TMP_FILE" ]]; then
            echo "Warnung: $FILENAME ist leer – übersprungen"
            continue
        fi

        # Letzte gespeicherte Version finden
        LATEST_FILE=$(ls -1 "$TARGET_DIR"/"$NAME".*."$EXT" 2>/dev/null | sort -V | tail -n 1)

        if [[ -f "$LATEST_FILE" ]] && cmp -s "$TMP_FILE" "$LATEST_FILE"; then
            echo "Keine Änderung an $FILENAME"
            continue
        fi

        # Neue Version speichern
        INDEX=1
        while [[ -f "$TARGET_DIR/$NAME.$INDEX.$EXT" ]]; do
            ((INDEX++))
        done

        cp "$TMP_FILE" "$TARGET_DIR/$NAME.$INDEX.$EXT"
        echo "Neue Version gespeichert als $NAME.$INDEX.$EXT"
    done
done

rm -rf "$TMP_DIR"
echo "Alle Dateien wurden verarbeitet und gespeichert."
