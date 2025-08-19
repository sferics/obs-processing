#!/bin/bash

# URL zur Datei
URL="https://example.com/pfad/datei.ext"
# Basisname für lokale Datei
BASENAME="datei"
# Erweiterung
EXT="ext"
# Zielverzeichnis
TARGET_DIR="/pfad/zum/zielverzeichnis"

# Temporäre Datei herunterladen
TMP_FILE=$(mktemp)

# Datei vom Server herunterladen (mit Zeitstempelprüfung)
wget -q --output-document="$TMP_FILE" "$URL"

# Prüfen, ob bereits frühere Versionen existieren
cd "$TARGET_DIR" || exit 1

# Prüfen, ob neue Datei identisch mit letzter Version ist
LATEST_FILE=$(ls -1 ${BASENAME}.*.${EXT} 2>/dev/null | sort -V | tail -n 1)

if [[ -f "$LATEST_FILE" ]]; then
    # Vergleich per Checksumme
    if cmp -s "$TMP_FILE" "$LATEST_FILE"; then
        echo "Keine Änderung an $URL."
        rm "$TMP_FILE"
        exit 0
    fi
fi

# Neue Version speichern
NEXT_INDEX=1
while [[ -e "${BASENAME}.${NEXT_INDEX}.${EXT}" ]]; do
    ((NEXT_INDEX++))
done

NEW_FILE="${BASENAME}.${NEXT_INDEX}.${EXT}"
mv "$TMP_FILE" "$NEW_FILE"
echo "Neue Version gespeichert als $NEW_FILE"
# Optional: Alte Versionen aufräumen
find . -name "${BASENAME}.*.${EXT}" -type f -mtime +30 -exec rm {} \;
# Ende des Skripts
# Hinweis: Dieses Skript setzt voraus, dass `wget` und `cmp` installiert sind.
