# This script fetches files from NOAA's FTP servers, checks for updates, and saves new versions.
#!/usr/bin/env python3
"""
-- coding: utf-8 --
"""

import os
import requests
import hashlib
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from pathlib import Path

# Konfiguration
TARGET_DIR = Path("/home/juri/obs-processing/NOAA")
TARGET_DIR.mkdir(parents=True, exist_ok=True)

SOURCES = {
    "https://tgftp.nws.noaa.gov/SL.us008001/DF.bf/DC.intl/": ".bin",
    "https://tgftp.nws.noaa.gov/SL.us008001/DF.an/DC.sflnd/DS.synop/": ".txt"
}

def get_links_from_directory(url, extension):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    links = []

    for link in soup.find_all('a'):
        href = link.get('href')
        if href and href.endswith(extension):
            links.append(urljoin(url, href))

    return links

def sha256sum(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(8192):
            h.update(chunk)
    return h.hexdigest()

def find_latest_version(name, ext):
    version = 0
    for file in TARGET_DIR.glob(f"{name}.*.{ext}"):
        try:
            idx = int(file.stem.split('.')[-1])
            if idx > version:
                version = idx
        except ValueError:
            continue
    return version

def process_file(file_url):
    filename = os.path.basename(file_url)
    name, ext = os.path.splitext(filename)
    ext = ext.lstrip(".")

    print(f"Prüfe: {filename}")

    response = requests.get(file_url)
    if response.status_code != 200 or len(response.content) == 0:
        print(f" - Datei leer oder nicht erreichbar: {file_url}")
        return

    temp_hash = hashlib.sha256(response.content).hexdigest()

    latest_version = find_latest_version(name, ext)
    if latest_version > 0:
        latest_file = TARGET_DIR / f"{name}.{latest_version}.{ext}"
        if sha256sum(latest_file) == temp_hash:
            print(f" - Keine Änderung an {filename}")
            return

    new_version = latest_version + 1
    new_file = TARGET_DIR / f"{name}.{new_version}.{ext}"
    with open(new_file, "wb") as f:
        f.write(response.content)
    print(f" - Neue Version gespeichert als {new_file.name}")

def main():
    for url, ext in SOURCES.items():
        print(f"\nVerzeichnis: {url}")
        try:
            links = get_links_from_directory(url, ext)
        except Exception as e:
            print(f"Fehler beim Abrufen von {url}: {e}")
            continue

        for file_url in links:
            try:
                process_file(file_url)
            except Exception as e:
                print(f"Fehler bei Datei {file_url}: {e}")

if __name__ == "__main__":
    main()
