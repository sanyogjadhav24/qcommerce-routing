#!/bin/bash
set -euo pipefail

# Download a tiny demo map (Monaco) and build OSRM files if missing
if [ ! -f /data/monaco-latest.osm.pbf ]; then
  echo "Downloading Monaco extract..."
  if command -v curl >/dev/null 2>&1; then
    curl -L -s https://download.geofabrik.de/europe/monaco-latest.osm.pbf -o /data/monaco-latest.osm.pbf
  elif command -v wget >/dev/null 2>&1; then
    wget -q -O /data/monaco-latest.osm.pbf https://download.geofabrik.de/europe/monaco-latest.osm.pbf
  else
    echo "Neither curl nor wget found in image" >&2
    exit 1
  fi
fi

if [ ! -f /data/monaco-latest.osrm ]; then
  echo "Building OSRM datasets..."
  osrm-extract -p /opt/car.lua /data/monaco-latest.osm.pbf
  osrm-partition /data/monaco-latest.osrm
  osrm-customize /data/monaco-latest.osrm
fi
