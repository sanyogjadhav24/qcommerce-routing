FROM osrm/osrm-backend:latest

RUN apt-get update && apt-get install -y --no-install-recommends wget ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /data

COPY run.sh /usr/local/bin/osrm-build
RUN chmod +x /usr/local/bin/osrm-build

CMD bash -lc "osrm-build; osrm-routed --algorithm mld /data/monaco-latest.osrm"
