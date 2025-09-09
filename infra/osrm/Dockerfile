FROM osrm/osrm-backend:latest

WORKDIR /data

COPY run.sh /usr/local/bin/osrm-build
RUN chmod +x /usr/local/bin/osrm-build

CMD bash -lc "osrm-build; osrm-routed --algorithm mld /data/monaco-latest.osrm"
