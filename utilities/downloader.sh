#!/usr/bin/env bash

set -euo pipefail

root="/home/sqltest/edsm-data"
install -dv "$root"
log="$root/TRANSFER_$(date +%Y%m%d%H%M).LOG"

exec 1>>"$log"
exec 2>>"$log"

download(){
    ts="$1"
    shift
    dest_dir="$root/$ts"
    if [[ ! -d "$dest_dir" ]]; then
        install -dv "$dest_dir"
        (   cd "$dest_dir"
            for url in "$@"; do
                wget -v -O - "$url" | gunzip -cv > "$(basename "$url" .gz)"
            done
        )
    fi
}

# # yearly
# download "$(date +%Y)" \
#     "https://www.edsm.net/dump/systemsWithCoordinates.json.gz"

# # monthly
# download "$(date +%Y%m)" \
#     "https://www.edsm.net/dump/stations.json.gz"
#
# # weekly
# download "$(date +%Y)_week$(date +%U)" \
#     "https://www.edsm.net/dump/systemsWithCoordinates7days.json.gz"

# daily
download "$(date +%Y%m%d)" "https://www.edsm.net/dump/powerPlay.json.gz" &
# download "$(date +%Y%m%d)" "https://www.edsm.net/dump/systemsPopulated.json.gz" &
# download "$(date +%Y%m%d)" "https://www.edsm.net/dump/codex.json.gz" &
# download "$(date +%Y%m%d)" "https://www.edsm.net/dump/bodies7days.json.gz" &
# download "$(date +%Y%m%d)" "https://www.edsm.net/dump/stations.json.gz" &
# download "$(date +%Y%m%d)" "https://www.edsm.net/dump/systemsWithCoordinates7days.json.gz" &
# download "$(date +%Y%m%d)" "https://www.edsm.net/dump/systemsWithoutCoordinates.json.gz" &
wait


# # hourly
# download "$(date +%Y%m%d%H)" \
#     "https://www.edsm.net/dump/systemsWithoutCoordinates.json.gz"