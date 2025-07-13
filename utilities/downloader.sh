#!/usr/bin/bash

# The first argument optionally specifies the base landing zone path. If not
# provided, "./landing" is used which mirrors the expected local layout.
# Ensure we are always working with an absolute path so that any internal
# directory changes do not break later file operations.
landing_root="${1:-./landing}"
landing_root="$(realpath "$landing_root")"

tmp=$(mktemp -d /tmp/data.XXXX)
root="$landing_root/data"
marker_root="$landing_root/markers"

install -dv "$tmp" "$root" "$marker_root"
touch "$marker_root/marker"

download(){
    ts_check="$1"
    shift
    tmp_save_dir="$tmp/$(date +%Y%m%d)"
    root_save_dir="$root/$(date +%Y%m%d)"
    check_dir="$marker_root/$ts_check"
    if [[ ! -d "$check_dir" ]]; then
        install -dv "$tmp_save_dir" "$root_save_dir" "$check_dir"
        touch "$check_dir/marker"
        (
            cd "$tmp_save_dir"
            for url in "$@"; do
                filename=$(basename "$url" .gz)
                wget -nv -O - "$url" \
                    | gunzip -c \
                    | sed \
                        -e '1s/^\[//' \
                        -e '$s/]$//' \
                        -e 's/^[[:space:]]*//' \
                        -e 's/},$/}/' > "$filename"
                cat "$tmp_save_dir/$filename" > "$root_save_dir/$filename"
            done
        )
    fi
}

# yearly example
download "$(date +%Y)" https://www.edsm.net/dump/systemsWithCoordinates.json.gz

# monthly example
# download "$(date +%Y%m)" https://www.edsm.net/dump/systemsWithCoordinates.json.gz

# Weekly example
download "$(date +%Y)_week$(date +%U)" https://www.edsm.net/dump/stations.json.gz \
    https://www.edsm.net/dump/codex.json.gz

# daily
download "$(date +%Y%m%d)" https://www.edsm.net/dump/powerPlay.json.gz \
    https://www.edsm.net/dump/systemsPopulated.json.gz \
    https://www.edsm.net/dump/bodies7days.json.gz \
    https://www.edsm.net/dump/systemsWithCoordinates7days.json.gz \
    https://www.edsm.net/dump/systemsWithoutCoordinates.json.gz













