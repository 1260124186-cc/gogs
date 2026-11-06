#!/usr/bin/env bash
#
# generate-release-manifest.sh builds the asset manifest that is consumed by
# sync-release-assets.sh. Every regular file directly inside the assets
# directory becomes one entry with its file name and SHA-256 digest.
#
# Usage:
#   generate-release-manifest.sh ASSETS_DIR MANIFEST_PATH
#
# The manifest is deterministic, entries are sorted by file name and no
# timestamp is written, so generating twice over the same files produces an
# identical manifest.

set -euo pipefail

usage() {
	sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'
	exit 1
}

fail() {
	echo "generate-release-manifest: $*" >&2
	exit 1
}

if [ "$#" -ne 2 ]; then
	usage
fi

assets_dir=$1
manifest_path=$2

[ -d "$assets_dir" ] || fail "assets directory does not exist: $assets_dir"

if command -v sha256sum >/dev/null 2>&1; then
	file_sha() {
		sha256sum "$1" | awk '{print $1}'
	}
elif command -v shasum >/dev/null 2>&1; then
	file_sha() {
		shasum -a 256 "$1" | awk '{print $1}'
	}
else
	fail "neither sha256sum nor shasum is available"
fi

command -v jq >/dev/null 2>&1 || fail "jq is required"

entries='[]'
count=0
while IFS= read -r -d '' file; do
	name=${file##*/}
	digest=$(file_sha "$file")
	entries=$(jq --arg name "$name" --arg sha256 "$digest" \
		'. + [{name: $name, sha256: $sha256}]' <<< "$entries")
	count=$((count + 1))
done < <(find "$assets_dir" -maxdepth 1 -type f -print0)

[ "$count" -gt 0 ] || fail "no files found in assets directory: $assets_dir"

tmp_path=${manifest_path}.tmp
jq -n --argjson assets "$entries" '{version: 1, assets: ($assets | sort_by(.name))}' > "$tmp_path"
mv "$tmp_path" "$manifest_path"

echo "generate-release-manifest: wrote $count entries to $manifest_path"
