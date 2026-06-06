#!/usr/bin/env bash
# Release helpers for the denokv workspace, used by the release
# workflow and runnable locally.
#
#   scripts/release.sh bump <version>    rewrite crate versions and the
#                                        lockfile to <version>
#   scripts/release.sh notes <version>   print release notes for the
#                                        range <last tag>..HEAD
set -euo pipefail

cd "$(dirname "$0")/.."

usage() {
  echo "usage: $0 bump|notes <version>" >&2
  exit 1
}

[ $# -eq 2 ] || usage
command=$1
version=$2

if ! grep -qE '^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$' <<< "$version"; then
  echo "invalid version: $version" >&2
  exit 1
fi

case "$command" in
  bump)
    for crate in denokv proto remote sqlite timemachine; do
      sed -i.bak -E "s/^version = \"[^\"]+\"$/version = \"$version\"/" \
        "$crate/Cargo.toml"
      rm "$crate/Cargo.toml.bak"
    done
    sed -i.bak -E \
      "s/^(denokv_[a-z]+ = \{ version = )\"[^\"]+\"/\1\"$version\"/" \
      Cargo.toml
    rm Cargo.toml.bak
    cargo update --workspace --quiet
    ;;
  notes)
    prev=$(git describe --tags --abbrev=0 HEAD)
    echo "## What's Changed"
    git log --oneline --no-decorate --no-merges "$prev..HEAD" \
      | sed -E 's/^[0-9a-f]+ /* /'
    echo
    echo "**Full Changelog**: https://github.com/denoland/denokv/compare/$prev...$version"
    ;;
  *)
    usage
    ;;
esac
