#!/bin/sh

# abort on any non-zero exit code
set -e

# ensure required environment variables are defined
if [ -z "$VERSION" ]; then
  echo "\$VERSION is required"
  exit 1
fi

# install deno
DENO_VERSION="v1.38.3"
curl -fsSL https://deno.land/x/install/install.sh | DENO_INSTALL=./deno-$DENO_VERSION sh -s $DENO_VERSION

# run unit tests as a sanity check
NO_COLOR=1 ./deno-$DENO_VERSION/bin/deno test --allow-read --allow-write --unstable

# build root package, then publish it (and native subpackages)
NO_COLOR=1 ./deno-$DENO_VERSION/bin/deno run --unstable --allow-all ./src/scripts/build_npm.ts $VERSION --napi=$VERSION --publish=$(which npm) ${DRY_RUN:+--dry-run}
