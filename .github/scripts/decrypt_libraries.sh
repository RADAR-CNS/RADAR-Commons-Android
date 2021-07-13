#!/bin/sh

set -e

gpg --quiet --batch --yes --decrypt --passphrase="$E4LINK_PASSPHRASE" \
  --output plugins/radar-android-empatica/libs/E4link-1.0.0.aar .github/libs/E4link-1.0.0.aar.gpg
