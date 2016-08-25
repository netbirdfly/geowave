#!/bin/bash

if [ "$TRAVIS_REPO_SLUG" == "ngageoint/geowave" ] && $BUILD_DOCS; then
  echo -e "Building docs...\n"
  mvn -e -X -P docs -pl docs install
fi