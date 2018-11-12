#! /bin/bash

# Fail if something fails
set -e

# Set snapshot version
export SNAPSHOT_VERSION=9.0-SNAPSHOT

# Reset the version on Maven modules
mvn versions:set -DnewVersion=$SNAPSHOT_VERSION -DgenerateBackupPoms=false

# Also set the version of the licensing module
mvn -f build/pom.xml versions:set -DnewVersion=$SNAPSHOT_VERSION -DgenerateBackupPoms=false

# Make a snapshot commit
git commit -am "Back to $SNAPSHOT_VERSION"

# Push the release commit to GitHub
git push origin 9.0
