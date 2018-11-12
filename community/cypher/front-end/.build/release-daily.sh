#! /bin/bash

# Fail if something fails
set -e

# Compute next release version (date)
export RELEASE_VERSION
RELEASE_VERSION=9.0.$(date -u +%Y%m%d)
echo RELEASE_VERSION=$RELEASE_VERSION

# Set the new version on Maven modules
mvn versions:set -DnewVersion=$RELEASE_VERSION -DgenerateBackupPoms=false

# Also set the version of the licensing module
mvn -f build/pom.xml versions:set -DnewVersion=$RELEASE_VERSION -DgenerateBackupPoms=false

# Export to TeamCity environment variable
echo "##teamcity[setParameter name='env.RELEASE_VERSION' value='$RELEASE_VERSION']"

# Make a release commit
git commit -am "Release version $RELEASE_VERSION"

# Find the git SHA of the commit
export RELEASE_COMMIT=$(git rev-parse HEAD)

echo "Release commit is $RELEASE_COMMIT"

# Export to TeamCity environment variable
echo "##teamcity[setParameter name='env.RELEASE_COMMIT' value='$RELEASE_COMMIT']"
