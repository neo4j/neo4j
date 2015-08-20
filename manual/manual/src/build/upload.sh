#!/bin/bash
set -e
set -x

DIR=$(cd `dirname $0` && cd ../../ && pwd)

# Uploads different formats of the manual to a public server.


# Which version the documentation is now.
VERSION=$(cat $DIR/target/contents/src/version)

# Name of the symlink created on the docs server, pointing to this version.
if [[ $VERSION == *SNAPSHOT* ]]
then
	SYMLINKVERSION=snapshot
else
	if [[ $VERSION == *M* ]]
	then
		SYMLINKVERSION=milestone
	else
		SYMLINKVERSION=stable
	fi
fi
DOCS_SERVER='neo@outreach.neotechnology.com'
ROOTPATHDOCS='/data/www/doc/docs.neo4j.org'
hostname=$(uname -n)

echo "VERSION = $VERSION"
echo "SYMLINKVERSION = $SYMLINKVERSION"

# Create initial directories
ssh $DOCS_SERVER mkdir -p $ROOTPATHDOCS/chunked/$VERSION

# Copy artifacts
rsync -r --delete $DIR/target/docbkx/webhelp/ $DOCS_SERVER:$ROOTPATHDOCS/chunked/$VERSION/
ssh $DOCS_SERVER mkdir -p $ROOTPATHDOCS/pdf
scp $DIR/target/docbkx/pdf/neo4j-manual-shortinfo.pdf $DOCS_SERVER:$ROOTPATHDOCS/pdf/neo4j-manual-$VERSION.pdf

echo Apparently, successfully published to $DOCS_SERVER.

