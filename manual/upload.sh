#!/bin/bash
set -e

# Deploys build artifacts to relevant servers

if [[ $CD ]]
then
	cd manual
fi


# Which version the documentation is now.
VERSION=$(cat target/classes/version)

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

ROOTPATHDOCS=public_html/docs
ROOTPATHDIST=public_html/dist

echo "VERSION = $VERSION"
echo "SYMLINKVERSION = $SYMLINKVERSION"

# Create initial directories
ssh docs-server mkdir -p $ROOTPATHDOCS/{text,chunked,annotated}/$VERSION

# Copy artifacts
rsync -r target/text/ docs-server:$ROOTPATHDOCS/text/$VERSION/
rsync -r --delete target/annotated/ docs-server:$ROOTPATHDOCS/annotated/
rsync -r --delete target/chunked/ docs-server:$ROOTPATHDOCS/chunked/$VERSION/
scp target/pdf/neo4j-manual.pdf dist-server:$ROOTPATHDIST/neo4j-manual-$VERSION.pdf

# Symlink this version to a generic url
ssh docs-server "cd $ROOTPATHDOCS/text/ && (rm $SYMLINKVERSION || true); ln -s $VERSION $SYMLINKVERSION"
ssh docs-server "cd $ROOTPATHDOCS/chunked/ && (rm $SYMLINKVERSION || true); ln -s $VERSION $SYMLINKVERSION"
ssh dist-server "cd $ROOTPATHDIST && (rm neo4j-manual-$SYMLINKVERSION.pdf || true); ln -s neo4j-manual-$VERSION.pdf neo4j-manual-$SYMLINKVERSION.pdf"

echo Apparently, successfully copied artifacts to docs-server and dist-server.

if [[ $CD ]]
then
	cd ..
fi

