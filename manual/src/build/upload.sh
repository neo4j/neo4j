#!/bin/bash
set -e
set -x

DIR=$(cd `dirname $0` && cd ../../ && pwd)

# Uploads different formats of the manual to a public server.


# Which version the documentation is now.
VERSION=$(cat $DIR/target/classes/version)

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
hostname=$(uname -n)

# If you're not a Jenkins node, don't deploy the docs
#[ "${hostname}" == 'build1' ] &&  exit 0

echo "VERSION = $VERSION"
echo "SYMLINKVERSION = $SYMLINKVERSION"

# Create initial directories
ssh docs-server mkdir -p $ROOTPATHDOCS/{text,chunked}/$VERSION
#ssh docs-server mkdir -p $ROOTPATHDOCS/{text,chunked,annotated}/$VERSION

# Copy artifacts
rsync -r $DIR/target/text/ docs-server:$ROOTPATHDOCS/text/$VERSION/
#rsync -r --delete $DIR/target/annotated/ docs-server:$ROOTPATHDOCS/annotated/
rsync -r --delete $DIR/target/chunked/ docs-server:$ROOTPATHDOCS/chunked/$VERSION/
scp $DIR/target/pdf/neo4j-manual.pdf docs-server:$ROOTPATHDOCS/pdf/neo4j-manual-$VERSION.pdf

# Symlink this version to a generic url
#ssh docs-server "cd $ROOTPATHDOCS/text/ && (rm $SYMLINKVERSION || true); ln -s $VERSION $SYMLINKVERSION"
#ssh docs-server "cd $ROOTPATHDOCS/chunked/ && (rm $SYMLINKVERSION || true); ln -s $VERSION $SYMLINKVERSION"
#ssh docs-server "cd $ROOTPATHDOCS/pdf/ && (rm neo4j-manual-$SYMLINKVERSION.pdf || true); ln -s neo4j-manual-$VERSION.pdf neo4j-manual-$SYMLINKVERSION.pdf"

#if [[ $SYMLINKVERSION == stable ]]
#then
  #ssh docs-server "cd $ROOTPATHDOCS/text/ && (rm milestone || true); ln -s $VERSION milestone"
  #ssh docs-server "cd $ROOTPATHDOCS/chunked/ && (rm milestone || true); ln -s $VERSION milestone"
  #ssh docs-server "cd $ROOTPATHDOCS/pdf/ && (rm neo4j-manual-milestone.pdf || true); ln -s neo4j-manual-$VERSION.pdf neo4j-manual-milestone.pdf"
#fi


echo Apparently, successfully published to docs-server.

