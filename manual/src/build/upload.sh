#!/bin/bash
set -e
set -x

DIR=$(cd `dirname $0` && cd ../../ && pwd)

# Uploads different formats of the manual to a public server.


# Which version the documentation is now.
VERSION=$(cat $DIR/target/src/version)

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
DOCS_SERVER='neo@static.neo4j.org'
ROOTPATHDOCS='/var/www/docs.neo4j.org'
hostname=$(uname -n)

# If you're not a Jenkins node, don't deploy the docs
#[ "${hostname}" == 'build1' ] &&  exit 0

echo "VERSION = $VERSION"
echo "SYMLINKVERSION = $SYMLINKVERSION"

# Create initial directories
ssh $DOCS_SERVER mkdir -p $ROOTPATHDOCS/{text,chunked}/$VERSION
#ssh $DOCS_SERVER mkdir -p $ROOTPATHDOCS/{text,chunked,annotated}/$VERSION

# Copy artifacts
rsync -r $DIR/target/text/ $DOCS_SERVER:$ROOTPATHDOCS/text/$VERSION/
#rsync -r --delete $DIR/target/annotated/ $DOCS_SERVER:$ROOTPATHDOCS/annotated/
rsync -r --delete $DIR/target/chunked/ $DOCS_SERVER:$ROOTPATHDOCS/chunked/$VERSION/
ssh $DOCS_SERVER mkdir -p $ROOTPATHDOCS/pdf
scp $DIR/target/pdf/neo4j-manual.pdf $DOCS_SERVER:$ROOTPATHDOCS/pdf/neo4j-manual-$VERSION.pdf

# Symlink this version to a generic url
#ssh $DOCS_SERVER "cd $ROOTPATHDOCS/text/ && (rm $SYMLINKVERSION || true); ln -s $VERSION $SYMLINKVERSION"
#ssh $DOCS_SERVER "cd $ROOTPATHDOCS/chunked/ && (rm $SYMLINKVERSION || true); ln -s $VERSION $SYMLINKVERSION"
#ssh $DOCS_SERVER "cd $ROOTPATHDOCS/pdf/ && (rm neo4j-manual-$SYMLINKVERSION.pdf || true); ln -s neo4j-manual-$VERSION.pdf neo4j-manual-$SYMLINKVERSION.pdf"

#if [[ $SYMLINKVERSION == stable ]]
#then
  #ssh $DOCS_SERVER "cd $ROOTPATHDOCS/text/ && (rm milestone || true); ln -s $VERSION milestone"
  #ssh $DOCS_SERVER "cd $ROOTPATHDOCS/chunked/ && (rm milestone || true); ln -s $VERSION milestone"
  #ssh $DOCS_SERVER "cd $ROOTPATHDOCS/pdf/ && (rm neo4j-manual-milestone.pdf || true); ln -s neo4j-manual-$VERSION.pdf neo4j-manual-milestone.pdf"
#fi


echo Apparently, successfully published to $DOCS_SERVER.


