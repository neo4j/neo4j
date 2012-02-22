#!/bin/sh
dest='target/repo'
mkdir -p $dest/binary
cp target/classes/*.deb $dest
( cd $dest && dpkg-scanpackages binary /dev/null | gzip -9c > binary/Packages.gz )
# rsync to s3 bucket
s3sync $dst s3://http://debian.us-east.neo4j.org.s3-website-us-east-1.amazonaws.com/
