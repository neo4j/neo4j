#!/bin/sh
script_dir=$(dirname $0)
dest="${script_dir}/target/repo"
mkdir -p $dest/binary
cp ${script_dir}/target/classes/*.deb $dest
( cd $dest && dpkg-scanpackages binary /dev/null | gzip -9c > binary/Packages.gz )
s3sync $dest s3://http://debian.us-east.neo4j.org.s3-website-us-east-1.amazonaws.com/
