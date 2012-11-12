#!/bin/sh
REPO='unstable'
script_dir=$(dirname $0)
dest="${script_dir}/target/repo"
mkdir -p $dest/${REPO}
cp ${script_dir}/target/classes/*.deb $dest/${REPO}
( cd $dest && dpkg-scanpackages ${REPO} /dev/null | gzip -9c > ${REPO}/Packages.gz )
s3cmd put --acl-public --recursive $dest s3://debian.neo4j.org
