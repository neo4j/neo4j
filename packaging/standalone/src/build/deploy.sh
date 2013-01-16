#!/bin/bash -ex
# depends libxml-simple-perl

rootpathdist=dist

function work {
    upload_packages neo4j-community neo4j-advanced neo4j-enterprise
}

function run_command {
    thecommand=$1
    echo $thecommand
    $thecommand
    if [ $? -ne 0 ]
    then
        echo "Fatal command failure: $thecommand"
        exit 1
    fi
}

function fetch_artifact {
    filename=$1
    run_command "cp ${WORKSPACE}/packaging/standalone/target/$filename ."
}

function upload_file {
    filename=$1
    run_command "s3cmd put --acl-public --guess-mime-type $filename s3://dist.neo4j.org/$filename"
}

function upload_package {
    artifact=$1
    version=$2
    filenameWindows=$artifact-$version-windows.zip
    filenameUnix=$artifact-$version-unix.tar.gz
    fetch_artifact $filenameWindows
    fetch_artifact $filenameUnix
    upload_file $filenameWindows
    upload_file $filenameUnix
}


# uses the global $artifact as input
function get_version {
    version=$( ./xmlgrep ${WORKSPACE}/pom.xml )
    echo "**************************************************************"
    echo "artifact:$artifact  version:$version"
}

function upload_packages {
    artifact="standalone"
    get_version
    for artifact in $@  ; do
        upload_package $artifact $version
    done
}

## prepare working dir
rm -rf target/upload
mkdir -p target/upload
cd target/upload


## create xml-version helper
cat<<EOF >xmlgrep
#!/usr/bin/ruby
require 'rexml/document'
file = File.read(ARGV[0])
doc = REXML::Document.new(file)
puts doc.root.elements['version'].text
EOF
chmod +x xmlgrep

###############
work
