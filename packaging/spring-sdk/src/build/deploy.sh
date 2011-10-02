#!/bin/bash -e
# depends libxml-simple-perl

tcrepo=http://builder.neo4j.org/guestAuth/repository/download/bt65/lastSuccessful
rootpathdist=public_html/dist

function work {
    upload_packages neo4j-community neo4j-advanced neo4j-enterprise
}

function repeat_command {
    thecommand=$1
    echo $thecommand
    success=0
    for counter in 1 2 3
    do
        if $thecommand
        then
            success=1
            break            
        fi
        echo "Command failed ($counter): $thecommand"
    done
    if [ $success == 0 ]
    then
        echo "Fatal command failure: $thecommand"
        exit 1
    fi
}

function fetch_artifact {
    artifact=$1
    filename=$2
    curlcommand="curl -f -O $tcrepo/$artifact/$filename"
    repeat_command "$curlcommand"
}

function upload_package {
    artifact=$1
    version=$2
    filenameWindows=$artifact-$version-windows.zip
    filenameUnix=$artifact-$version-unix.tar.gz
    fetch_artifact standalone $filenameWindows
    fetch_artifact standalone $filenameUnix
    upload_file $filenameWindows
    upload_file $filenameUnix
}

function upload_file {
    filename=$1
    scpcommand="scp $filename dist-server:$rootpathdist/$filename"
    repeat_command "$scpcommand"
}

# uses the global $artifact as input
function get_version {
    fetch_artifact $artifact pom.xml
    version=$( ./xmlgrep pom.xml )
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
[ -e target/upload ] && rm -rf target/upload
mkdir -p target/upload
cd target/upload


## create xml-version helper
cat<<EOF >xmlgrep
#!/usr/bin/perl -w
use XML::Simple;
print XML::Simple->new()->XMLin(shift)->{'version'};
EOF
chmod +x xmlgrep

###############
work
