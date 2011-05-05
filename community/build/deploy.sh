#!/bin/bash -e
# depends libxml-simple-perl

tcrepo=http://builder.neo4j.org/guestAuth/repository/download/bt18/lastSuccessful
mvnrepo=http://repo.neo4j.org/content/repositories/snapshots

function work {
    deploy_defaults neo4j-graph-algo jmx neo4j-kernel neo4j-lucene-index neo4j-shell neo4j-udc server-api

    deploy neo4j docs javadoc sources test-sources
    deploy neo4j-community docs javadoc sources test-sources
    
    deploy neo4j-server site static-web docs javadoc sources test-sources tests

    deploy_tarball neo4j-examples download
    deploy neo4j-examples docs sources test-sources tests site

    deploy_tarball neo4j-server-examples download
    deploy neo4j-server-examples docs site sources test-sources tests
}

function deploy_defaults {
    for artifact in $@  ; do
        deploy $artifact docs javadoc sources test-sources tests
    done
}

function repeat_command {
    thecommand=$1
    for counter in 1 2 3
    do
        if $thecommand
        then
            break
        fi
        echo "Command failed ($counter): $thecommand"
    done
}

function deploy_maven_jar {
    artifact=$1
    version=$2
    filename=$artifact-$version.jar
    curlcommand="curl -f -O $tcrepo/$artifact/$filename"
    echo $curlcommand
    $curlcommand
    deploycommand="mvn deploy:deploy-file -Durl=$mvnrepo -DrepositoryId=snapshots -DuniqueVersion=false -Dfile=$filename -Dpackaging=jar -DpomFile=pom.xml"
    echo $deploycommand
    repeat_command "$deploycommand"
}

function deploy_maven_type_classifier {
    artifact=$1
    version=$2
    type=$3
    classifier=$4
    filename=$artifact-$version-$classifier.$type
    curlcommand="curl -f -O $tcrepo/$artifact/$filename"
    echo $curlcommand
    $curlcommand
    deploycommand="mvn deploy:deploy-file -Durl=$mvnrepo -DrepositoryId=snapshots -DuniqueVersion=false -Dfile=$filename -Dpackaging=$type -Dclassifier=$classifier -DpomFile=pom.xml"
    echo $deploycommand
    repeat_command "$deploycommand"
}

# uses the global $artifact as input
function get_version {
    curlcommand="curl -f -s -O $tcrepo/$artifact/pom.xml"
    echo $curlcommand
    $curlcommand
    version=$( ./xmlgrep pom.xml )
    echo "**************************************************************"
    echo "artifact:$artifact  version:$version"
}

function deploy_tarball {
    artifact=$1
    classifier=$2
    type=tar.gz
    get_version
    deploy_maven_type_classifier $artifact $version $type $classifier
}

function deploy {
    artifact=$1
    get_version
    deploy_maven_jar $artifact $version
    first=1
    for classifier in $@  ; do
        if [ -z $first ] ; then
            deploy_maven_type_classifier $artifact $version jar $classifier
        fi
        unset first
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
