#!/bin/bash -e
# depends libxml-simple-perl

tcrepo=http://builder.neo4j.org/guestAuth/repository/download/bt18/lastSuccessful
# mvnrepo=https://repo.neo4j.org/content/repositories/snapshots
mvnrepo=http://build-repo.neo4j.org/repository/snapshots/

function work {
    deploy_defaults neo4j-graph-algo neo4j-jmx neo4j-kernel neo4j-lucene-index neo4j-shell server-api neo4j-graph-matching neo4j-graphviz

    deploy neo4j-udc docs javadoc sources test-sources tests neo4j

    deploy neo4j docs javadoc sources test-sources
    deploy neo4j-community docs javadoc sources test-sources
    
    deploy neo4j-cypher sources test-sources tests docs
    
    deploy neo4j-server static-web docs javadoc sources test-sources tests

    deploy neo4j-examples docs sources test-sources tests

    deploy neo4j-server-examples docs sources test-sources tests
}

function deploy_defaults {
    for artifact in $@  ; do
        deploy $artifact docs javadoc sources test-sources tests
    done
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

function deploy_maven_jar {
    artifact=$1
    version=$2
    filename=$artifact-$version.jar
    fetch_artifact $artifact $filename
    deploycommand="mvn deploy:deploy-file -Durl=$mvnrepo -DrepositoryId=snapshots -DuniqueVersion=false -Dfile=$filename -Dpackaging=jar -DpomFile=pom.xml"
    repeat_command "$deploycommand"
}

function deploy_maven_type_classifier {
    artifact=$1
    version=$2
    type=$3
    classifier=$4
    filename=$artifact-$version-$classifier.$type
    fetch_artifact $artifact $filename
    deploycommand="mvn deploy:deploy-file -Durl=$mvnrepo -DrepositoryId=snapshots -DuniqueVersion=false -Dfile=$filename -Dpackaging=$type -Dclassifier=$classifier -DpomFile=pom.xml"
    repeat_command "$deploycommand"
}


# uses the global $artifact as input
function get_version {
    fetch_artifact $artifact pom.xml
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
