#!/bin/bash -e
# depends libxml-simple-perl

tcrepo=http://builder.neo4j.org/guestAuth/repository/download/bt18/lastSuccessful
mvnrepo=http://repo.neo4j.org/content/repositories/snapshots

function work {
    deploy_tarball neo4j-server-examples
    deploy_tarball neo4j-examples
    deploy_jar neo4j docs javadoc sources test-sources
    deploy_jar neo4j-community docs javadoc sources test-sources
    
### todo http://builder.neo4j.org/repository/download/bt18/3307:id/neo4j-examples/neo4j-1.3-SNAPSHOT-site.jar
    deploy_jar neo4j-examples docs sources test-sources tests
    deploy_jar neo4j-graph-algo docs javadoc sources test-sources tests
    deploy_jar neo4j-jmx docs javadoc sources test-sources tests
    deploy_jar neo4j-kernel docs javadoc sources test-sources tests
    deploy_jar neo4j-lucene-index docs javadoc sources test-sources tests
    deploy_jar neo4j-server docs javadoc site sources static-web test-sources tests
    deploy_jar neo4j-server-examples docs site sources test-sources tests
    deploy_jar neo4j-shell docs javadoc sources test-sources tests
    deploy_jar neo4j-udc docs javadoc neo4j sources test-sources tests
    deploy_jar server-api docs javadoc sources test-sources tests
}

function repeat_command {
    thecommand=$1
    for counter in 1 2 3
    do
        if ( $thecommand )
        then
            break
        fi
        echo "Command failed ($counter): $thecommand"
    done
}

function deploy_maven {
    artifactandversion=$1
    curlcommand="curl -f -s -O $tcrepo/$artifact/$artifactandversion$filesuffix.$extension"
    echo $curlcommand
    $curlcommand
    deploycommand="mvn deploy:deploy-file -Durl=$mvnrepo -DrepositoryId=snapshots -DuniqueVersion=false -Dfile=$artifactandversion$filesuffix.$extension -Dpackaging=$extension -DpomFile=pom.xml"
    echo $deploycommand
    repeat_command $deploycommand
}

function deploy_maven_classifier {
    artifactandversion=$1
    classifier=$2
    curlcommand="curl -f -s -O $tcrepo/$artifact/$artifactandversion-$classifier$filesuffix.$extension"
    echo $curlcommand
    $curlcommand
    deploycommand="mvn deploy:deploy-file -Durl=$mvnrepo -DrepositoryId=snapshots -DuniqueVersion=false -Dfile=$artifactandversion-$classifier$filesuffix.$extension -Dpackaging=$extension -DpomFile=pom.xml -Dclassifier=$classifier"
    echo $deploycommand
    repeat_command $deploycommand
}

function deploy_tarball {
    artifact=$1
    extension=tar.gz
    filesuffix=
    c=download
    curl -f -s -O $tcrepo/$artifact/pom.xml
    echo curl -f -s -O $tcrepo/$artifact/pom.xml
    version=$( ./xmlgrep pom.xml )
    echo "**************************************************************"
    echo "artifact:$artifact  version:$version"
    deploy_maven_classifier $artifact-$version $c
}

function deploy_jar {
	filesuffix=
	extension=jar
	deploy $*
}

function deploy {
    artifact=$1
    curl -f -s -O $tcrepo/$artifact/pom.xml
    echo curl -f -s -O $tcrepo/$artifact/pom.xml
    version=$( ./xmlgrep pom.xml )
    echo "**************************************************************"
    echo "artifact:$artifact  version:$version"

    deploy_maven $artifact-$version
    first=1
    for c in $@  ; do
        if [ -z $first ] ; then
            deploy_maven_classifier $artifact-$version $c
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
