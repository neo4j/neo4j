export NEO4J_HOME=target/neo4j_home
export NEO4J_PRODUCT=community
export DOWNLOAD_PATH=http://builder.neo4j.org/guestAuth/repository/download/bt85/.lastSuccessful
export NEO4J_VERSION=`curl ${DOWNLOAD_PATH}/reports/buildNumber.properties | cut -f2 -d=`
export DOWNLOAD_LOCATION=${DOWNLOAD_PATH}/packages/neo4j-community-${NEO4J_VERSION}-unix.tar.gz
rake