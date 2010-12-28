SCRIPTDIR=$(cd $(dirname $0); pwd)
HADIR=$(dirname $(dirname $(dirname $SCRIPTDIR)))
HAZIP="$HADIR/target/neo4j-ha-0.5-SNAPSHOT-dev.zip"
LIBDIR="$HADIR/target/dependency"
function rebuild () {
    return 0
}
