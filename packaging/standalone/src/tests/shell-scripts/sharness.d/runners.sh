run_console() {
  neo4j-home/bin/neo4j console
}

start_daemon() {
  export JAVA_SENTINEL=$(mktemp /tmp/java-sentinel.XXXXX)
  trap "rm -rf ${JAVA_SENTINEL}" EXIT
  neo4j-home/bin/neo4j start
}

run_daemon() {
  start_daemon && \
    FAKE_JAVA_DISABLE_RECORD_ARGS="t" neo4j-home/bin/neo4j stop
}
