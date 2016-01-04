run_console() {
  neo4j-home/bin/neo4j console
}

run_daemon() {
  FAKE_JAVA_SLEEP=1 neo4j-home/bin/neo4j start &&
  neo4j-home/bin/neo4j stop
}
