fake_install() {
  mkdir -p neo4j-home/bin
  mkdir -p neo4j-home/conf
  mkdir -p neo4j-home/lib
  mkdir -p neo4j-home/system/lib
  cp ../../../main/distribution/shell-scripts/bin/neo4j neo4j-home/bin
  chmod +x neo4j-home/bin/neo4j
  cp ../../../main/distribution/shell-scripts/bin/utils neo4j-home/bin
}

export JAVACMD="$(pwd)/sharness.d/fake-java"
