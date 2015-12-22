fake_install() {
  mkdir -p neo4j-home/bin
  mkdir -p neo4j-home/conf
  mkdir -p neo4j-home/lib
  mkdir -p neo4j-home/system/lib
  cp ../../../main/distribution/shell-scripts/bin/neo4j neo4j-home/bin
  chmod +x neo4j-home/bin/neo4j
  cp ../../../main/distribution/shell-scripts/bin/utils neo4j-home/bin
}

set_config() {
  name=$1
  value=$2
  file=$3

  echo "${name}=${value}" >>"neo4j-home/conf/${file}"
}

set_main_class() {
  class=$1
  sed -i '' -e "s/#{neo4j\.mainClass}/${class}/" neo4j-home/bin/neo4j
}

neo4j_home() {
  echo "$(pwd)/neo4j-home"
}

export JAVACMD="$(pwd)/sharness.d/fake-java"
export NEO4J_START_WAIT=0
