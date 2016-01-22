fake_install() {
  mkdir -p neo4j-home/bin
  mkdir -p neo4j-home/conf
  cp ../../../main/distribution/shell-scripts/bin/* neo4j-home/bin 2>/dev/null
  chmod +x neo4j-home/bin/neo4j
  chmod +x neo4j-home/bin/neo4j-arbiter
}

clear_config() {
  rm -f neo4j-home/conf/*
}

set_config() {
  name=$1
  value=$2
  file=$3
  echo "${name}=${value}" >>"neo4j-home/conf/${file}"
}

clear_config() {
  file=$1
  rm "neo4j-home/conf/${file}"
}

set_main_class() {
  class=$1
  sed -i.bak -e "s/#{neo4j\.mainClass}/${class}/" neo4j-home/bin/neo4j-common.sh
}

neo4j_home() {
  echo "$(pwd)/neo4j-home"
}

export JAVA_CMD="$(pwd)/sharness.d/fake-java"
export NEO4J_START_WAIT=1
