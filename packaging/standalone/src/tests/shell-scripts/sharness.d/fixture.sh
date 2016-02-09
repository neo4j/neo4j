fake_install() {
  mkdir -p neo4j-home/bin
  cp ../../../main/distribution/shell-scripts/bin/* neo4j-home/bin 2>/dev/null
  chmod +x neo4j-home/bin/neo4j
  mkdir -p neo4j-home/conf
  mkdir -p neo4j-home/lib
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

set_main_class() {
  class=$1
  sed -i.bak -e "s/#{neo4j\.mainClass}/${class}/" neo4j-home/bin/neo4j
}

neo4j_home() {
  echo "$(pwd)/neo4j-home"
}

export JAVA_CMD="$(pwd)/sharness.d/fake-java"
