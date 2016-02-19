fake_install() {
  path="neo4j-home"

  mkdir -p "${path}/bin"
  cp ../../../main/distribution/shell-scripts/bin/* "${path}/bin" 2>/dev/null
  chmod +x "${path}/bin/neo4j"
  mkdir -p "${path}/conf"
  mkdir -p "${path}/data/databases"
  mkdir -p "${path}/lib"
}

clear_config() {
  rm -f neo4j-home/conf/*
}

set_config() {
  name=$1
  value=$2
  file=$3
  echo "${name}=${value}" >>"neo4j_home/conf/${file}"
}

set_main_class() {
  class=$1
  sed -i.bak -e "s/#{neo4j\.mainClass}/${class}/" neo4j-home/bin/neo4j
}

neo4j_home() {
  echo "$(pwd)/neo4j-home"
}

export JAVA_CMD="$(pwd)/sharness.d/fake-java"
