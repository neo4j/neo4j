fake_install() {
  path="${1:-./neo4j-home}"
  version="${2:-3.0}"

  case "${version}" in
    3.0)
      mkdir -p "${path}/bin"
      cp ../../../main/distribution/shell-scripts/bin/* "${path}/bin" 2>/dev/null
      chmod +x "${path}/bin/neo4j"
      mkdir -p "${path}/conf"
      mkdir -p "${path}/data/databases"
      mkdir -p "${path}/lib"
      ;;
    *)
      mkdir -p "${path}/bin"
      mkdir -p "${path}/conf"
      mkdir -p "${path}/data"
      mkdir -p "${path}/lib"
      ;;
  esac
}

clear_config() {
  rm -f neo4j-home/conf/*
}

set_config() {
  name=$1
  value=$2
  file=$3
  neo4j_home="${4:-./neo4j-home}"
  echo "${name}=${value}" >>"${neo4j_home}/conf/${file}"
}

set_main_class() {
  class=$1
  sed -i.bak -e "s/#{neo4j\.mainClass}/${class}/" neo4j-home/bin/neo4j
}

neo4j_home() {
  echo "$(pwd)/neo4j-home"
}

export JAVA_CMD="$(pwd)/sharness.d/fake-java"
