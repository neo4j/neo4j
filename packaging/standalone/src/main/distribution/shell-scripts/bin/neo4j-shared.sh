# Copyright (c) 2002-2016 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
#
# This file is part of Neo4j.
#
# Neo4j is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

declare -r PROGRAM="$(basename "$0")"

# Sets up the standard environment for running Neo4j shell scripts.
#
# Provides these environment variables:
#   NEO4J_HOME
#   NEO4J_CONF
#   NEO4J_DATA
#   NEO4J_LIB
#   NEO4J_LOGS
#   NEO4J_PIDFILE
#   NEO4J_PLUGINS
#   one per config setting, with dots converted to underscores
#
setup_environment() {
  _setup_calculated_paths
  _read_config
  _setup_configurable_paths
}

build_classpath() {
  CLASSPATH="${NEO4J_PLUGINS}:${NEO4J_CONF}:${NEO4J_LIB}/*:${NEO4J_PLUGINS}/*"
}

detect_os() {
  if uname -s | grep -q Darwin; then
    DIST_OS="macosx"
  elif [[ -e /etc/gentoo-release ]]; then
    DIST_OS="gentoo"
  else
    DIST_OS="other"
  fi
}

check_java() {
  _find_java_cmd

  version_command=("${JAVA_CMD}" "-version")
  [[ -n "${JAVA_MEMORY_OPTS:-}" ]] && version_command+=("${JAVA_MEMORY_OPTS[@]}")

  JAVA_VERSION=$("${version_command[@]}" 2>&1 | awk -F '"' '/version/ {print $2}')
  if [[ "${JAVA_VERSION}" < "1.8" ]]; then
    echo "ERROR! Neo4j cannot be started using java version ${JAVA_VERSION}. "
    _show_java_help
    exit 1
  fi

  if ! ("${version_command[@]}" 2>&1 | egrep -q "(Java HotSpot\\(TM\\)|OpenJDK|IBM) (64-Bit Server|Server|Client|J9) VM"); then
    echo "WARNING! You are using an unsupported Java runtime. "
    _show_java_help
  fi
}

# Resolve a path relative to $NEO4J_HOME.  Don't resolve if
# the path is absolute.
resolve_path() {
    orig_filename=$1
    if [[ ${orig_filename} == /* ]]; then
        filename="${orig_filename}"
    else
        filename="${NEO4J_HOME}/${orig_filename}"
    fi
    echo "${filename}"
}

_find_java_cmd() {
  [[ "${JAVA_CMD:-}" ]] && return
  detect_os
  _find_java_home

  if [[ "${JAVA_HOME:-}" ]] ; then
    JAVA_CMD="${JAVA_HOME}/bin/java"
    if [[ ! -f "${JAVA_CMD}" ]]; then
      echo "ERROR: JAVA_HOME is incorrectly defined as ${JAVA_HOME} (the executable ${JAVA_CMD} does not exist)"
      exit 1
    fi
  else
    if [ "${DIST_OS}" != "macosx" ] ; then
      # Don't use default java on Darwin because it displays a misleading dialog box
      JAVA_CMD="$(which java || true)"
    fi
  fi

  if [[ ! "${JAVA_CMD:-}" ]]; then
    echo "ERROR: Unable to find Java executable."
    _show_java_help
    exit 1
  fi
}

_find_java_home() {
  [[ "${JAVA_HOME:-}" ]] && return

  case "${DIST_OS}" in
    "macosx")
      JAVA_HOME="$(/usr/libexec/java_home -v 1.8)"
      ;;
    "gentoo")
      JAVA_HOME="$(java-config --jre-home)"
      ;;
  esac
}

_show_java_help() {
  echo "* Please use Oracle(R) Java(TM) 8, OpenJDK(TM) or IBM J9 to run Neo4j Server."
  echo "* Please see http://docs.neo4j.org/ for Neo4j Server installation instructions."
}

_setup_calculated_paths() {
  if [[ -z "${NEO4J_HOME:-}" ]]; then
    NEO4J_HOME="$(cd "$(dirname "$0")"/.. && pwd)"
  fi
  : "${NEO4J_CONF:="${NEO4J_HOME}/conf"}"
  readonly NEO4J_HOME NEO4J_CONF
}

_read_config() {
  # - plain key-value pairs become environment variables
  # - keys have '.' chars changed to '_'
  # - keys of the form KEY.# (where # is a number) are concatenated into a single environment variable named KEY
  parse_line() {
    line="$1"
    if [[ "${line}" =~ ^([^#\s][^=]+)=(.+)$ ]]; then
      key="${BASH_REMATCH[1]//./_}"
      value="${BASH_REMATCH[2]}"
      if [[ "${key}" =~ ^(.*)_([0-9]+)$ ]]; then
        key="${BASH_REMATCH[1]}"
      fi
      if [[ "${!key:-}" ]]; then
        export ${key}="${!key} ${value}"
      else
        export ${key}="${value}"
      fi
    fi
  }

  if [[ -f "${NEO4J_CONF}/neo4j-wrapper.conf" ]]; then
    cat >&2 <<EOF
WARNING: neo4j-wrapper.conf is deprecated and support for it will be removed in a future
         version of Neo4j; please move all your settings to neo4j.conf
EOF
  fi

  for file in "neo4j-wrapper.conf" "neo4j.conf"; do
    path="${NEO4J_CONF}/${file}"
    if [ -e "${path}" ]; then
      while read line; do
        parse_line "${line}"
      done <"${path}"
    fi
  done
}

_setup_configurable_paths() {
  NEO4J_DATA=$(resolve_path "${dbms_directories_data:-data}")
  NEO4J_LIB=$(resolve_path "${dbms_directories_lib:-lib}")
  NEO4J_LOGS=$(resolve_path "${dbms_directories_logs:-logs}")
  NEO4J_PLUGINS=$(resolve_path "${dbms_directories_plugins:-plugins}")
  NEO4J_RUN=$(resolve_path "${dbms_directories_run:-run}")
  readonly NEO4J_DATA NEO4J_LIB NEO4J_LOGS NEO4J_PLUGINS NEO4J_RUN
}
