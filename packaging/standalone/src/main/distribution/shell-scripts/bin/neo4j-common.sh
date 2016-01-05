# Copyright (c) 2002-2015 "Neo Technology,"
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

# This Bash library file is used for running neo4j and neo4j-arbiter.
#
# Callers must do the following:
#  * set -e
#  * define these variables and functions:
#     - FRIENDLY_NAME
#     - SHUTDOWN_TIMEOUT
#     - CONFIG_FILES
#     - MAIN_CLASS
#     - MIN_ALLOWED_OPEN_FILES
#     - CONSOLE_LOG_FILE
#     - NEO4J_HOME
#     - SCRIPT
#     - printstartmessage()
#     - printextrainfo()
#  * source this file
#  * run `main "$@"`
#
# This script defines the following variables which callers may use in their functions:
#  * variables for all Neo4j configuration values
#  * NEO4J_PID

find_java_home() {
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

find_java_cmd() {
  [[ "${JAVACMD:-}" ]] && return
  detectos
  find_java_home

  if [[ "${JAVA_HOME:-}" ]] ; then
    JAVACMD="${JAVA_HOME}/bin/java"
  else
    if [ "${DIST_OS}" != "macosx" ] ; then
      # Don't use default java on Darwin because it displays a misleading dialog box
      JAVACMD="$(which java)"
    fi
  fi

  if [[ ! "${JAVACMD:-}" ]]; then
    echo "ERROR: Unable to find Java executable."
    show_java_help
    exit 1
  fi
}

check_java() {
  find_java_cmd

  JAVAVERSION=$("${JAVACMD}" -version 2>&1 | awk -F '"' '/version/ {print $2}')
  if [[ "${JAVAVERSION}" < "1.8" ]]; then
    echo "ERROR! Neo4j cannot be started using java version ${JAVAVERSION}. "
    show_java_help
    exit 1
  fi

  if ! ("${JAVACMD}" -version 2>&1 | egrep -q "(Java HotSpot\\(TM\\)|OpenJDK) (64-Bit Server|Server|Client) VM"); then
    echo "WARNING! You are using an unsupported Java runtime. "
    show_java_help
  fi
}

show_java_help() {
  echo "* Please use Oracle(R) Java(TM) 8 or OpenJDK(TM) to run Neo4j Server."
  echo "* Please see http://docs.neo4j.org/ for Neo4j Server installation instructions."
}

checkstatus() {
  if [ -e "${NEO4J_PIDFILE}" ] ; then
    NEO4J_PID=$( cat "${NEO4J_PIDFILE}" )
    kill -0 "${NEO4J_PID}" 2>/dev/null || NEO4J_PID=""
  fi
}

detectos() {
  if uname -s | grep -q Darwin; then
    DIST_OS="macosx"
  elif [[ -e /etc/gentoo-release ]]; then
    DIST_OS="gentoo"
  else
    DIST_OS="other"
  fi
}

# Runs before the server command, making sure that whatever should be in place is
# in place.
checkandrepairenv() {
    # Create log directory if missing, change owner if created.
    if [ ! -d "${NEO4J_LOG}" ]; then
      echo "${NEO4J_LOG} was missing, recreating..."
      mkdir -p "${NEO4J_LOG}"
    fi
}

# Checks system limits, warns if not proper
checklimits() {
  detectos
  if [ "${DIST_OS}" != "macosx" ] ; then
    ALLOWED_OPEN_FILES="$(ulimit -n)"

    if [ "${ALLOWED_OPEN_FILES}" -lt "${MIN_ALLOWED_OPEN_FILES}" ]; then
      echo "WARNING: Max ${ALLOWED_OPEN_FILES} open files allowed, minimum of ${MIN_ALLOWED_OPEN_FILES} recommended. See the Neo4j manual."
    fi
  fi
}

parseConfig() {
  # - plain key-value pairs become environment variables
  # - keys have '.' chars changed to '_'
  # - keys of the form KEY.# (where # is a number) are concatenated into a single environment variable named KEY
  parseline() {
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

  for file in "${CONFIG_FILES[@]}"; do
    path="${NEO4J_CONFIG}/${file}"
    if [ -e "${path}" ]; then
      while read line; do
        parseline "${line}"
      done <"${path}"
    fi
  done
}

setuppaths() {
  [[ "${NEO4J_CONFIG:-}" ]] || NEO4J_CONFIG="${NEO4J_HOME}/conf"
  [[ "${NEO4J_LOG:-}" ]] || NEO4J_LOG="${NEO4J_HOME}/data/log"
  [[ "${NEO4J_PIDFILE:-}" ]] || NEO4J_PIDFILE="${NEO4J_HOME}/data/neo4j-service.pid"
  CONSOLE_LOG="${NEO4J_LOG}/${CONSOLE_LOG_FILE}"
}

setupjavaopts() {
  JAVA_OPTS="-server"
  [[ "${wrapper_java_additional:-}" ]] && JAVA_OPTS="${JAVA_OPTS} ${wrapper_java_additional}"
  [[ "${wrapper_java_initmemory:-}" ]] && JAVA_OPTS="${JAVA_OPTS} -Xms${wrapper_java_initmemory}m"
  [[ "${wrapper_java_maxmemory:-}" ]] && JAVA_OPTS="${JAVA_OPTS} -Xmx${wrapper_java_maxmemory}m"
  return 0
}

buildclasspath() {
  CLASSPATH="${NEO4J_HOME}/lib/*:${NEO4J_HOME}/system/lib/*:${NEO4J_HOME}/plugins/*"
}

do_console() {
  checkstatus
  if [[ "${NEO4J_PID:-}" ]] ; then
    echo "${FRIENDLY_NAME} is already running (pid ${NEO4J_PID})."
    exit 1
  fi

  echo "Starting ${FRIENDLY_NAME}."

  checklimits
  buildclasspath
  checkandrepairenv

  exec "${JAVACMD}" -cp "${CLASSPATH}" ${JAVA_OPTS} -Dneo4j.home="${NEO4J_HOME}" -Dfile.encoding=UTF-8 "${MAIN_CLASS}"
}

do_start() {
  checkstatus
  if [[ "${NEO4J_PID:-}" ]] ; then
    echo "${FRIENDLY_NAME} is already running (pid ${NEO4J_PID})."
    exit 0
  fi

  echo "Starting ${FRIENDLY_NAME}."

  checklimits
  buildclasspath
  checkandrepairenv

  nohup "${JAVACMD}" -cp "${CLASSPATH}" ${JAVA_OPTS} -Dneo4j.home="${NEO4J_HOME}" -Dfile.encoding=UTF-8 "${MAIN_CLASS}" \
    >>"${CONSOLE_LOG}" 2>&1 &
  echo "$!" >"${NEO4J_PIDFILE}"

  sleep "${NEO4J_START_WAIT:-5}"
  checkstatus
  if [[ ! "${NEO4J_PID:-}" ]] ; then
    echo "Unable to start. See ${CONSOLE_LOG} for details."
    rm "${NEO4J_PIDFILE}"
    exit 1
  fi

  printstartmessage
  echo "See ${CONSOLE_LOG} for current status."
}

do_stop() {
  checkstatus

  if [[ ! "${NEO4J_PID:-}" ]] ; then
    echo "ERROR: ${FRIENDLY_NAME} not running"
    [ -e "${NEO4J_PIDFILE}" ] && rm "${NEO4J_PIDFILE}"
  else
    echo -n "Stopping ${FRIENDLY_NAME} [${NEO4J_PID}]..."
    elapsed=0
    while [ "${NEO4J_PID}" != "" ]  ; do
      kill ${NEO4J_PID} 2>/dev/null
      if [ "${elapsed}" -le "$SHUTDOWN_TIMEOUT" ]; then
        printf "."
      fi
      sleep 1
      checkstatus

      if [ "${elapsed}" -eq "$SHUTDOWN_TIMEOUT" ] ;then
	    echo ""
	    echo "${FRIENDLY_NAME} [${NEO4J_PID}] is taking more than ${SHUTDOWN_TIMEOUT}s to stop"
	    echo "There might be some troubles with the shutdown of ${FRIENDLY_NAME}, please read log files for details"
	    echo "This script will keep waiting for the server to shutdown, you could manually kill the process ${NEO4J_PID}"
	    echo ""
      fi

      elapsed="$((elapsed+1))"
    done
    echo " done"
    [ -e "${NEO4J_PIDFILE}" ] && rm  "${NEO4J_PIDFILE}"
  fi
}

do_status() {
  checkstatus
  if [[ ! "${NEO4J_PID:-}" ]] ; then
    echo "${FRIENDLY_NAME} is not running"
    exit 3
  else
    echo "${FRIENDLY_NAME} is running at pid ${NEO4J_PID}"
  fi
}

do_info() {
  do_status
  buildclasspath
  echo "NEO4J_HOME:        ${NEO4J_HOME}"
  echo "JAVA_HOME:         ${JAVA_HOME}"
  echo "JAVA_OPTS:         ${JAVA_OPTS}"
  echo "CLASSPATH:         ${CLASSPATH}"
  printextrainfo
}

main() {
  cd "${NEO4J_HOME}"
  setuppaths
  parseConfig
  setupjavaopts
  check_java

  case "$1" in
    console)
      do_console
      ;;

    start)
      do_start
      ;;

    stop)
      do_stop
      ;;

    restart)
      do_stop
      do_start
      ;;

    status)
      do_status
      ;;

    info)
      do_info
      ;;

    *)
      echo "Usage: ${SCRIPT} { console | start | stop | restart | status | info }"
      ;;
  esac
}
