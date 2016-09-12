test_expect_java_arg() {
  arg="$1"
  end="$((SECONDS+5))"
  java_args="${SHARNESS_TRASH_DIRECTORY}/java-args"
  while true; do
    if grep --fixed-strings --regexp "${arg}" "${java_args}" >/dev/null ; then
      break
    fi

    if [[ "${SECONDS}" -ge "${end}" ]]; then
      echo >&2 "test_expect_java_arg: expected argument '$arg' but got '$(cat "${java_args}")'"
      return 1
    fi

    echo >&2 "waiting for java args"
    sleep 1
  done
}

test_expect_stdout_matching() {
  expected_pattern=$1
  shift

  stdout="$("$@")"
  err="$?"
  [[ "${err}" -ne 0 ]] && return "${err}"

  echo "${stdout}" | grep "${expected_pattern}" >/dev/null
  exit_code="$?"
  if [[ "${exit_code}" -eq 0 ]]; then
    return 0
  fi

  echo >&2 "test_expect_stdout_matching: expected '${expected_pattern}' but got '${stdout}'"
  return 1
}

test_expect_stderr_matching() {
  expected_pattern=$1
  shift

  stdout="$("$@" 2>&1 1>/dev/null)"
  err="$?"
  [[ "${err}" -ne 0 ]] && return "${err}"

  echo "${stdout}" | grep "${expected_pattern}" >/dev/null
  exit_code="$?"
  if [[ "${exit_code}" -eq 0 ]]; then
    return 0
  fi

  echo >&2 "test_expect_stdout_matching: expected '${expected_pattern}' but got '${stdout}'"
  return 1
}

test_expect_file_matching() {
  expected_pattern=$1
  content="$(cat "${2}")"
  echo "${content}" | grep "${expected_pattern}" >/dev/null
  exit_code="$?"
  if [[ "${exit_code}" -eq 0 ]]; then
    return 0
  fi

  echo >&2 "test_expect_stdout_matching: expected '${expected_pattern}' but got '${content}'"
  return 1
}

assert_equals() {
  actual="$1"
  expected="$2"
  if [[ ! "${actual}" = "${expected}" ]]; then
    echo >&2 "Expected '${expected}' but got '${actual}'"
    return 1
  fi
}

assert_failure_with_stderr() {
  expected_pattern="$1"
  shift
  test_expect_code 1 "$@" 2>neo4j.stderr
  err="$?"
  [[ "${err}" -ne 0 ]] && return "${err}"
  test_expect_file_matching "${expected_pattern}" neo4j.stderr
}
