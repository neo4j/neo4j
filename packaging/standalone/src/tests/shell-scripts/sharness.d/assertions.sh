test_expect_java_arg() {
  arg="$1"
  end="$((SECONDS+5))"
  while true; do
    if grep --fixed-strings --regexp "${arg}" java-args >&2 ; then
      break
    fi

    if [[ "${SECONDS}" -ge "${end}" ]]; then
      echo >&2 "test_expect_java_arg: expected argument '$arg' but got '$(cat java-args)'"
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
	echo "${stdout}" | grep "${expected_pattern}"
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
	stdout="$("$@" 2>&1)"
	echo "${stdout}" | grep "${expected_pattern}"
	exit_code="$?"
	if [[ "${exit_code}" -eq 0 ]]; then
		return 0
	fi

	echo >&2 "test_expect_stdout_matching: expected '${expected_pattern}' but got '${stdout}'"
	return 1
}
