test_expect_java_arg() {
  arg="$1"
  if grep --fixed-strings --regexp "${arg}" java-args >/dev/null ; then
    return 0
  fi

	echo >&2 "test_expect_java_arg: expected argument '$arg' but got '$(cat java-args)'"
	return 1
}
