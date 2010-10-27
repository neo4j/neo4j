#!/bin/sh
$(dirname $(cd $(dirname $0); pwd))/src/main/script/$(basename $0) "$@"
