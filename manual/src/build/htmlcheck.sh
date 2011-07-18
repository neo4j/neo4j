#!/bin/bash

lscommand="ls $1/_*"

if $lscommand > /dev/null 2>&1
then
	echo "There are missing identifiers in the following chapters:"
	$lscommand | xargs -n1 basename
	exit 1
else
	echo "There are no missing identifiers."
fi

