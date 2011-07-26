#!/bin/bash

errors=""
while IFS= read -r line
do
	echo "${line}"
	if [[ "$line" =~ "include file not found" ]]
	then
		errors="${errors}${line}"'\n'
	fi
done

if [ -n "$errors" ]
then
	echo "================================"
	echo "There are missing include files:"
	echo "================================"
	echo -e $errors
	exit 1
else
	echo "Outputcheck: There are no missing include files."
fi

