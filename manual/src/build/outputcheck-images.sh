#!/bin/bash

errors=""
while IFS= read -r line
do
	echo "${line}"
	if [[ "$line" == "a2x: WARNING: missing resource:"* ]]
	then
		errors="${errors}${line}"'\n'
	fi
done

if [ -n "$errors" ]
then
	echo "================================="
	echo "There are missing resource files:"
	echo "================================="
	echo -e $errors
	exit 1
else
	echo "Outputcheck: There are no missing resource files."
fi

