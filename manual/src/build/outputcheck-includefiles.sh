#!/bin/bash

errors=""
while IFS= read -r line
do
	echo "${line}"
	if [[ "$line" =~ "include file not found" ]]
	then
		errors="${errors}${line}"'\n'
	elif [[ "$line" == "Error:"* ]]
	then
		errors="${errors}${line}"'\n'
	elif [[ "$line" == "asciidoc: WARNING"* ]]
	then
		errors="${errors}${line}"'\n'
	fi
done

if [ -n "$errors" ]
then
	echo "================================"
	echo "There are errors/warnings:"
	echo "================================"
	echo -e $errors
	exit 1
else
	echo "Outputcheck: There are no missing include files."
fi

