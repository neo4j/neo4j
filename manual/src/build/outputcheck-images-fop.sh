#!/bin/bash

errors=""
while IFS= read -r line
do
	echo "${line}"
	if [[ "$line" =~ "Image not available" ]]
	then
		errors="${errors}${line}"'\n'
	fi
done

if [ -n "$errors" ]
then
	echo "===================================="
	echo "There are images that FOP can't load"
	echo "===================================="
	echo -e $errors
	exit 1
else
	echo "Outputcheck-fop: All images were successfully loaded."
fi

