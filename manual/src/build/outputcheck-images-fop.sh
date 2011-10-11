#!/bin/bash

errors=""
while IFS= read -r line
do
	echo "${line}"
	if [[ "$line" =~ "Image not available" ]]
	then
		errors="${errors}${line}"'\n'
	elif [[ "$line" == "[ERROR]"* ]]
	then
		errors="${errors}${line}"'\n'
	fi
done

if [ -n "$errors" ]
then
	echo "===================================="
	echo " There is a PDF build error"
	echo "===================================="
	echo -e $errors
	exit 1
else
	echo "Outputcheck-fop: No PDF build errors detected.."
fi

