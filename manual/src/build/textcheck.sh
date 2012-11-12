#!/bin/bash

errors=""
while IFS= read -r line
do
	if [[ "$line" == "ValueError: Missing snippet for tag"* || "$line" == "IOError: [Errno 2] No such file or directory"* ]]
	then
		errors="${errors}${line}"'\n'
	fi
done <$1
if [ -n "$errors" ]
then
	echo "There are errors in the sources:"
	echo -e $errors
	exit 1
else
	echo "There are no missing snippet source files or tags."
fi

