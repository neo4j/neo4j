#!/bin/bash

projectdir=$1

currentyear=$(date +%Y)
xmlyear="<year>$currentyear</year>"
docinfofile="$projectdir/docinfo.xml"

if grep -Fq "$xmlyear" $docinfofile
then
	echo "Correct year confirmed in docinfo.xml."
else
	echo "The year is not correct in docinfo.xml."
	exit 1
fi

