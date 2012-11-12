#!/bin/sh
srcdir=$1
imgdir=$2

find "$srcdir" -path '*images' -type d -print | while read i; do 
  cp -fr "${i}"/* "$imgdir"
done

