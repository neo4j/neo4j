#!/bin/bash

verbose=$1
shift
mandir=$1
shift
importdir=$1
shift
a2x=$1
shift
scriptdir=$1
shift

for pagedef in "$@"
do
  set -- $pagedef
  page=$1
  component=$2
  echo "-----------------------------------------------------------------------------"
  echo "Creating manpage '$page' from component '$component'."

  "$a2x" -k $verbose -f manpage -d  manpage -D "$mandir" "$importdir/neo4j-${component}-docs-jar/man/${page}.1.asciidoc"
  gzip -q "$mandir/${page}.1"
  "$a2x" -k $verbose -f text -d  manpage -D "$mandir" "$mandir/${page}.1.xml"

  cp -f "$scriptdir/bom" "$mandir/${page}.txt"
  cat "$mandir/${page}.1.text" >> "$mandir/${page}.txt"
  rm "$mandir/${page}.1.text"
done

echo "-----------------------------------------------------------------------------"

