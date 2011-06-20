#!/bin/bash

indata=$(cat);

svgfile=$1
pngfile="${svgfile%.svg}.png"

prepend="digraph g{ \
  node [shape=box fillcolor=\"palegreen\" style=\"filled,rounded\" fontsize=11 fontname=\"Sans\"] \
  edge [arrowhead=\"vee\" arrowsize=0.8 fontsize=10   fontname=\"Sans\"] \
  nodesep=0.4 \
  fontname=\"Sans\" "

echo "${prepend} ${indata} }" | dot -Tpng -o"$pngfile"
echo "${prepend} ${indata} }" | dot -Tsvg -o"$svgfile"

echo ""

