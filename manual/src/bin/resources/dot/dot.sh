#!/bin/bash

nodefillcolor=palegreen
nodehighlight=lightgoldenrod1
edgehighlight=mediumblue

nodeshape=box
nodestyle=filled,rounded
nodeheight=0.37
nodesep=0.4

arrowhead=vee
arrowsize=0.75

graphfont=Sans
nodefont=$graphfont
edgefont=$graphfont

nodefontsize=10
edgefontsize=$nodefontsize

indata=$(cat);
indata=${indata//NODEHIGHLIGHT/$nodehighlight}
indata=${indata//EDGEHIGHLIGHT/$edgehighlight}

svgfile=$1
pngfile="${svgfile%.svg}.png"

prepend="digraph g{ \
  node [shape=\"$nodeshape\" fillcolor=\"$nodefillcolor\" style=\"$nodestyle\" \
    fontsize=$nodefontsize fontname=\"$nodefont\" height=$nodeheight]
  edge [arrowhead=\"$arrowhead\" arrowsize=$arrowsize fontsize=$edgefontsize fontname=\"$edgefont\"] \
  nodesep=$nodesep \
  fontname=\"$graphfont\" "

echo "${prepend} ${indata} }" | dot -Tpng -o"$pngfile"
echo "${prepend} ${indata} }" | dot -Tsvg -o"$svgfile"

echo ""

