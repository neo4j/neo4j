#!/bin/bash

nodefontsize=10
edgefontsize=$nodefontsize

if [[ "$2" == "meta" ]]
then
  nodefillcolor=slategray1
  nodehighlight=darkseagreen1
  nodehighlight2=aquamarine
  edgehighlight=mediumblue
  boxcolor=black
  nodeshape=box
elif [[ "$2" == "neoviz" ]]
then
  nodefillcolor=ivory1
  nodehighlight=khaki1
  nodehighlight2=lemonchiffon1
  edgehighlight=mediumblue
  boxcolor=black
  nodeshape=Mrecord
  nodefontsize=8
  edgefontsize=$nodefontsize
else
  nodefillcolor=ivory1
  nodehighlight=khaki1
  nodehighlight2=lemonchiffon1
  edgehighlight=mediumblue
  boxcolor=black
  nodeshape=box
fi

maxsize="graph [size=\"7.0,9.0\"]"

nodestyle=filled,rounded
#nodeheight=0.37
nodesep=0.4
textnode=shape=plaintext,style=diagonals,height=0.2,margin=0.0,0.0

arrowhead=vee
arrowsize=0.75

graphfont=Sans
nodefont=$graphfont
edgefont=$graphfont


indata=$(cat);
indata=${indata//NODEHIGHLIGHT/$nodehighlight}
indata=${indata//NODE2HIGHLIGHT/$nodehighlight2}
indata=${indata//EDGEHIGHLIGHT/$edgehighlight}
indata=${indata//BOXCOLOR/$boxcolor}
indata=${indata//TEXTNODE/$textnode}

svgfile=$1
pngfile="${svgfile}.png"

prepend="digraph g{ $maxsize\
  node [shape=\"$nodeshape\" fillcolor=\"$nodefillcolor\" style=\"$nodestyle\" \
    fontsize=$nodefontsize fontname=\"$nodefont\"]
  edge [arrowhead=\"$arrowhead\" arrowtail=\"$arrowhead\" arrowsize=$arrowsize fontsize=$edgefontsize fontname=\"$edgefont\"] \
  nodesep=$nodesep \
  fontname=\"$graphfont\" "

echo "${prepend} ${indata} }" | dot -Tpng -o"$pngfile"
echo "${prepend} ${indata} }" | dot -Tsvg -o"$svgfile"

echo ""

