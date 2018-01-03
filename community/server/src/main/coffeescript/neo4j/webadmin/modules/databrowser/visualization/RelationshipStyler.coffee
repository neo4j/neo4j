###
Copyright (c) 2002-2018 "Neo Technology,"
Network Engine for Objects in Lund AB [http://neotechnology.com]

This file is part of Neo4j.

Neo4j is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
###

define(
  [], 
  () ->
  
    class RelationshipStyler

      defaultBetweenExploredStyle :
        edgeStyle :
          color : "rgba(0, 0, 0, 1)"
          width : 1
        labelStyle : 
          color : "rgba(0, 0, 0, 1)"
          font  : "10px Helvetica"

      defaultToUnknownStyle : 
        edgeStyle :
          color : "rgba(0, 0, 0, 0.2)"
          width : 1
        labelStyle : 
          color : "rgba(0, 0, 0, 0.4)"
          font  : "10px Helvetica"

      defaultToGroupStyle : 
        edgeStyle : 
          color : "rgba(0, 0, 0, 0.8)"
          width : 1
        labelStyle : 
          color : "rgba(0, 0, 0, 0.8)"
          font  : "10px Helvetica"

      getStyleFor : (visualRelationship) ->
        srcType = visualRelationship.source.data.type
        dstType = visualRelationship.target.data.type

        if visualRelationship.target.data.relType != null
          types = for url, rel of visualRelationship.data.relationships
            rel.getType()
          types = _.uniq(types)
          labelText = types.join(", ")

        if srcType is "explored" and dstType is "explored"
          return { edgeStyle : @defaultBetweenExploredStyle.edgeStyle, labelStyle : @defaultBetweenExploredStyle.labelStyle, labelText : labelText }

        if srcType is "unexplored" or dstType is "unexplored"
          return { edgeStyle : @defaultToUnknownStyle.edgeStyle, labelStyle : @defaultToUnknownStyle.labelStyle, labelText : labelText }
        
        return { edgeStyle : @defaultToGroupStyle.edgeStyle, labelStyle : @defaultToGroupStyle.labelStyle, labelText : visualRelationship.data.relType }

)
