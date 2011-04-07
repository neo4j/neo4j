###
Copyright (c) 2002-2011 "Neo Technology,"
Network Engine for Objects in Lund AB [http://neotechnology.com]

This file is part of Neo4j.

Neo4j is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
###

define(
  ['neo4j/webadmin/data/ItemUrlResolver'], 
  (ItemUrlResolver) ->
  
    class NodeStyler

      defaultExploredStyle : 
        nodeStyle  : 
          fill  : "#000000"
          alpha : 0.9
        labelStyle : 
          color : "white"
          font  : "12px Helvetica"

      defaultUnexploredStyle : 
        nodeStyle  : 
          fill  : "#000000"
          alpha : 0.2
        labelStyle : 
          color : "rgba(255, 255, 255, 0.4)"
          font  : "12px Helvetica"

      defaultGroupStyle : 
        nodeStyle  : 
          shape : "dot"
          fill  : "#000000"
          alpha : 0.8
        labelStyle : 
          color : "white"
          font  : "10px Helvetica"

      constructor : () ->
        @labelProperties = []
        @itemUrlUtils = new ItemUrlResolver()

      getStyleFor : (visualNode) ->
        type = visualNode.data.type
        if type is "group"
          nodeStyle = @defaultGroupStyle.nodeStyle
          labelStyle = @defaultGroupStyle.labelStyle
          labelText = visualNode.data.group.nodeCount
        else 

          if visualNode.data.neoNode?
            node = visualNode.data.neoNode
            id = @itemUrlUtils.extractNodeId(node.getSelf())
            for prop in @labelProperties
              if node.hasProperty(prop)
                labelText = id + ": " + node.getProperty(prop)
                break
                      
            labelText ?= id
          else 
            labelText = "?"
          
          if type is "explored"
            nodeStyle = @defaultExploredStyle.nodeStyle
            labelStyle = @defaultExploredStyle.labelStyle
          else
            nodeStyle = @defaultUnexploredStyle.nodeStyle
            labelStyle = @defaultUnexploredStyle.labelStyle

        return { nodeStyle:nodeStyle, labelStyle:labelStyle, labelText:labelText }

      setLabelProperties : (labelProperties) ->
        @labelProperties = labelProperties

)
