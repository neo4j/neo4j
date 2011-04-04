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
          alpha : 0.6
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
          alpha : 0.2
        labelStyle : 
          color : "white"
          font  : "12px Helvetica"

      constructor : () ->
        @labelProperties = []
        @itemUrlUtils = new ItemUrlResolver()

      getStyleFor : (visualNode) ->
        switch visualNode.data.type
          when "explored-node"
            node = visualNode.data.neoNode
            
            for prop in @labelProperties
              if node.hasProperty(prop)
                label = node.getProperty(prop)
                break
                      
            label ?= @itemUrlUtils.extractNodeId(node.getSelf())
            return { nodeStyle:@defaultExploredStyle.nodeStyle, labelStyle:@defaultExploredStyle.labelStyle, labelText:label}

          when "unexplored-node"
            label = @itemUrlUtils.extractNodeId(visualNode.data.neoUrl)
            return { nodeStyle:@defaultUnexploredStyle.nodeStyle, labelStyle:@defaultUnexploredStyle.labelStyle, labelText:label }

          else
            return { nodeStyle:@defaultGroupStyle.nodeStyle, labelStyle:@defaultGroupStyle.labelStyle, labelText:"Group" }

      setLabelProperties : (labelProperties) ->
        @labelProperties = labelProperties

)
