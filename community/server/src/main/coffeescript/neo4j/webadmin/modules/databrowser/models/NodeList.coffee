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
  ['neo4j/webadmin/utils/ItemUrlResolver'
   './NodeProxy'
   'ribcage/Model'], 
  (ItemUrlResolver, NodeProxy, Model) ->
  
    class NodeList extends Model
      
      initialize : (nodes) =>
        @setRawNodes(nodes || [])

      setRawNodes : (nodes) =>
        @set "rawNodes" : nodes
        proxiedNodes = []
        propertyKeyMap = {}
        for node in nodes
          for key, value of node.getProperties()
            propertyKeyMap[key] = true
          proxiedNodes.push( new NodeProxy(node) )

        propertyKeys = (key for key, value of propertyKeyMap)

        @set "propertyKeys" : propertyKeys
        @set "nodes" : proxiedNodes

      getPropertyKeys : () =>
        @get "propertyKeys"

      getNodes : () =>
        @get "nodes"

      getRawNodes : () =>
        @get "rawNodes"

)
