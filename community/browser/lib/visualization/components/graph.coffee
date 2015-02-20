###!
Copyright (c) 2002-2015 "Neo Technology,"
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

'use strict'

class neo.models.Graph
  constructor: () ->
    @nodeMap = {}
    @_nodes = []
    @relationshipMap = {}
    @_relationships = []

  nodes: ->
    @_nodes

  relationships: ->
    @_relationships

  groupedRelationships: ->
    class NodePair
      constructor: (node1, node2) ->
        @relationships = []
        if node1.id < node2.id
          @nodeA = node1
          @nodeB = node2
        else
          @nodeA = node2
          @nodeB = node1

      isLoop: ->
        @nodeA is @nodeB

      toString: ->
        "#{@nodeA.id}:#{@nodeB.id}"
    groups = {}
    for relationship in @_relationships
      nodePair = new NodePair(relationship.source, relationship.target)
      nodePair = groups[nodePair] ? nodePair
      nodePair.relationships.push relationship
      groups[nodePair] = nodePair
    (pair for ignored, pair of groups)

  addNodes: (nodes) =>
    for node in nodes
      if !@findNode(node.id)?
        @nodeMap[node.id] = node
        @_nodes.push(node)
    @

  addRelationships: (relationships) =>
    for relationship in relationships
      existingRelationship = @findRelationship(relationship.id)
      if existingRelationship?
        existingRelationship.internal = false
      else
        relationship.internal = false
        @relationshipMap[relationship.id] = relationship
        @_relationships.push(relationship)
    @

  addInternalRelationships: (relationships) =>
    for relationship in relationships
      relationship.internal = true
      if not @findRelationship(relationship.id)?
        @relationshipMap[relationship.id] = relationship
        @_relationships.push(relationship)
    @

  pruneInternalRelationships: =>
    relationships = @_relationships.filter((relationship) -> not relationship.internal)
    @relationshipMap = {}
    @_relationships = []
    @addRelationships(relationships)

  findNode: (id) => @nodeMap[id]

  findRelationship: (id) => @relationshipMap[id]
