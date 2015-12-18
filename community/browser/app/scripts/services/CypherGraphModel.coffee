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

'use strict';

angular.module('neo4jApp.services')
  .service 'CypherGraphModel', () ->

    malformed = ->
      new Error('Malformed graph: must add nodes before relationships that connect them')

    @convertNode = () ->
      (node) ->
        new neo.models.Node(node.id, node.labels, node.properties)

    @convertRelationship = (graph) ->
      (relationship) ->
        source = graph.findNode(relationship.startNode) or throw malformed()
        target = graph.findNode(relationship.endNode) or throw malformed()
        new neo.models.Relationship(relationship.id, source, target, relationship.type, relationship.properties)

    return @
