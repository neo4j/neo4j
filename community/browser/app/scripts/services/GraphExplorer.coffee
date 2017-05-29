###!
Copyright (c) 2002-2017 "Neo Technology,"
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
  .factory 'GraphExplorer', [
    '$q'
    'Cypher'
    'CypherGraphModel'
    'Settings'
    ($q, Cypher, CypherGraphModel, Settings) ->
      return  {
        exploreNeighbours: (node, graph, withInternalRelationships) ->
          q = $q.defer()
          currentNeighbourIds = graph.findNodeNeighbourIds node.id
          returnObj =
            neighbourDisplayedSize: currentNeighbourIds.length
            neighbourSize: currentNeighbourIds.length

          if returnObj.neighbourDisplayedSize >= Settings.maxNeighbours
            @findNumberOfNeighbours(node).then (numberNeighboursResult) =>
              returnObj.neighbourSize = numberNeighboursResult._response.data[0]?.row[0]
              q.resolve(returnObj)
            return q.promise

          @findNeighbours(node, currentNeighbourIds).then (neighboursResult) =>
            graph.addNodes(neighboursResult.nodes.map(CypherGraphModel.convertNode()))
            graph.addRelationships(neighboursResult.relationships.map(CypherGraphModel.convertRelationship(graph)))

            currentNeighbourIds = graph.findNodeNeighbourIds node.id
            returnObj =
              neighbourDisplayedSize: currentNeighbourIds.length
              neighbourSize: neighboursResult._response.data[0]?.row[1]
            if withInternalRelationships
              @internalRelationships(graph, graph.nodes(), neighboursResult.nodes).then ->
                q.resolve(returnObj)
            else
              q.resolve(returnObj)
          q.promise

        findNeighbours: (node, currentNeighbourIds) ->
          q = $q.defer()
          Cypher.transaction()
          .commit("""
            MATCH path = (a)--(o)
            WHERE id(a)= #{node.id}
            AND NOT (id(o) IN[#{currentNeighbourIds.join(',')}])
            RETURN path, size((a)--()) as c
            ORDER BY id(o)
            LIMIT #{Settings.maxNeighbours-currentNeighbourIds.length}""")
          .then(q.resolve)
          q.promise

        findNumberOfNeighbours: (node) ->
          q = $q.defer()
          Cypher.transaction()
          .commit("""
              MATCH (a)
              WHERE id(a)= #{node.id}
              RETURN size((a)--()) as c""")
          .then(q.resolve)
          q.promise

        internalRelationships: (graph, existingNodes, newNodes) ->
          q = $q.defer()
          if newNodes.length is 0
            q.resolve()
            return q.promise
          newNodeIds = newNodes.map((node) -> node.id)
          existingNodeIds = existingNodes.map((node) -> node.id).concat(newNodeIds)
          Cypher.transaction()
          .commit("""
            MATCH a -[r]- b WHERE id(a) IN[#{existingNodeIds.join(',')}]
            AND id(b) IN[#{newNodeIds.join(',')}]
            RETURN r;"""
          )
          .then (result) =>
            graph.addInternalRelationships(result.relationships.map(CypherGraphModel.convertRelationship(graph)))
            q.resolve()
          q.promise
      }
  ]
