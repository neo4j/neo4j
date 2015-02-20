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
  .factory 'GraphExplorer', [
    '$q'
    'Cypher'
    'CypherGraphModel'
    'Settings'
    ($q, Cypher, CypherGraphModel, Settings) ->
      return  {
        exploreNeighbours: (node, graph, withInternalRelationships) ->
          q = $q.defer()
          @findNeighbours(node).then (neighboursResult) =>
            if neighboursResult.nodes.length > Settings.maxNeighbours
              return q.reject('Sorry! Too many neighbours')
            graph.addNodes(neighboursResult.nodes.map(CypherGraphModel.convertNode()))
            graph.addRelationships(neighboursResult.relationships.map(CypherGraphModel.convertRelationship(graph)))
            if withInternalRelationships
              @internalRelationships(graph, graph.nodes(), neighboursResult.nodes).then ->
                q.resolve()
            else
              q.resolve()
          q.promise

        findNeighbours: (node) ->
          q = $q.defer()
          Cypher.transaction()
          .commit("MATCH (a)-[r]-() WHERE id(a)= #{node.id} RETURN r;")
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

        collapseNeighbours: (node, graph) ->
          neighbours = graph.relationships().filter((r) -> r.source is node or r.target is node).map((r) ->
            node: if node is r.source then r.target else r.source
            relationship: r
          )
          neighbourRelationships = {}
          for neighbour in neighbours
            neighbourRelationships[neighbour.node.id] = []

          for r in graph.relationships()
            if neighbourRelationships.hasOwnProperty(r.source.id)
              neighbourRelationships[r.source.id].push r
            if neighbourRelationships.hasOwnProperty(r.target.id)
              neighbourRelationships[r.target.id].push r

          connects = (r, n1, n2) ->
            (r.source is n1 and r.target is n2) or (r.source is n2 and r.target is n1)

          for neighbour in neighbours
            nonOrphans = neighbourRelationships[neighbour.node.id].filter((r) -> not connects(r, neighbour.node, node))
            if nonOrphans.length is 0
              graph.removeRelationship neighbour.relationship
              graph.removeNode neighbour.node
      }
  ]
