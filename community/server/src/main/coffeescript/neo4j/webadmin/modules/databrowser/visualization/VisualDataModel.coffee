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
  
    class VisualDataModel

      constructor : (@groupingThreshold=5) ->
        @clear()
        
      clear : () ->
        @groupCount = 0

        @visualGraph = 
          nodes : {}
          edges : {}

        @data =
          relationships : {}
          nodes : {}
          groups : {}

      getVisualGraph : () ->
        # XXX
        # Arbor.js borks if given a graph with no relationships to render.
        # Add a "secret" hack node and relationship if there are no real
        # relationships.
        # (2011-04-07)
        if _(@visualGraph.edges).keys().length == 0
          for key, node of @visualGraph.nodes
            @visualGraph.nodes["#{key}-SECRET-HACK-NODE"] = { hidden : true }
            @visualGraph.edges[key] ?= {}
            @visualGraph.edges[key]["#{key}-SECRET-HACK-NODE"] = { hidden : true }
        @visualGraph
        
      addNode : (node, relationships, relatedNodes) =>

        if not @visualGraph.nodes[node.getSelf()]? or @visualGraph.nodes[node.getSelf()].type isnt "explored"
          @ungroup([node])
          @ungroup(relatedNodes)

          @data.nodes[node.getSelf()] ?= { node : node, groups : {}  }
          @visualGraph.nodes[node.getSelf()] = { neoNode : node, type : "explored" }
        
        # Add any related nodes to our local cache, if we don't already have them.
        for relatedNode in relatedNodes
          @data.nodes[relatedNode.getSelf()] ?= { node : relatedNode, groups : {} }

        potentialGroups = {incoming:{},outgoing:{}}
        for rel in relationships

          if @data.relationships[rel.getSelf()]?
            continue
          @data.relationships[rel.getSelf()] = rel

          otherUrl = rel.getOtherNodeUrl(node.getSelf())
          dir = if rel.isStartNode(node.getSelf()) then "outgoing" else "incoming"
          
          if not @visualGraph.nodes[otherUrl]?
            potentialGroups[dir][rel.getType()] ?= { relationships:[] }
            potentialGroups[dir][rel.getType()].relationships.push(rel)
          else
            @_addRelationship(rel.getStartNodeUrl(), rel.getEndNodeUrl(), rel)

        for dir, groups of potentialGroups
          for type, group of groups
            if group.relationships.length >= @groupingThreshold
              @_addGroup node, group.relationships, dir
            else
              for rel in group.relationships
                @_addUnexploredNode node, rel

      ungroup : (nodes) ->

        for node in nodes
          nodeUrl = node.getSelf()
          if @data.nodes[nodeUrl]?
            meta = @data.nodes[nodeUrl]
            for key, groupMeta of meta.groups
              group = groupMeta.group
              group.nodeCount--
              delete group.grouped[nodeUrl]

              for rel in groupMeta.relationships
                @_addUnexploredNode group.baseNode, rel

              if group.nodeCount <= 0
                @_removeGroup group

      unexplore : (node) ->
        nodeUrl = node.getSelf()

        if @_isLastExploredNode node
          return

        if @visualGraph.nodes[nodeUrl]?
          visualNode = @visualGraph.nodes[nodeUrl]
          visualNode.type = "unexplored"
          node.fixed = false

          potentiallRemove = @_getUnexploredNodesRelatedTo nodeUrl
          
          for relatedNodeUrl, relatedNodeMeta of potentiallRemove
            if not @_hasExploredRelationships relatedNodeUrl, nodeUrl
              @removeNode relatedNodeMeta.node

          @_removeGroupsFor node
          if not @_hasExploredRelationships nodeUrl
            @removeNode node

      removeNode : (node) ->
        nodeUrl = node.getSelf()
        delete @visualGraph.nodes[nodeUrl]
        delete @data.nodes[nodeUrl]
        @_removeRelationshipsFor(node)

      _isLastExploredNode : (node) ->
        nodeUrl = node.getSelf()
        for url, visualNode of @visualGraph.nodes
          if visualNode.type == "explored" and url != nodeUrl
            return false
        return true

      _getUnexploredNodesRelatedTo : (nodeUrl) ->
        found = []
        for fromUrl, toMap of @visualGraph.edges
          for toUrl, relMeta of toMap
            if fromUrl is nodeUrl
              if @visualGraph.nodes[toUrl].type? and @visualGraph.nodes[toUrl].type is "unexplored"
                found[toUrl] = @data.nodes[toUrl]
            if toUrl is nodeUrl
              if @visualGraph.nodes[fromUrl].type? and @visualGraph.nodes[fromUrl].type is "unexplored"
                found[fromUrl] = @data.nodes[fromUrl]

        return found

      _hasExploredRelationships : (nodeUrl, excludeNodeUrl="") ->
        for fromUrl, toMap of @visualGraph.edges
          for toUrl, relMeta of toMap
            if fromUrl is nodeUrl
              if not (toUrl is excludeNodeUrl) and @visualGraph.nodes[toUrl].type is "explored"
                return true
            if toUrl is nodeUrl
              if not (fromUrl is excludeNodeUrl) and @visualGraph.nodes[fromUrl].type is "explored"
                return true

        return false

      _addRelationship : (from, to, rel, relType=null) ->
        @visualGraph.edges[from] ?= {}
        @visualGraph.edges[from][to] ?= { length:.5, relationships : {}, directed:true, relType:relType }
        if rel != false
          @visualGraph.edges[from][to].relationships[rel.getSelf()] = rel

      _addUnexploredNode : (baseNode, rel) ->
        unexploredUrl = rel.getOtherNodeUrl(baseNode.getSelf())
        
        @visualGraph.nodes[unexploredUrl] ?=
          neoNode : @data.nodes[unexploredUrl].node
          type : "unexplored"

        @_addRelationship(rel.getStartNodeUrl(), rel.getEndNodeUrl(), rel)
        
      _addGroup : (baseNode, relationships, direction) ->

        baseNodeUrl = baseNode.getSelf()
        
        nodeCount = 0
        grouped = {}
        for rel in relationships 
          nodeUrl = rel.getOtherNodeUrl(baseNodeUrl)
          if not @data.nodes[nodeUrl]?
            continue
          nodeMeta = @data.nodes[nodeUrl]
          if not grouped[nodeUrl]?
            grouped[nodeUrl] = { node : nodeMeta.node, relationships : []}
            nodeCount++
          grouped[nodeUrl].relationships.push(rel)
        
        key = "group-#{@groupCount++}"

        group = @data.groups[key] = 
          key : key
          baseNode : baseNode
          grouped : grouped
          nodeCount : nodeCount

        @visualGraph.nodes[key] =
          key : key
          type : "group"
          group : group

        for url, meta of grouped
          @data.nodes[url].groups[key] = { group : group, relationships : meta.relationships }

        if direction is "outgoing"
          @_addRelationship(baseNode.getSelf(), key, false, relationships[0].getType())
        else
          @_addRelationship(key, baseNode.getSelf(), false, relationships[0].getType())

      _removeRelationshipsFor : (node) ->
        nodeUrl = node.getSelf()
        for fromUrl, toMap of @visualGraph.edges
          for toUrl, relMeta of toMap
            if toUrl is nodeUrl or fromUrl is nodeUrl
              for url, rel of @visualGraph.edges[fromUrl][toUrl].relationships
                delete @data.relationships[rel.getSelf()]
            if toUrl is nodeUrl
              delete @visualGraph.edges[fromUrl][toUrl]
        if @visualGraph.edges[nodeUrl]?
          delete @visualGraph.edges[nodeUrl]

      _removeRelationshipsInGroup : (group) ->
        
        for url, grouped of group.grouped
          node = @data.nodes[url]
          if node.groups[group.key]?
            delete node.groups[group.key]
          
          for rel in grouped.relationships
            delete @data.relationships[rel.getSelf()]

      _removeGroup : (group) ->

        delete @visualGraph.nodes[group.key]
        delete @data.groups[group.key]
        if @visualGraph.edges[group.key]?
          delete @visualGraph.edges[group.key]
        else
          if @visualGraph.edges[group.baseNode.getSelf()]? and @visualGraph.edges[group.baseNode.getSelf()][group.key]?
            delete @visualGraph.edges[group.baseNode.getSelf()][group.key]

      _removeGroupsFor : (node) ->
        nodeUrl = node.getSelf()
        for key, group of @data.groups
          if group.baseNode.getSelf() is nodeUrl
            @_removeRelationshipsInGroup group
            @_removeGroup group

)
