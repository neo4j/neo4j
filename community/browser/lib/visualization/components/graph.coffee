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
      if !@findRelationship(relationship.id)?
        @relationshipMap[relationship.id] = relationship
        @_relationships.push(relationship)
    @

  findNode: (id) => @nodeMap[id]

  findRelationship: (id) => @relationshipMap[id]
