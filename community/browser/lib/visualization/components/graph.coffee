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
