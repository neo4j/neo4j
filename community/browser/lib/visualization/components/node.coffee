class neo.models.Node
  constructor: (@id, @labels, properties) ->
    @propertyMap = properties
    @propertyList = for own key,value of properties
        { key: key, value: value }

  toJSON: ->
    @propertyMap

  isNode: true
  isRelationship: false

  relationshipCount: (graph) ->
    node = @
    rels = []
    rels.push[relationship] for relationship in graph.relationships() when relationship.source is node or relationship.target is node
    rels.length
