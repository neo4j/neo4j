class neo.models.Relationship
  constructor: (@id, @source, @target, @type, properties) ->
    @propertyMap = properties
    @propertyList = for own key,value of @propertyMap
      { key: key, value: value }

  toJSON: ->
    @propertyMap

  isNode: false
  isRelationship: true