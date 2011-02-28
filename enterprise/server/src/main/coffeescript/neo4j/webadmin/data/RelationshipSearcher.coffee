
define [], () ->

  class RelationshipSearcher

    constructor : ->
      @pattern = /^((rel)|(relationship)):([0-9]+)$/i

    match : (statement) =>
      @pattern.test(statement)
      
    exec : (statement) =>
      location.hash = "#/data/relationship/" + @getRelationshipId(statement)

    getRelationshipId : (statement) ->
      @pattern.exec(statement)[4]

