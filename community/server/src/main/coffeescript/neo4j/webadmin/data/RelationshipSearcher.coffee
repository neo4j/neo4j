
define ["./ItemUrlResolver","lib/backbone"], (ItemUrlResolver) ->

  class RelationshipSearcher

    constructor : (server) ->
      @server = server
      @urlResolver = new ItemUrlResolver(server)
      @pattern = /^((rel)|(relationship)):([0-9]+)$/i

    match : (statement) =>
      @pattern.test(statement)
      
    exec : (statement) =>
      @server.rel( @urlResolver.getRelationshipUrl( @extractRelId(statement) ))

    extractRelId : (statement) =>
      match = @pattern.exec(statement)
      return match[4]
