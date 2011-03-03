
define ["./ItemUrlResolver","lib/backbone"], (ItemUrlResolver) ->

  class NodeSearcher

    constructor : (server) ->
      @server = server
      @urlResolver = new ItemUrlResolver(server)
      @pattern = /^(node:)?([0-9]+)$/i

    match : (statement) =>
      @pattern.test(statement)
      
    exec : (statement) =>
      @server.node( @urlResolver.getNodeUrl( @extractNodeId(statement) ))

    extractNodeId : (statement) =>
      match = @pattern.exec(statement)
      return match[2]
 
