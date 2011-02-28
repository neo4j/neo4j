
define ["lib/backbone"], () ->

  class NodeSearcher

    constructor : ->
      @pattern = /^(node:)?([0-9]+)$/i

    match : (statement) =>
      @pattern.test(statement)
      
    exec : (statement) =>
      location.hash = "#/data/node/" + @getNodeId(statement)

    getNodeId : (statement) ->
      match = @pattern.exec(statement)
      return match[2]
 
