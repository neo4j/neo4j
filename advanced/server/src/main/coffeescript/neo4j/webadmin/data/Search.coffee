
define ["./NodeSearcher", "./RelationshipSearcher"], (NodeSearcher, RelationshipSearcher) ->

  class Search

    constructor : (server) ->
      
      @searchers = [
        new NodeSearcher(server),
        new RelationshipSearcher(server)
      ]
    

    exec : (statement) =>
      searcher = @pickSearcher statement
      if searcher? 
        searcher.exec statement

    pickSearcher : (statement) =>
      
      for searcher in @searchers
        if searcher.match statement
          return searcher

