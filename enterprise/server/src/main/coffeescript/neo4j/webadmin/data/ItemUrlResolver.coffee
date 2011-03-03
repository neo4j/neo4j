define ["lib/backbone"], () ->

  class ItemUrlResolver

    constructor : (server) ->
      @server = server

    getNodeUrl : (id) =>
      @server.url + "/db/data/node/" + id

    getRelationshipUrl : (id) =>
      @server.url + "/db/data/relationship/" + id
 
    extractNodeId : (url) =>
      @extractLastUrlSegment(url)

    extractRelationshipId : (url) =>
      @extractLastUrlSegment(url)

    extractLastUrlSegment : (url) =>
      if url.substr(-1) is "/"
        url = url.substr(0, url.length - 1)

      url.substr(url.lastIndexOf("/") + 1)
