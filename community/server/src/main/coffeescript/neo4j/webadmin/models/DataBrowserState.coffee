
define(
  ['neo4j/webadmin/security/HtmlEscaper',
   'lib/backbone'], 
  (HtmlEscaper) ->
  
    class DataBrowserState extends Backbone.Model
      
      defaults :
        type : null
        data : null
        query : ""
        queryOutOfSyncWithData : true

      initialize : (options) =>
        @server = options.server
        @escaper = new HtmlEscaper

      getEscapedQuery : =>
        @escaper.escape(@get "query")

      setQuery : (val, isForCurrentData=false, opts={}) =>
        @set {"queryOutOfSyncWithData": not isForCurrentData }, opts
        @set {"query" : val }, opts

      setData : (result, basedOnCurrentQuery=true, opts={}) =>
        @set({"data":result, "queryOutOfSyncWithData":basedOnCurrentQuery }, {silent:true})

        if result instanceof neo4j.models.Node
          @set({type:"node"}, opts)
        else if result instanceof neo4j.models.Relationship
          @set({type:"relationship"}, opts)
        else if _(result).isArray()
          @set({type:"list"}, opts)
        else
          @set({"data":null, type:"not-found"}, opts)

)
