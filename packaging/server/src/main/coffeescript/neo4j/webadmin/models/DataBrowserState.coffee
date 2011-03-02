
define ['lib/backbone'], () ->
  
  class DataBrowserState extends Backbone.Model
    
    defaults :
      type : null
      data : null

    initialize : (options) =>
      @server = options.server
