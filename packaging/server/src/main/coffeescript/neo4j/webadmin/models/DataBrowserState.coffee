
define ['lib/backbone'], () ->
  
  class DataBrowserState extends Backbone.Model
    
    initialize : (options) =>
      @server = options.server
