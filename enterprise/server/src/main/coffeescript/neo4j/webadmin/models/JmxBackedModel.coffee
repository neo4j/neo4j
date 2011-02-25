
define ['lib/backbone'], () ->
  
  class JmxBackedModel extends Backbone.Model
    
    initialize : (options) =>
      @server = options.server
      for key, definition of @beans
        console.log key, definition

