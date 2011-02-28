
define ['./JmxBackedModel','lib/backbone'], (JmxBackedModel) ->
  
  class ServerPrimitives extends JmxBackedModel
    
    beans :
      primitives : { domain : 'neo4j', name:'Primitive count' }

    
