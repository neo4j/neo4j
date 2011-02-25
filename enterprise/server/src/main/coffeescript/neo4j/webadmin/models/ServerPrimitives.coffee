
define ['./JmxBackedModel','lib/backbone'], () ->
  
  class ServerPrimitives extends JmxBackedModel
    
    beans :
      primitives : { domain : 'neo4j', name:'Primitive count' }

    
