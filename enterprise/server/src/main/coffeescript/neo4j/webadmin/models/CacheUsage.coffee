
define ['./JmxBackedModel','lib/backbone'], (JmxBackedModel) ->
  
  class CacheUsage extends JmxBackedModel
    
    beans :
      cache : { domain : 'neo4j', name:'Cache' }
