
define ['./JmxBackedModel','lib/backbone'], (JmxBackedModel) ->
  
  class DiskUsage extends JmxBackedModel
    
    beans :
      diskUsage : { domain : 'neo4j', name:'Store file sizes' }

    getDatabaseSize : =>
      @get("TotalStoreSize") - @get("LogicalLogSize")
      
    getDatabasePercentage : =>
      Math.round( ((@get("TotalStoreSize") - @get("LogicalLogSize")) / @get("TotalStoreSize")) * 100 )

    getLogicalLogSize : =>
      @get("LogicalLogSize")

    getLogicalLogPercentage : =>
      Math.round((@get("LogicalLogSize") / @get("TotalStoreSize")) * 100)
