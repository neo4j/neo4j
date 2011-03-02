define(function(){return function(vars){ with(vars||{}) { return "<div class=\"span-third\"><div class=\"module\"><h2>Primitives</h2><table class=\"fancy\"><tbody><tr><th>Cached nodes</th><td>" + 
primitives.get("NumberOfNodeIdsInUse") + 
"</td></tr><tr><th>Properties</th><td>" + 
primitives.get("NumberOfPropertyIdsInUse") + 
"</td></tr><tr><th>Relationships</th><td>" + 
primitives.get("NumberOfRelationshipIdsInUse") + 
"</td></tr><tr><th>Relationship types</th><td>" + 
primitives.get("NumberOfRelationshipTypeIdsInUse") + 
"</td></tr></tbody></table></div></div><div class=\"span-third\"><div class=\"module\"><h2>Disk usage</h2><table class=\"fancy\"><tbody><tr><th>Total size</th><td>" + 
Math.round(diskUsage.get("TotalStoreSize") / 1024) + " kB " + 
"</td></tr><tr><th>Database size</th><td>" + 
Math.round( diskUsage.getDatabaseSize() / 1024) + " kB " + "(" + diskUsage.getDatabasePercentage() + "%)" + 
"</td></tr><tr><th>Logical log size</th><td>" + 
Math.round( diskUsage.getLogicalLogSize() / 1024) + " kB " + "(" + diskUsage.getLogicalLogPercentage() + "%)" + 
"</td></tr></tbody></table></div></div><div class=\"span-third last\"><div class=\"module\"><h2>Cache</h2><table class=\"fancy\"><tbody><tr><th>Cached nodes</th><td>" + 
cacheUsage.get("NodeCacheSize") + 
"</td></tr><tr><th>Cached relationships</th><td>" + 
cacheUsage.get("RelationshipCacheSize") + 
"</td></tr><tr><th>Cache type</th><td>" + 
cacheUsage.get("CacheType") + 
"</td></tr></tbody></table></div></div>";}}; });