define(function(){return function(vars){ with(vars||{}) { return "<div class=\"pad\"><table cellspacing=\"0\" class=\"info-table\"><tbody>" + 
(function () { if (primitives.isDataAvailable()) { return (
"<tr><td>" + 
primitives.get("NumberOfNodeIdsInUse") + 
"</td><th>nodes</th><td>" + 
primitives.get("NumberOfPropertyIdsInUse") + 
"</td><th>properties</th><td>" + 
primitives.get("NumberOfRelationshipIdsInUse") + 
"</td><th>relationships</th><td>" + 
primitives.get("NumberOfRelationshipTypeIdsInUse") + 
"</td><th>relationship types</th></tr>"
);} else { return ""; } }).call(this) +
(function () { if (diskUsage.isDataAvailable()) { return (
"<tr><td>" + 
Math.round(diskUsage.get("TotalStoreSize") / 1024) + " kB " + 
"</td><th>total disk usage</th><td>" + 
Math.round( diskUsage.getDatabaseSize() / 1024) + " kB " + "(" + diskUsage.getDatabasePercentage() + "%)" + 
"</td><th>database disk usage</th><td>" + 
Math.round( diskUsage.getLogicalLogSize() / 1024) + " kB " + "(" + diskUsage.getLogicalLogPercentage() + "%)" + 
"</td><th>logical log disk usage</th><td></td><th></th></tr>"
);} else { return ""; } }).call(this) +
(function () { if (cacheUsage.isDataAvailable()) { return (
"<tr><td>" + 
cacheUsage.get("NodeCacheSize") + 
"</td><th>cached nodes</th><td>" + 
cacheUsage.get("RelationshipCacheSize") + 
"</td><th>cached relationships</th><td>" + 
cacheUsage.get("CacheType") + 
"</td><th>is the cache type</th><td></td><th></th></tr>"
);} else { return ""; } }).call(this) + 
"</tbody></table><div class=\"break\"></div></div>";}}; });