/*
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
define(function(){return function(vars){ with(vars||{}) { return "<div class=\"pad\"><div class=\"span-third\"><table cellspacing=\"0\" class=\"info-table\"><tbody><tr><th>Nodes</th><td>" + 
primitives.get("NumberOfNodeIdsInUse") + 
"</td></tr><tr><th>Properties</th><td>" + 
primitives.get("NumberOfPropertyIdsInUse") + 
"</td></tr><tr><th>Relationships</th><td>" + 
primitives.get("NumberOfRelationshipIdsInUse") + 
"</td></tr><tr><th>Relationship types</th><td>" + 
primitives.get("NumberOfRelationshipTypeIdsInUse") + 
"</td></tr></tbody></table></div><div class=\"span-third\"><table cellspacing=\"0\" class=\"info-table\"><tbody><tr><th>Total disk usage</th><td>" + 
Math.round(diskUsage.get("TotalStoreSize") / 1024) + " kB " + 
"</td></tr><tr><th>Database size</th><td>" + 
Math.round( diskUsage.getDatabaseSize() / 1024) + " kB " + "(" + diskUsage.getDatabasePercentage() + "%)" + 
"</td></tr><tr><th>Logical log size</th><td>" + 
Math.round( diskUsage.getLogicalLogSize() / 1024) + " kB " + "(" + diskUsage.getLogicalLogPercentage() + "%)" + 
"</td></tr></tbody></table></div><div class=\"span-third last\"><table cellspacing=\"0\" class=\"info-table\"><tbody><tr><th>Cached nodes</th><td>" + 
cacheUsage.get("NodeCacheSize") + 
"</td></tr><tr><th>Cached relationships</th><td>" + 
cacheUsage.get("RelationshipCacheSize") + 
"</td></tr><tr><th>Cache type</th><td>" + 
cacheUsage.get("CacheType") + 
"</td></tr></tbody></table></div><div class=\"break\"></div></div>";}}; });