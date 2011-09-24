###
Copyright (c) 2002-2011 "Neo Technology,"
Network Engine for Objects in Lund AB [http://neotechnology.com]

This file is part of Neo4j.

Neo4j is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
###

define(
  ['./filters/PropertyFilter', 
   './filters/GroupSizeFilter', 
   'ribcage/LocalCollection'], 
  (PropertyFilter, GroupSizeFilter, LocalCollection) ->
  
    filters = [
      PropertyFilter
      GroupSizeFilter
    ]
    
    filterMap = {}
    for f in filters
      filterMap[f.name] = f
  
    class Filters extends LocalCollection
      
      filters : filterMap
      
      # Override the normal deserialization method, 
      # to allow us to deserialize to multiple different
      # filter types.
      deserializeItem : (json) ->
        if @filters[json.type]?
          return new @filters[json.type](json)
        throw new Error("Unknown filter type '#{json.type}' for visualization profile")

)
