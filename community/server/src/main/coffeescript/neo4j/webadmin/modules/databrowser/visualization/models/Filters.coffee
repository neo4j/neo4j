###
Copyright (c) 2002-2018 "Neo Technology,"
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
      filterMap[f.type] = f
  
    class Filters extends LocalCollection
      
      filters : filterMap
      
      # Override the normal deserialization method, 
      # to allow us to deserialize to multiple different
      # filter types.
      deserializeItem : (raw) ->
        # Fix for a corruption bug where type was set to "d", rather than "propertyFilter"
        raw.type = PropertyFilter.type if raw.type is 'd'

        if @filters[raw.type]?
          return new @filters[raw.type](raw)
        throw new Error("Unknown filter type '#{raw.type}' for visualization profile")

)
