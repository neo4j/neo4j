###
Copyright (c) 2002-2015 "Neo Technology,"
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
  ['./cypherResult',
   'ribcage/View',
   'lib/amd/jQuery'], 
  (template, View, $) ->
  
    class CypherResultView extends View

      render : =>
        $(@el).html(template(
          columns : @queryResult.getColumns(),
          result : @queryResult.getResult(),
          id : (entity) ->
            value = entity
            if entity.self
              value=entity.self
            value.substr(value.lastIndexOf("/")+1)

          isArray : (value) ->
            value != null && typeof(value) == "object" && typeof(value.pop) == "function"
          isPath : (value) ->
            value != null && typeof(value) == "object" && typeof(value.length) == "number" && typeof(value.pop) != "function"
          isNode : (value) ->
            value != null && (typeof(value)=="string" && value.indexOf("/node/")!=-1 || typeof(value.self) != "undefined" && typeof(value.type) == "undefined")
          isRel : (value) ->
            value != null && (typeof(value)=="string" && value.indexOf("/relationship/")!=-1 || typeof(value.self) != "undefined" && typeof(value.type) != "undefined")
          toArray : (value) ->
            if !value.nodes then return value
            result = []
            for i in [0..value.length-1]
              result.push(value.nodes[i])
              result.push(value.relationships[i])
            result.push(value.nodes[value.length])
            result
        ))
        return this
      
      setData : (queryResult) =>
        @queryResult = queryResult

)
