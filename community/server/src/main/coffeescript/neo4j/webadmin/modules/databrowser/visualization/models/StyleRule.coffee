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
  ['./Filters'
   './style',
   'ribcage/LocalModel'], 
  (Filters, style, LocalModel) ->

    {NodeStyle} = style

    class StyleRule extends LocalModel
      
      defaults : 
        target : 'node'
        style : {}
        order : 0
      
      initialize : () ->
        @initNestedModel('filters', Filters)
        @initNestedModel('style', { deserialize : (raw) => 
          return new NodeStyle(raw)
        })
      
      setTarget : (target) -> @set target:target
      getTarget : () -> @get 'target'
      
      getStyle : () -> @style
      setStyle : (s) -> this.style = s
      
      getOrder : () -> @get 'order'
      setOrder : (order) -> @set 'order',order
      
      getTargetEntity : () -> @getTarget().split(':')[0]
      getTargetEntityType : () -> @getTarget().split(':')[1]
      hasTargetEntityType : () -> @getTarget().split(':').length > 1
      
      appliesTo : (item, type) ->
        if type != @getTargetEntity() or (@hasTargetEntityType() and item.type != @getTargetEntityType())
          return false
        
        for filter in @filters.models
          if not filter.matches(item)
            return false
        return true
        
      applyStyleTo : (target) ->
        style = @getStyle()
        if style?
          style.applyTo target
)
