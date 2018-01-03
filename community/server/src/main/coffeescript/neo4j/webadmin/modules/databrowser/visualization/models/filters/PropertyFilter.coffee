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
  ['./Filter',
   './propertyFilterTemplate',
   '../../views/AbstractFilterView'], 
  (Filter, template, AbstractFilterView) ->

    class PropertyFilterView extends AbstractFilterView

      events : 
        'change .method' : 'methodChanged'
        'change .propertyName' : 'propertyNameChanged'
        'change .compareValue' : 'compareValueChanged'
        "click button.removeFilter" : "deleteFilter"
              
      render : () ->
        $(@el).html template()
        
        select = $(".method", @el)
        
        select.append("<option value='exists'>exists</option>")
        select.append("<option value='!exists'>doesn't exist</option>")
        
        for method, definition of PropertyFilter.compareMethods
          label = definition.label
          select.append("<option value='#{htmlEscape(method)}'>#{htmlEscape(label)}</option>")
        
        @uiSetMethod @filter.getMethodName()
        @uiSetPropertyName @filter.getPropertyName()
        @uiSetCompareValue @filter.getCompareValue()
        
        return this
        
      methodChanged : () =>
        method = $(".method", @el).val()
        @uiSetMethod method
        @filter.setMethodName method
        
      propertyNameChanged : () =>
        name = $('.propertyName',@el).val()
        @filter.setPropertyName name
      
      compareValueChanged : () =>
        val = $('.compareValue',@el).val()
        @filter.setCompareValue val
        
      uiSetMethod : (method) ->
        $(".method", @el).val method
        if PropertyFilter.compareMethods[method]?
          $('.compareValue',@el).show()
        else
          $('.compareValue',@el).hide()
        
      uiSetPropertyName : (prop) -> $('.propertyName',@el).val(prop)
      uiSetCompareValue : (val) -> $('.compareValue',@el).val(val)
          

    class PropertyFilter extends Filter
      
      @type : 'propertyFilter'
      
      @compareMethods : 
        '==' : {label : "is",    cmp : (actual, expected) -> actual == expected}
        '!=' : {label : "isn't", cmp : (actual, expected) -> actual != expected}
        '>' : {label : ">", cmp : (actual, expected) -> actual > expected}
        '<' : {label : "<", cmp : (actual, expected) -> actual < expected}
        '>=' : {label : ">=", cmp : (actual, expected) -> actual >= expected}
        '<=' : {label : "<=", cmp : (actual, expected) -> actual <= expected}
      
      defaults : 
        'method' : 'exists'
      
      getViewClass : ()-> PropertyFilterView
      getType : () -> PropertyFilter.type
      
      getMethodName : -> @get 'method'
      getPropertyName : -> @get 'propertyName'
      getCompareValue : -> @get 'compareValue'
      
      setMethodName :   (v) -> @set 'method', v
      setPropertyName : (v) -> @set 'propertyName', v
      setCompareValue : (v) -> @set 'compareValue', v
      
      matches : (item) =>
        method = @getMethodName()
        
        if item.neoNode?
          node = item.neoNode
          if method == 'exists'
            return node.hasProperty @getPropertyName()
          else if method == '!exists'
            return not node.hasProperty @getPropertyName()
          else if PropertyFilter.compareMethods[method]?
            cmp = PropertyFilter.compareMethods[method].cmp
            val = node.getProperty(@getPropertyName())
            cmpVal = @getCompareValue()
            
            cmpVal = Number cmpVal if _(val).isNumber()
            
            return cmp val, cmpVal
        return false
      
)
