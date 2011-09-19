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
        @filter.set method : method
        
      propertyNameChanged : () =>
        name = $('.propertyName',@el).val()
        @filter.set property : name
      
      compareValueChanged : () =>
        val = $('.compareValue',@el).val()
        @filter.set compareValue : val
        
      uiSetMethod : (method) ->
        $(".method", @el).val method
        if PropertyFilter.compareMethods[method]?
          $('.compareValue',@el).show()
        else
          $('.compareValue',@el).hide()
        
      uiSetPropertyName : (prop) -> $('.propertyName',@el).val(prop)
      uiSetCompareValue : (val) -> $('.compareValue',@el).val(val)
          

    class PropertyFilter extends Filter
      
      @name : 'propertyFilter'
      
      @methods : 
        'exists' : (item, propertyName) -> true
        'compare' : (item, propertyName) -> true
      
      @compareMethods : 
        '==' : {label : "is",    filter : (actual, expected) -> actual == expected}
        '!=' : {label : "isn't", filter : (actual, expected) -> actual != expected}
      
      defaults : 
        'method' : 'exists'
      
      getViewClass : ()-> PropertyFilterView
      getType : () -> PropertyFilter.name
      
      getMethodName : -> @get 'method'
      getPropertyName : -> @get 'propertyName'
      getCompareValue : -> @get 'compareValue'
      
      matches : (item) =>
        method = @getMethodName()
        if method == 'exists'
          return item.neoNode.hasProperty @getPropertyName()
        else if @compareMethods[method]?
          cmp = @compareMethods[method]
          return cmp item.getProperty(@getPropertyName()), getCompareValue()
        return false
      
)
