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
  ['ribcage/View'
   './filterListTemplate'
   './filterListSelect'], 
  (View, template, selectTemplate) ->

    FILTER_PLACEHOLDER_TEXT = "Search available nodes"

    class FilterList extends View
      
      events : 
        'keyup .filterText' : 'filterKeyUp'
        'change .filterText' : 'filterChanged'
        'focus .filterText' : 'filterFocused'
        'blur .filterText' : 'filterUnfocused'

      initialize : (items) ->
        @items = @filteredItems = items
        @filter = ""

        @keyMap = {}
        for item in items
          @keyMap[item.key] = item

        super()

      render : () ->
        $(@el).html(template( filter:@filter ))
        @renderListSelector()
        $(".filterText",@el).focus()

      height : (val) ->
        if val?
          $(".selectList", @el).height(val - 50)
        else
          super()
        

      renderListSelector : () ->
        $('.selectWrap', @el).html(selectTemplate(items : @filteredItems))

      filterKeyUp : (ev) ->
        if not ($(ev.target).val().toLowerCase() is @filter)  
          @filterChanged(ev)  

      filterFocused : (ev) ->
        if $(ev.target).val() is FILTER_PLACEHOLDER_TEXT
          $(ev.target).val("")
        
      filterUnfocused : (ev) ->
        if $(ev.target).val().length is 0
          $(ev.target).val(FILTER_PLACEHOLDER_TEXT)

      filterChanged : (ev) ->
        @filter = $(ev.target).val().toLowerCase()
        if @filter.length == 0 or @filter is FILTER_PLACEHOLDER_TEXT.toLowerCase()
          @filteredItems = @items
        else
          @filteredItems = []
          for item in @items
            if item.label.toLowerCase().indexOf(@filter) != -1
              @filteredItems.push(item)
        @renderListSelector()

      getFilteredItems : () ->
        keys = $(".selectList", @el).val()
        if keys != null
          @keyMap[key] for key in keys
        else 
          []
)
