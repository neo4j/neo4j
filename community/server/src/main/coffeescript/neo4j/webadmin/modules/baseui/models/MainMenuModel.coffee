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
  ['ribcage/Model',
   'lib/amd/Underscore'], 
  (Model,_) ->
  
    class MainMenuModel extends Model
      
      class @Item extends Model
        
        getTitle : -> @get "title"
        getSubtitle : -> @get "subtitle"
        getUrl : -> @get "url"
        
        setUrl : (url) -> @set "url":url

      constructor : ->
        super()
        @_items = []
 
      addMenuItem : (item) ->
        @_items.push(item)
        @trigger "change:items"
        item.bind "change", => @trigger "change:items"

      getMenuItems : -> @_items

      getCurrentItem : ->
        url = location.hash
        for item in _(@getMenuItems()).sortBy( (i)-> (-i.getUrl().length) )
          if url.indexOf(item.getUrl()) is 0 or (url.length is 0 and item.getUrl() is "#")
            return item

        return null

)
