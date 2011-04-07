###
Copyright (c) 2002-2011 "Neo Technology,"
Network Engine for Objects in Lund AB [http://neotechnology.com]

This file is part of Neo4j.

Neo4j is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
###

define(
  ['neo4j/webadmin/ui/Overlay'
   'neo4j/webadmin/views/View'
   'lib/backbone'], 
  (Overlay, View) ->
  
    class Dialog extends View
      
      className: "dialog"

      initialize : () ->
        @wrapper = $("<div class='dialog-wrap'></div>")
        @wrapper.append(@el)
        @attachedToBody = false
        @overlay = new Overlay()

      show : (timeout=false) =>
        
        @overlay.show()
        @bind()
        
        if not @attachedToBody
          $("body").append @wrapper

        @render()
        @wrapper.show()
        
        if timeout 
          setTimeout @hide, timeout

      bind : () ->
        @wrapper.bind("click", @wrapperClicked)
    
      unbind : () ->
        @wrapper.unbind("click", @wrapperClicked)

      wrapperClicked : (ev) =>
        if ev.originalTarget is ev.currentTarget
          @hide()

      hide : () =>
        @unbind()
        @wrapper.hide()
        @overlay.hide()

      remove : () =>
        @unbind()
        @wrapper.remove()
        @overlay.hide()

      detach : =>
        @wrapper.detach()

      attach : (parent) =>
        $(parent).append(@wrapper)
)
