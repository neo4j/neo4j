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
  ['ribcage/View'], 
  (View) ->
    class Dropdown extends View
      
      constructor : () ->
        super()
        @el = $ @el
        @el.hide()
        @el.addClass "dropdown"
        
        @listElement = $ '<ul></ul>'
        @el.append @listElement
        
        $('body').append(@el)
        
      isVisible : => @el.is(":visible")
      hide : => @el.hide()
      
      render : () ->
        @listElement.html('')
        for li in @getItems()
          @listElement.append li
        return this
      
      renderFor : (target) ->
        @render()
        @positionFor(target)
        @el.show()
        setTimeout @activateHideOnClickAnywhere, 0
        
      positionFor : (target) ->
        target = $ target
        {left, top} = target.offset()
        th = target.outerHeight()
        tw = target.outerWidth()
        
        ww = $(window).width()
        wh = $(window).height()
        
        pw = @el.outerWidth()
        ph = @el.outerHeight()
        
        if left + pw > ww
          left = left - (pw - tw)
        if top + ph > wh and not top - ph < 0
          top = top - (th + ph)
        
        @el.css {position:"absolute",top:top+th, left:left}
      
      activateHideOnClickAnywhere : =>
        $('body').bind('click', @clickedAnywhere)
        
      clickedAnywhere : =>
        $('body').unbind 'click', @clickedAnywhere
        @hide()
        
      #
      # Helpers for generating dropdown items
      #
      
      title : (title) -> "<li><h3>#{htmlEscape(title)}</h3></li>"
      divider : () -> "<li><hr /></li>"
      actionable : (contents, clickhandler) -> 
        el = $ "<li class='actionable'></li>"
        el.click clickhandler
        el.append contents
        el
      item : (contents) ->
        el = $ "<li></li>"
        el.append contents
        el
        
)
