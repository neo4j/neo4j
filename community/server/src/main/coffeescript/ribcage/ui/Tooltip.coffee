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
  ['lib/amd/jQuery', 'lib/amd/Underscore'], 
  ( $ , _ ) ->
    class Tooltip
    
      defaultSettings :
        hideOnMouseOut : true
        css : {}
        position : "above"
        closeButton : true
    
      constructor : (opts={}) ->
          
        @_tooltip = $("<div class='tooltip-wrap'></div>");
        @_tooltipContent = $("<div class='tooltip'></div>");
        @_closeButton = $("<div class='tooltip-close'></div>");
    
        @_currentPos = [0,0]
        @_currentContent = ""
        @_visible = false

        @settings = _.extend(@defaultSettings, opts)
        if @settings.hideOnMouseOut
          @_tooltip.bind "mouseout", @hide
    
        @_tooltip.addClass("tooltip-pos-" + @settings.position)
        @_tooltip.css(@settings.css)
        @_tooltip.append(@_tooltipContent)

        if @settings.closeButton
          @_tooltip.append @_closeButton
          @_closeButton.bind "click", @hide
    
        @_tooltip.appendTo "body"
    
        $(window).resize @onWindowResized
      
      show : (content, pos, timeout) =>
        @_currentPos = pos;
        @_currentContent = content;
        
        @_tooltipContent.html(content);
        pos = @getTooltipPositionFor(@getPosition(pos));
        @_tooltip.css({left: pos[0], top: pos[1] }).show();
        @_visible = true;
        
        if timeout 
          setTimeout @hide, timeout
    
      hide : () =>
        @_tooltip.hide()
        @_visible = false

      remove : () =>
        @_tooltip.unbind "mouseout", @hide
        @_closeButton.unbind "click", @hide
        @_tooltip.remove()

      onWindowResized : () =>
        if @_visible
          updatePosition = () => 
            @show(@_currentContent, @_currentPos)
          setTimeout( updatePosition, 0 )
    
      getPosition : (pos) =>
        if _.isArray(pos) 
          pos;
        else
          el = $(pos);
          pos = el.offset();
          switch @settings.position
            when "right"
              [pos.left + (el.width()), pos.top + (el.height()/2)]
            when "left"
              [pos.left, pos.top + (el.height()/2)]
            when "above"
              [pos.left + (el.width()/2), pos.top]
            else
              [pos.left + (el.width()/2), pos.top - el.height()]
      
      getTooltipPositionFor: (pointToPosition) =>
          switch @settings.position
            when "right"
              [pointToPosition[0]+10, pointToPosition[1]-(@_tooltip.height()/2)]
            when "left"
              [pointToPosition[0]-(@_tooltip.width()+10), pointToPosition[1]-(@_tooltip.height()/2)]
            when "above"
              [pointToPosition[0]-@_tooltip.width()/2, pointToPosition[1]-(@_tooltip.height()+10)]
            else
              [pointToPosition[0]-this._tooltip.width()/2, pointToPosition[1]+(@_tooltip.height())]
)

