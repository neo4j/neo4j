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
  ['lib/DateFormat'
   'ribcage/ui/Tooltip'
   'ribcage/security/HtmlEscaper'
   'lib/amd/Flot'
   'lib/amd/Underscore']
  (DateFormat, Tooltip, HtmlEscaper, Flot, _ ) ->

    class LineChart
      
      timezoneOffset = (new Date()).getTimezoneOffset()  * 60
      defaultSettings : 
        label : ""
        xaxis : 
          mode : "time"
          timeformat : "%H:%M:%S"
          min : 0
        yaxis : { }
        legend:
            position : 'nw'

        series:
          points : { show: true }
          lines : { show: true }

        grid  : { hoverable: true }
        colors : ["#490A3D","#BD1550","#E97F02","#F8CA00","#8A9B0F"]
        tooltipYFormatter : (v) -> Math.round v
        tooltipXFormatter : (v) ->
          DateFormat.format new Date( (v + timezoneOffset) * 1000 )

      constructor : (el) ->
        @el = $(el)
        @settings = @defaultSettings
        
        @htmlEscaper = new HtmlEscaper

        @tooltip = new Tooltip({ closeButton : false })
        
        @el.bind "plothover", @mouseOverPlot

      mouseOverPlot : (event, pos, item) =>
        if item
          if @previousHoverPoint != item.datapoint
            @previousHoverPoint = item.datapoint

            x = @settings.tooltipXFormatter(item.datapoint[0])
            y = @settings.tooltipYFormatter(item.datapoint[1])

            @tooltip.show("<b>" + @htmlEscaper.escape(item.series.label) + "</b><span class='chart-y'>" + @htmlEscaper.escape(y) + "</span><span class='chart-x'>" + @htmlEscaper.escape(x) + "</span>", [item.pageX, item.pageY])

        else
          @tooltip.hide()

      render : (data, opts) =>
        @settings = _.extend({}, @defaultSettings, opts)
        $.plot @el, data, @settings

      remove : () =>
        @el.unbind "plothover", @mouseOverPlot
        @tooltip.remove()
        @el.remove()

)
