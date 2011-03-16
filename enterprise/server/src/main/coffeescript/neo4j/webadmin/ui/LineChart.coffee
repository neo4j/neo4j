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
  ['lib/DateFormat'
   'neo4j/webadmin/ui/Tooltip'
   'lib/jquery.flot','lib/backbone']
  (DateFormat, Tooltip) ->
    class LineChart
      
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
        colors : ["#326a75","#4f848f","#a0c2c8","#00191e"]
        tooltipYFormatter : (v) -> Math.round(v)
        tooltipXFormatter : (v) -> DateFormat.format(new Date(v))

      constructor : (el) ->
        @el = $(el)
        @settings = @defaultSettings

        @tooltip = new Tooltip({ closeButton : false })
        
        @el.bind "plothover", @mouseOverPlot

      mouseOverPlot : (event, pos, item) =>
        if item
          if @previousHoverPoint != item.datapoint
            @previousHoverPoint = item.datapoint

            x = @settings.tooltipXFormatter(item.datapoint[0])
            y = @settings.tooltipYFormatter(item.datapoint[1])

            @tooltip.show("<b>" + item.series.label + "</b><span class='chart-y'>" + y + "</span><span class='chart-x'>" + x + "</span>", [item.pageX, item.pageY])

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
