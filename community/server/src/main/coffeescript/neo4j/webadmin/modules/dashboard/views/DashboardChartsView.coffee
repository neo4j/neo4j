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
  ['ribcage/ui/LineChart'
   'ribcage/View',
   './charts','lib/backbone'], 
  (LineChart, View, template) ->
  
    class DashboardChartsView extends View
      
      template : template

      events : 
        'click .switch-dashboard-chart' : 'switchChartClicked'
        'click .switch-dashboard-zoom' : 'switchZoomClicked'
      
      initialize : (opts) =>
        @statistics = opts.statistics
        @dashboardState = opts.dashboardState
        @bind()

      render : =>
        $(@el).html @template()

        @chart = new LineChart($("#monitor-chart"))
        @redrawChart()

        @highlightChartSwitchTab @dashboardState.getChartKey()
        @highlightZoomTab @dashboardState.getZoomLevelKey()

        return this

      redrawChart : =>
        if @chart?
          chartDef = @dashboardState.getChart()
          zoomLevel = @dashboardState.getZoomLevel()

          metricKeys = for v in chartDef.layers
            v.key

          startTime = Math.round new Date().getTime() / 1000 - zoomLevel.xSpan
          metrics = @statistics.getMetrics(metricKeys, startTime, zoomLevel.granularity)
          
          # Add meta info to each data layer
          data = for i in [0...metrics.length]
            _.extend({ data:metrics[i] }, chartDef.layers[i] )

          settings =
            xaxis : 
              min : startTime - @statistics.timezoneOffset
              mode : "time"
              timeformat : zoomLevel.timeformat
              tickFormatter : (v) ->
                $.plot.formatDate new Date( v * 1000 ), zoomLevel.timeformat
          @chart.render data, _.extend(chartDef.chartSettings || {}, settings)

      switchChartClicked : (ev) =>
        @highlightChartSwitchTab $(ev.target).val()
        @dashboardState.setChartByKey $(ev.target).val()
      
      switchZoomClicked : (ev) =>
        @highlightZoomTab $(ev.target).val()
        @dashboardState.setZoomLevelByKey $(ev.target).val()

      highlightChartSwitchTab : (tabKey) =>
        $("button.switch-dashboard-chart", @el).removeClass("current")
        $("button.switch-dashboard-chart[value='#{tabKey}']", @el).addClass("current")

      highlightZoomTab : (tabKey) =>
        $("button.switch-dashboard-zoom", @el).removeClass("current")
        $("button.switch-dashboard-zoom[value='#{tabKey}']", @el).addClass("current")

      remove : =>
        @unbind()
        if @chart?        
          @chart.remove()
        super()

      bind : =>
        @dashboardState.bind "change:chart", @redrawChart
        @dashboardState.bind "change:zoomLevel", @redrawChart
        @statistics.bind "change:metrics", @redrawChart

      unbind : =>
        @dashboardState.unbind "change:chart", @redrawChart
        @dashboardState.unbind "change:zoomLevel", @redrawChart
        @statistics.unbind "change:metrics", @redrawChart

)
