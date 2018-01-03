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
  ['ribcage/ui/LineChart'
   'ribcage/ui/LineChartTimeTicker'
   'ribcage/View'
   './charts'
   'lib/amd/jQuery'], 
  (LineChart, LineChartTimeTicker, View, template, $) ->
  
    class DashboardChartsView extends View
      
      template : template

      events : 
        'click .switch-dashboard-chart' : 'switchChartClicked'
        'click .switch-dashboard-zoom' : 'switchZoomClicked'
      
      initialize : (opts) =>
        @statistics = opts.statistics
        @dashboardState = opts.dashboardState
        @timeTicker = new LineChartTimeTicker()
        @bind()

      render : =>
        $(@el).html @template()

        @monitorChart = new LineChart($("#monitor-chart"))
        #@usageRequestsChart = new LineChart($("#usage-requests-chart"))
        #@usageTimeChart = new LineChart($("#usage-time-chart"))
        #@usageBytesChart = new LineChart($("#usage-bytes-chart"))
        @redrawAllCharts()

        @highlightChartSwitchTab @dashboardState.getChartKey()
        @highlightZoomTab @dashboardState.getZoomLevelKey()

        return this

      redrawAllCharts : =>
        @redrawChart @monitorChart, "primitives"
        #@redrawChart @usageRequestsChart, "usageRequests"
        #@redrawChart @usageTimeChart, "usageTimes"
        #@redrawChart @usageBytesChart, "usageBytes"

      redrawChart : (chart, name) =>
        if chart?
          chartDef = @dashboardState.getChart name
          zoomLevel = @dashboardState.getZoomLevel()

          metricKeys = for v in chartDef.layers
            v.key

          endTime = Math.round(new Date().getTime() / 1000) - @statistics.timezoneOffset
          startTime = endTime - zoomLevel.xSpan
          metrics = @statistics.getMetrics(metricKeys, startTime, zoomLevel.granularity)
          
          # Add meta info to each data layer
          data = for i in [0...metrics.length]
            _.extend({ data:metrics[i] }, chartDef.layers[i] )
         
          settings =
            xaxis : 
              min : startTime
              #mode : "time"
              ticks : @timeTicker.getTicks(startTime, endTime)
              #tickFormatter : (v) ->
              #  $.plot.formatDate new Date( v * 1000 ), zoomLevel.timeformat
          chart.render data, _.extend(chartDef.chartSettings || {}, settings)

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
        @monitorChart.remove() if @monitorChart?
        #@usageRequestsChart.remove() if @usageRequestsChart?
        #@usageTimeChart.remove() if @usageTimeChart?
        #@usageBytesChart.remove() if @usageBytesChart?
        super()

      bind : =>
        @dashboardState.bind "change:chart", @redrawAllCharts
        @dashboardState.bind "change:zoomLevel", @redrawAllCharts
        @statistics.bind "change:metrics", @redrawAllCharts

      unbind : =>
        @dashboardState.unbind "change:chart", @redrawAllCharts
        @dashboardState.unbind "change:zoomLevel", @redrawAllCharts
        @statistics.unbind "change:metrics", @redrawAllCharts

)
