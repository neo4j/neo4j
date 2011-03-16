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
  ['neo4j/webadmin/ui/LineChart'
   'neo4j/webadmin/templates/dashboard/charts','lib/backbone'], 
  (LineChart, template) ->
  
    class DashboardChartsView extends Backbone.View
      
      template : template

      events : 
        'click .switch-dashboard-chart' : 'switchChartClicked'
        'click .switch-dashboard-zoom' : 'switchZoomClicked'
      
      initialize : (opts) =>
        @statistics = opts.statistics
        @dashboardState = opts.dashboardState

        @dashboardState.bind "change:chart", @redrawChart
        @dashboardState.bind "change:zoomLevel", @redrawChart
        @statistics.bind "change:metrics", @redrawChart

      render : =>
        $(@el).html @template()

        @chart = new LineChart($("#monitor-chart"))
        @redrawChart()

        return this

      redrawChart : =>
        if @chart?
          chartSettings = @dashboardState.getChart()
          zoomLevel = @dashboardState.getZoomLevel()

          metrics = @statistics.getMetrics(chartSettings.metrics)

          xmin = 0
          if metrics[0].length > 0
            xmin = metrics[0][metrics[0].length-1][0] - zoomLevel.xSpan
          
          @chart.render metrics,
            xaxis : 
              min : xmin
              mode : "time"
              timeformat : zoomLevel.timeformat

      switchChartClicked : (ev) =>
        @dashboardState.setChartByKey $(ev.target).val()
      
      switchZoomClicked : (ev) =>
        @dashboardState.setZoomLevelByKey $(ev.target).val()

      remove : =>
        @dashboardState.unbind "change:chart", @redrawChart
        @dashboardState.unbind "change:zoomLevel", @redrawChart
        @statistics.unbind "change:metrics", @redrawChart
        delete @chart
        super()

)
