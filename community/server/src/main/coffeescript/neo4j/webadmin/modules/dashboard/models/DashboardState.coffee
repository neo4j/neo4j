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
  ['ribcage/Model'], 
  (Model) ->
  
    class DashboardState extends Model

      charts :
        primitives :
          layers : [
            {label:"Nodes",         key:'node_count'},
            {label:"Properties",    key:'property_count'}, 
            {label:"Relationships", key:'relationship_count'}]
          chartSettings:
            yaxis :
              min : 0

        usageRequests :
          layers : [
           {label:"Count",    key:'request_count'}]
          chartSettings:
            yaxis :
              min : 0

        usageTimes :
          layers : [
            {label:"Time Mean", key:'request_mean_time'},
            {label:"Time Median", key:'request_median_time'},
            {label:"Time Max", key:'request_max_time'},
            {label:"Time Min", key:'request_min_time'}]
          chartSettings:
            yaxis :
              min : 0

        usageBytes :
          layers : [
            {label:"Bytes",    key:'request_bytes'}]
          chartSettings:
            yaxis :
              min : 0

        memory :
          layers : [{
            label:"Memory usage", 
            key:'memory_usage_percent',
            lines: { show: true, fill: true, fillColor: "#4f848f" }
          }]
          chartSettings:
            yaxis :
              min : 0
              max : 100
            tooltipYFormatter : (v) ->
              return Math.floor(v) + "%"

      zoomLevels : # All granularities approximate 30 points per timespan
        year : 
          xSpan : 60 * 60 * 24 * 365
          granularity : 60 * 60 * 24 * 12
          timeformat: "%d/%m %y"
        month : 
          xSpan : 60 * 60 * 24 * 30
          granularity : 60 * 60 * 24
          timeformat: "%d/%m"
        week :
          xSpan : 60 * 60 * 24 * 7
          granularity : 60 * 60 * 6
          timeformat: "%d/%m"
        day :
          xSpan : 60 * 60 * 24
          granularity : 60 * 48
          timeformat: "%H:%M"
        six_hours :
          xSpan : 60 * 60 * 6
          granularity : 60 * 12
          timeformat: "%H:%M"
        thirty_minutes :
          xSpan : 60 * 30
          granularity : 60
          timeformat: "%H:%M"

      initialize : (options) =>
        @setChartByKey "primitives"
        #@setChartByKey "usageRequests"
        #@setChartByKey "usageTimes"
        #@setChartByKey "usageBytes"
        @setZoomLevelByKey "six_hours"

      getChart : (key) =>
        @get "chart" + key
      
      getChartKey : () =>
        @get "chartKey"

      getZoomLevel : () =>
        @get "zoomLevel"

      getZoomLevelKey : () =>
        @get "zoomLevelKey"

      setZoomLevelByKey : (key) =>
        @set "zoomLevelKey" : key
        @setZoomLevel @zoomLevels[key]
      
      setZoomLevel : (zl) =>
        @set zoomLevel : zl

      setChartByKey : (key) =>
        @set "chartKey" : key
        @setChart key, @charts[key]
      
      setChart : (key, chart) =>
        tmp =  {}
        tmp["chart" + key] = chart
        @set tmp


)
