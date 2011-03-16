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
  ['lib/backbone'], 
  (PropertyContainer) ->
  
    class DashboardState extends Backbone.Model

      charts :
        primitives :
          metrics : ['node_count','property_count','relationship_count']
        memory :
          metrics : ['memory_usage_percent']

      zoomLevels :
        year : 
          xSpan : 1000 * 60 * 60 * 24 * 365
          timeformat: "%d/%m %y"
        month : 
          xSpan : 1000 * 60 * 60 * 24 * 30
          timeformat: "%d/%m"
        week :
          xSpan : 1000 * 60 * 60 * 24 * 7
          timeformat: "%d/%m"
        day :
          xSpan : 1000 * 60 * 60 * 24
          timeformat: "%H:%M"
        six_hours :
          xSpan : 1000 * 60 * 60 * 6
          timeformat: "%H:%M"
        thirty_minutes :
          xSpan : 1000 * 60 * 30
          timeformat: "%H:%M"

      initialize : (options) =>
        @set
          chart : @charts.primitives
          zoomLevel : @zoomLevels.six_hours

      getChart : () =>
        @get "chart"

      getZoomLevel : () =>
        @get "zoomLevel"

      setZoomLevelByKey : (key) =>
        @setZoomLevel @zoomLevels[key]
      
      setZoomLevel : (zl) =>
        @set zoomLevel : zl

      setChartByKey : (key) =>
        @setChart @charts[key]
      
      setChart : (chart) =>
        @set chart : chart
      

)
