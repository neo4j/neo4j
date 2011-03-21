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

define ['lib/backbone'], () ->
  
  class ServerStatistics extends Backbone.Model

    initialize : (options) =>
      @server = options.server
      @heartbeat = @server.heartbeat
      
      @setMonitorData @heartbeat.getCachedData()
      @heartbeat.addListener (ev) =>
        @setMonitorData ev.allData

    setMonitorData : (monitorData) =>

      timestamps = @toLocalTimestamps(monitorData.timestamps)
      update = {}
      for key, data of monitorData.data
        update["metric:#{key}"] = @addTimestampsToArray(data, timestamps)
      
      @set update

      @trigger "change:metrics"

    getMetrics : (keys) =>
      for key in keys
        val = @get "metric:#{key}"
        if val then val else []

    addTimestampsToArray : (data, timestamps) =>
      for i in [0..data.length-1]
        [timestamps[i], data[i]]

    toLocalTimestamps : (timestamps) =>
      tzo = (new Date()).getTimezoneOffset()  * 60 * 1000
      ((t) -> t - tzo) t for t in timestamps

