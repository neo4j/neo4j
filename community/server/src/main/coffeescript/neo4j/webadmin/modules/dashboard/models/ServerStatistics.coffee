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

define ['lib/DateFormat', 'ribcage/Model'], (DateFormat, Model) ->

  class ServerStatistics extends Model

    initialize : (options) =>
  
      @timezoneOffset = (new Date()).getTimezoneOffset() * 60
      @server = options.server
      @heartbeat = @server.heartbeat

      @setMonitorData @heartbeat.getCachedData()
      @heartbeat.addListener (ev) =>
        @setMonitorData ev.allData

    setMonitorData : (monitorData) =>

      @timestamps = @toLocalTimestamps(monitorData.timestamps) # Because flot has no notion of timezones

      # We don't need data as granular as neo4js gives us. 
      # Weed some of it out to save memory.
      @indexesToSave = @getTimestampIndexes(0, 30)
      @timestamps = @pickFromArray(@timestamps, @indexesToSave)

      update = {}
      for key, data of monitorData.data
        update["metric:#{key}"] = @addTimestampsToArray(@pickFromArray(data, @indexesToSave), @timestamps)
      
      @set update

      @trigger "change:metrics"

    getMetrics : (keys, fromTimestamp=0, granularity=10000) =>
      # fromTimestamp and granularity handling here is
      # to keep data points low in order to lower the memory overhead of the charts.
      startIndex = @getClosestPreceedingTimestampIndex(fromTimestamp)
      if startIndex is -1
        startIndex = 0

      indexesToInclude = @getTimestampIndexes(startIndex, granularity)

      for key in keys
        val = @get "metric:#{key}"
        if val and startIndex < val.length
          @pickFromArray(val, indexesToInclude)
        else 
          []

    getClosestPreceedingTimestampIndex : (gmtTimestamp) ->
      localTimestamp = gmtTimestamp - @timezoneOffset
      for i in [0..@timestamps.length]
        if @timestamps[i] >= localTimestamp
          return if i > 0 then i-1 else i
      return 0

    pickFromArray : (array, indexesToExtract) ->
      for i in indexesToExtract
        array[i]

    getTimestampIndexes : (startIndex, granularity) ->
      out = []
      prevTimestamp = 0
      for i in [startIndex..@timestamps.length]
        delta = @timestamps[i] - prevTimestamp
        if delta > granularity
          prevTimestamp = @timestamps[i]
          out.push i

      return out

    addTimestampsToArray : (data, timestamps) =>
      for i in [0..data.length-1]
        [timestamps[i], data[i]]

    toLocalTimestamps : (timestamps) =>
      for t in timestamps
        t - @timezoneOffset

