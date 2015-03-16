###
Copyright (c) 2002-2015 "Neo Technology,"
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
   'ribcage/security/HtmlEscaper']
  (DateFormat, HtmlEscaper) ->

    ### Used to generate x-axis time ticks for
    time series charts.
    ###
    class LineChartTimeTicker
      
      timezoneOffset : new Date().getTimezoneOffset() * 60
      
      scales : [
        {  # 30-min span
           maxSpan : 31 * 60, 
           tickLength : 5 * 60, 
           dateFormat : "HH:MM",
           findFirstTickFrom : (startTime) -> 
             timezoneOffset = new Date().getTimezoneOffset()
             time = new Date((startTime+timezoneOffset*60) * 1000)
             new Date(time.getFullYear(), time.getMonth(), time.getDate(), 0,timezoneOffset,0).getTime() / 1000
        }
        {  # 6-hour span
           maxSpan : 7 * 60 * 60, 
           tickLength : 60 * 60, 
           dateFormat : "HH:MM",
           findFirstTickFrom : (startTime) -> 
             timezoneOffset = new Date().getTimezoneOffset()
             time = new Date((startTime+timezoneOffset*60) * 1000)
             new Date(time.getFullYear(), time.getMonth(), time.getDate(),0,timezoneOffset,0).getTime() / 1000
             
        }
        {  # 1-day span
           maxSpan : 25 * 60 * 60, 
           tickLength : 6 * 60 * 60, 
           dateFormat : "dddd HH:MM",
           findFirstTickFrom : (startTime) -> 
             timezoneOffset = new Date().getTimezoneOffset()
             time = new Date((startTime+timezoneOffset*60) * 1000)
             new Date(time.getFullYear(), time.getMonth(), time.getDate(),0,timezoneOffset,0).getTime() / 1000
        }
        {  # 1-week span
           maxSpan : 8 * 24 * 60 * 60, 
           tickLength : 2 * 24 * 60 * 60, 
           dateFormat : "dddd dd mmmm",
           findFirstTickFrom : (startTime) ->
             timezoneOffset = new Date().getTimezoneOffset()
             time = new Date((startTime+timezoneOffset*60) * 1000)
             new Date(time.getFullYear(), time.getMonth(), time.getDate(),0,timezoneOffset,0).getTime() / 1000
        }
        {  # 1-month span
           maxSpan : 32 * 24 * 60 * 60, 
           tickLength : 6 * 24 * 60 * 60, 
           dateFormat : "dd mmmm",
           findFirstTickFrom : (startTime) -> 
             timezoneOffset = new Date().getTimezoneOffset()
             time = new Date((startTime+timezoneOffset*60) * 1000)
             new Date(time.getFullYear(), time.getMonth(), 0,0,timezoneOffset,1).getTime() / 1000
        }
        {  # 1-year span
           maxSpan : 370 * 24 * 60 * 60, 
           tickLength : 60 * 24 * 60 * 60, 
           dateFormat : "dd mmmm",
           findFirstTickFrom : (startTime) -> 
             timezoneOffset = new Date().getTimezoneOffset()
             time = new Date((startTime+timezoneOffset*60) * 1000)
             new Date(time.getFullYear(), time.getMonth(), 0,0,timezoneOffset,1).getTime() / 1000
        }
      ]

      constructor : () ->

 
      ### Get an array of flot-formatted x-axis ticks
      appropriate for a given time span.

      Returns ticks in this format:

      [[0, "zero"], [1.2, "one mark"], [2.4, "two marks"]]
      ###
      getTicks : (startTime, stopTime) ->
        ticks = []

        scale = @_getScaleFor stopTime - startTime
        currentPosition = scale.findFirstTickFrom startTime
        while currentPosition <= stopTime
          if currentPosition >= startTime
            label = DateFormat.format((currentPosition + @timezoneOffset) * 1000, scale.dateFormat)
            ticks.push([currentPosition,label])
          currentPosition += scale.tickLength
      
        ticks
  
      _getScaleFor : (timeSpan) ->
        for scale in @scales
          if scale.maxSpan > timeSpan
            return scale
        # Fall back to largest available scale
        @scales[@scales.length-1] 

      

)
