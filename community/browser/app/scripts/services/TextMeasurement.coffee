###!
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

'use strict';

angular.module('neo4jApp')
  .service 'TextMeasurement', () ->

    measureUsingCanvas = (text, font) ->
      canvasSelection = d3.select('canvas#textMeasurementCanvas').data([this])
      canvasSelection.enter().append('canvas')
        .attr('id', 'textMeasuringCanvas')
        .style('display', 'none')

      canvas = canvasSelection.node()
      context = canvas.getContext('2d')
      context.font = font
      context.measureText(text).width

    cache = do () ->
      cacheSize = 10000
      map = {}
      list = []
      (key, calc) ->
        cached = map[key]
        if cached
          cached
        else
          result = calc()
          if (list.length > cacheSize)
            delete map[list.splice(0, 1)]
            list.push(key)
          map[key] = result

    @measure = (text, fontFamily, fontSize) ->
      font = 'normal normal normal ' + fontSize + 'px/normal ' + fontFamily;
      cache(text + font, () ->
        measureUsingCanvas(text, font)
      )

    return @
