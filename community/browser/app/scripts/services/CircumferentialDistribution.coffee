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

angular.module('neo4jApp.services')
  .service 'CircumferentialDistribution', () ->

    @distribute = (arrowAngles, minSeparation) ->
      list = []
      for key, angle of arrowAngles.floating
        list.push
          key: key
          angle: angle

      list.sort((a, b) -> a.angle - b.angle)

      runsOfTooDenseArrows = []

      length = (startIndex, endIndex) ->
        if startIndex < endIndex
          endIndex - startIndex + 1
        else
          endIndex + list.length - startIndex + 1

      angle = (startIndex, endIndex) ->
        if startIndex < endIndex
          list[endIndex].angle - list[startIndex].angle
        else
          360 - (list[startIndex].angle - list[endIndex].angle)

      tooDense = (startIndex, endIndex) ->
        angle(startIndex, endIndex) < length(startIndex, endIndex) * minSeparation

      wrapIndex = (index) ->
        if index == -1
          list.length - 1
        else if index >= list.length
          index - list.length
        else
          index

      wrapAngle = (angle) ->
        if angle >= 360
          angle - 360
        else
          angle

      expand = (startIndex, endIndex) ->
        if length(startIndex, endIndex) < list.length
          if tooDense(startIndex, wrapIndex(endIndex + 1))
            return expand startIndex, wrapIndex(endIndex + 1)
          if tooDense(wrapIndex(startIndex - 1), endIndex)
            return expand wrapIndex(startIndex - 1), endIndex

        runsOfTooDenseArrows.push(
          start: startIndex
          end: endIndex
        )

      for i in [0..list.length - 2]
        if tooDense(i, i + 1)
          expand i, i + 1

      result = {}

      for run in runsOfTooDenseArrows
        center = list[run.start].angle + angle(run.start, run.end) / 2
        runLength = length(run.start, run.end)
        for i in [0..runLength - 1]
          rawAngle = center + (i - (runLength - 1) / 2) * minSeparation
          result[list[wrapIndex(run.start + i)].key] = wrapAngle(rawAngle)

      for key, angle of arrowAngles.floating
        if !result[key]
          result[key] = arrowAngles.floating[key]

      result

    return @
