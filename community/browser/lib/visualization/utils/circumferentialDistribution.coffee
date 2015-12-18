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

'use strict'

neo.utils.distributeCircular = (arrowAngles, minSeparation) ->
  list = []
  for key, angle of arrowAngles.floating
    list.push
      key: key
      angle: angle
      fixed: false
  for key, angle of arrowAngles.fixed
    list.push
      key: key
      angle: angle
      fixed: true

  list.sort((a, b) -> a.angle - b.angle)

  angleList = new neo.utils.angleList(list)
  runsOfTooDenseArrows = new neo.utils.adjacentAngles().findRuns(angleList, minSeparation)

  wrapAngle = (angle) ->
    if angle >= 360
      angle - 360
    else if angle < 0
      angle + 360
    else
      angle

  result = {}

  splitByFixedArrows = (run) ->
    runs = []
    currentStart = run.start
    for i in [1..angleList.length(run)]
      wrapped = angleList.wrapIndex(run.start + i)
      if angleList.fixed(wrapped)
        runs.push
          start: currentStart
          end: wrapped
        currentStart = wrapped
    if not angleList.fixed(run.end)
      runs.push
        start: currentStart
        end: run.end
    runs

  for tooDenseRun in runsOfTooDenseArrows
    moveableRuns = splitByFixedArrows(tooDenseRun)
    for run in moveableRuns
      runLength = angleList.length(run)
      if angleList.fixed(run.start) and angleList.fixed(run.end)
        separation = angleList.angle(run) / runLength
        for i in [0..runLength]
          rawAngle = list[run.start].angle + i * separation
          result[list[angleList.wrapIndex(run.start + i)].key] = wrapAngle(rawAngle)
      else if angleList.fixed(run.start) and not angleList.fixed(run.end)
        for i in [0..runLength]
          rawAngle = list[run.start].angle + i * minSeparation
          result[list[angleList.wrapIndex(run.start + i)].key] = wrapAngle(rawAngle)
      else if not angleList.fixed(run.start) and angleList.fixed(run.end)
        for i in [0..runLength]
          rawAngle = list[run.end].angle - (runLength - i) * minSeparation
          result[list[angleList.wrapIndex(run.start + i)].key] = wrapAngle(rawAngle)
      else
        center = list[run.start].angle + angleList.angle(run) / 2
        for i in [0..runLength]
          rawAngle = center + (i - runLength / 2) * minSeparation
          result[list[angleList.wrapIndex(run.start + i)].key] = wrapAngle(rawAngle)


  for key of arrowAngles.floating
    if !result.hasOwnProperty(key)
      result[key] = arrowAngles.floating[key]

  for key of arrowAngles.fixed
    if !result.hasOwnProperty(key)
      result[key] = arrowAngles.fixed[key]

  result