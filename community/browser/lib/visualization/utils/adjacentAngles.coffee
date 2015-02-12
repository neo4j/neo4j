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

class neo.utils.adjacentAngles

  findRuns: (angleList, minSeparation) ->

    p = 0
    start = 0
    end = 0
    runs = []
    minStart = () ->
      if runs.length == 0
        0
      else
        runs[0].start

    scanForDensePair = ->
      start = p
      end = angleList.wrapIndex(p + 1)
      if end == minStart()
        'done'
      else
        p = end
        if tooDense(start, end)
          extendEnd

        else
          scanForDensePair

    extendEnd = ->
      if p == minStart()
        'done'

      else if tooDense(start, angleList.wrapIndex(p + 1))
        end = angleList.wrapIndex(p + 1)
        p = end
        extendEnd

      else
        p = start
        extendStart

    extendStart = ->
      candidateStart = angleList.wrapIndex(p - 1)
      if tooDense(candidateStart, end) and candidateStart != end
        start = candidateStart
        p = start
        extendStart

      else
        runs.push
          start: start
          end: end
        p = end
        scanForDensePair

    tooDense = (start, end) ->
      run =
        start: start
        end: end
      angleList.angle(run) < angleList.length(run) * minSeparation

    stepCount = 0
    step = scanForDensePair
    while step != 'done'
      if stepCount++ > angleList.totalLength() * 10
        console.log 'Warning: failed to layout arrows', ("#{ key }: #{ value.angle }" for own key, value of angleList.list).join('\n'), minSeparation
        break
      step = step()

    runs