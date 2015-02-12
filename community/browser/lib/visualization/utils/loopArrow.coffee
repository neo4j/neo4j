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

class neo.utils.loopArrow

  constructor: (nodeRadius, straightLength, spreadDegrees, shaftWidth, headWidth, headLength, captionHeight) ->

    spread = spreadDegrees * Math.PI / 180
    r1 = nodeRadius
    r2 = nodeRadius + headLength
    r3 = nodeRadius + straightLength
    loopRadius = r3 * Math.tan(spread / 2)
    shaftRadius = shaftWidth / 2
    @shaftLength = loopRadius * 3 + shaftWidth

    class Point
      constructor: (@x, @y) ->
      toString: ->
        "#{@x} #{@y}"

    normalPoint = (sweep, radius, displacement) ->
      localLoopRadius = radius * Math.tan(spread / 2)
      cy = radius / Math.cos(spread / 2)
      new Point(
              (localLoopRadius + displacement) * Math.sin(sweep),
              cy + (localLoopRadius + displacement) * Math.cos(sweep)
      )
    @midShaftPoint = normalPoint(0, r3, shaftRadius + captionHeight / 2 + 2)
    startPoint = (radius, displacement) ->
      normalPoint((Math.PI + spread) / 2, radius, displacement)
    endPoint = (radius, displacement) ->
      normalPoint(-(Math.PI + spread) / 2, radius, displacement)

    @outline = ->
      inner = loopRadius - shaftRadius
      outer = loopRadius + shaftRadius
      [
        'M', startPoint(r1, shaftRadius)
        'L', startPoint(r3, shaftRadius)
        'A', outer, outer, 0, 1, 1, endPoint(r3, shaftRadius)
        'L', endPoint(r2, shaftRadius)
        'L', endPoint(r2, -headWidth / 2)
        'L', endPoint(r1, 0)
        'L', endPoint(r2, headWidth / 2)
        'L', endPoint(r2, -shaftRadius)
        'L', endPoint(r3, -shaftRadius)
        'A', inner, inner, 0, 1, 0, startPoint(r3, -shaftRadius)
        'L', startPoint(r1, -shaftRadius)
        'Z'
      ].join(' ')

    @overlay = (minWidth) ->
      displacement = Math.max(minWidth / 2, shaftRadius)
      inner = loopRadius - displacement
      outer = loopRadius + displacement
      [
        'M', startPoint(r1, displacement)
        'L', startPoint(r3, displacement)
        'A', outer, outer, 0, 1, 1, endPoint(r3, displacement)
        'L', endPoint(r2, displacement)
        'L', endPoint(r2, -displacement)
        'L', endPoint(r3, -displacement)
        'A', inner, inner, 0, 1, 0, startPoint(r3, -displacement)
        'L', startPoint(r1, -displacement)
        'Z'
      ].join(' ')