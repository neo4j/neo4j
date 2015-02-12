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

class neo.utils.straightArrow

  constructor: (startRadius, endRadius, centreDistance, shaftWidth, headWidth, headHeight, captionLayout) ->

    @length = centreDistance - (startRadius + endRadius)

    @shaftLength = @length - headHeight
    startArrow = startRadius
    endShaft = startArrow + @shaftLength
    endArrow = startArrow + @length
    shaftRadius = shaftWidth / 2
    headRadius = headWidth / 2

    @midShaftPoint =
      x: startArrow + @shaftLength / 2
      y: 0

    @outline = (shortCaptionLength) ->
      if captionLayout is "external"
        startBreak = startArrow + (@shaftLength - shortCaptionLength) / 2
        endBreak = endShaft - (@shaftLength - shortCaptionLength) / 2

        [
          'M', startArrow, shaftRadius,
          'L', startBreak, shaftRadius,
          'L', startBreak, -shaftRadius,
          'L', startArrow, -shaftRadius,
          'Z'
          'M', endBreak, shaftRadius,
          'L', endShaft, shaftRadius,
          'L', endShaft, headRadius,
          'L', endArrow, 0,
          'L', endShaft, -headRadius,
          'L', endShaft, -shaftRadius,
          'L', endBreak, -shaftRadius,
          'Z'
        ].join(' ')
      else
        [
          'M', startArrow, shaftRadius,
          'L', endShaft, shaftRadius,
          'L', endShaft, headRadius,
          'L', endArrow, 0,
          'L', endShaft, -headRadius,
          'L', endShaft, -shaftRadius,
          'L', startArrow, -shaftRadius,
          'Z'
        ].join(' ')

    @overlay = (minWidth) ->
      radius = Math.max(minWidth / 2, shaftRadius)
      [
        'M', startArrow, radius,
        'L', endArrow, radius,
        'L', endArrow, -radius,
        'L', startArrow, -radius,
        'Z'
      ].join(' ')      
      
  deflection: 0
