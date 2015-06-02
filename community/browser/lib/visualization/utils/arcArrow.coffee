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

class neo.utils.arcArrow

  constructor: (startRadius, endRadius, endCentre, @deflection, arrowWidth, headWidth, headLength, captionLayout) ->
    square = (l) ->
      l * l

    deflectionRadians = @deflection * Math.PI / 180
    startAttach =
      x: Math.cos( deflectionRadians ) * (startRadius),
      y: Math.sin( deflectionRadians ) * (startRadius)

    radiusRatio = startRadius / (endRadius + headLength)
    homotheticCenter = -endCentre * radiusRatio / (1 - radiusRatio)

    intersectWithOtherCircle = (fixedPoint, radius, xCenter, polarity) ->
      gradient = fixedPoint.y / (fixedPoint.x - homotheticCenter)
      hc = fixedPoint.y - gradient * fixedPoint.x

      A = 1 + square(gradient)
      B = 2 * (gradient * hc - xCenter)
      C = square(hc) + square(xCenter) - square(radius)

      intersection = { x: (-B + polarity * Math.sqrt(square(B) - 4 * A * C)) / (2 * A) }
      intersection.y = (intersection.x - homotheticCenter) * gradient

      intersection

    endAttach = intersectWithOtherCircle(startAttach, endRadius + headLength, endCentre, -1)

    g1 = -startAttach.x / startAttach.y
    c1 = startAttach.y + (square(startAttach.x) / startAttach.y)
    g2 = -(endAttach.x - endCentre) / endAttach.y
    c2 = endAttach.y + (endAttach.x - endCentre) * endAttach.x / endAttach.y

    cx = ( c1 - c2 ) / (g2 - g1)
    cy = g1 * cx + c1

    arcRadius = Math.sqrt(square(cx - startAttach.x) + square(cy - startAttach.y))
    startAngle = Math.atan2(startAttach.x - cx, cy - startAttach.y)
    endAngle = Math.atan2(endAttach.x - cx, cy - endAttach.y)
    sweepAngle = endAngle - startAngle
    if @deflection > 0
      sweepAngle = 2 * Math.PI - sweepAngle

    @shaftLength = sweepAngle * arcRadius
    if startAngle > endAngle
      @shaftLength = 0

    midShaftAngle = (startAngle + endAngle) / 2
    if @deflection > 0
      midShaftAngle += Math.PI
    @midShaftPoint =
      x: cx + arcRadius * Math.sin(midShaftAngle)
      y: cy - arcRadius * Math.cos(midShaftAngle)

    startTangent = (dr) ->
      dx = (if dr < 0 then 1 else -1) * Math.sqrt(square(dr) / (1 + square(g1)))
      dy = g1 * dx
      {
        x: startAttach.x + dx,
        y: startAttach.y + dy
      }

    endTangent = (dr) ->
      dx = (if dr < 0 then -1 else 1) * Math.sqrt(square(dr) / (1 + square(g2)))
      dy = g2 * dx
      {
        x: endAttach.x + dx,
        y: endAttach.y + dy
      }

    angleTangent = (angle, dr) ->
      {
        x: cx + (arcRadius + dr) * Math.sin(angle),
        y: cy - (arcRadius + dr) * Math.cos(angle)
      }

    endNormal = (dc) ->
      dx = (if dc < 0 then -1 else 1) * Math.sqrt(square(dc) / (1 + square(1 / g2)))
      dy = dx / g2
      {
        x: endAttach.x + dx
        y: endAttach.y - dy
      }

    endOverlayCorner = (dr, dc) ->
      shoulder = endTangent(dr)
      arrowTip = endNormal(dc)
      {
        x: shoulder.x + arrowTip.x - endAttach.x,
        y: shoulder.y + arrowTip.y - endAttach.y
      }

    coord = (point) ->
      "#{point.x},#{point.y}"

    shaftRadius = arrowWidth / 2
    headRadius = headWidth / 2
    positiveSweep = if startAttach.y > 0 then 0 else 1
    negativeSweep = if startAttach.y < 0 then 0 else 1

    @outline = (shortCaptionLength) ->
      if startAngle > endAngle
        return [
          'M', coord(endTangent(-headRadius)),
          'L', coord(endNormal(headLength)),
          'L', coord(endTangent(headRadius)),
          'Z'
        ].join(' ')

      if captionLayout is 'external'
        captionSweep = shortCaptionLength / arcRadius
        if @deflection > 0
          captionSweep *= -1

        startBreak = midShaftAngle - captionSweep / 2
        endBreak = midShaftAngle + captionSweep / 2

        [
          'M', coord(startTangent(shaftRadius)),
          'L', coord(startTangent(-shaftRadius)),
          'A', arcRadius - shaftRadius, arcRadius - shaftRadius, 0, 0, positiveSweep, coord(angleTangent(startBreak, -shaftRadius)),
          'L', coord(angleTangent(startBreak, shaftRadius)),
          'A', arcRadius + shaftRadius, arcRadius + shaftRadius, 0, 0, negativeSweep, coord(startTangent(shaftRadius))
          'Z',
          'M', coord(angleTangent(endBreak, shaftRadius)),
          'L', coord(angleTangent(endBreak, -shaftRadius)),
          'A', arcRadius - shaftRadius, arcRadius - shaftRadius, 0, 0, positiveSweep, coord(endTangent(-shaftRadius)),
          'L', coord(endTangent(-headRadius)),
          'L', coord(endNormal(headLength)),
          'L', coord(endTangent(headRadius)),
          'L', coord(endTangent(shaftRadius)),
          'A', arcRadius + shaftRadius, arcRadius + shaftRadius, 0, 0, negativeSweep, coord(angleTangent(endBreak, shaftRadius))
        ].join(' ')
      else
        [
          'M', coord(startTangent(shaftRadius)),
          'L', coord(startTangent(-shaftRadius)),
          'A', arcRadius - shaftRadius, arcRadius - shaftRadius, 0, 0, positiveSweep, coord(endTangent(-shaftRadius)),
          'L', coord(endTangent(-headRadius)),
          'L', coord(endNormal(headLength)),
          'L', coord(endTangent(headRadius)),
          'L', coord(endTangent(shaftRadius)),
          'A', arcRadius + shaftRadius, arcRadius + shaftRadius, 0, 0, negativeSweep, coord(startTangent(shaftRadius))
        ].join(' ')

    @overlay = (minWidth) ->
      radius = Math.max(minWidth / 2, shaftRadius)

      [
        'M', coord(startTangent(radius)),
        'L', coord(startTangent(-radius)),
        'A', arcRadius - radius, arcRadius - radius, 0, 0, positiveSweep, coord(endTangent(-radius)),
        'L', coord(endOverlayCorner(-radius, headLength)),
        'L', coord(endOverlayCorner(radius, headLength)),
        'L', coord(endTangent(radius)),
        'A', arcRadius + radius, arcRadius + radius, 0, 0, negativeSweep, coord(startTangent(radius))
      ].join(' ')
