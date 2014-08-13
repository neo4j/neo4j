class neo.utils.arcArrow

  constructor: (startRadius, endRadius, endCentre, @deflection, arrowWidth, headWidth, headLength, captionLayout) ->
    square = (l) ->
      l * l

    deflectionRadians = deflection * Math.PI / 180
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
    @shaftLength = sweepAngle * arcRadius
    midShaftAngle = (startAngle + endAngle) / 2
    if cy < 0
      midShaftAngle += Math.PI
    @midShaftPoint =
      x: cx + arcRadius * Math.sin(midShaftAngle)
      y: cy - arcRadius * Math.cos(midShaftAngle)

    startTangent = (dr) ->
      dx = (if dr < 0 then 1 else -1) * Math.sqrt(square(dr) / (1 + square(g1)))
      dy = g1 * dx
      return [
        startAttach.x + dx,
        startAttach.y + dy
      ].join(',')

    endTangent = (dr) ->
      dx = (if dr < 0 then -1 else 1) * Math.sqrt(square(dr) / (1 + square(g2)))
      dy = g2 * dx
      return [
        endAttach.x + dx,
        endAttach.y + dy
      ].join(',')

    angleTangent = (angle, dr) ->
      [
        cx + (arcRadius + dr) * Math.sin(angle),
        cy - (arcRadius + dr) * Math.cos(angle)
      ].join(',')

    endNormal = (dc) ->
      dx = (if dc < 0 then -1 else 1) * Math.sqrt(square(dc) / (1 + square(1 / g2)))
      dy = dx / g2
      return [
        endAttach.x + dx,
        endAttach.y - dy
      ].join(',')

    shaftRadius = arrowWidth / 2
    headRadius = headWidth / 2

    @outline = () ->
      if captionLayout is 'external'
        captionSweep = @shortCaptionLength / arcRadius
        if @deflection > 0
          captionSweep *= -1

        startBreak = midShaftAngle - captionSweep / 2
        endBreak = midShaftAngle + captionSweep / 2

        [
          'M', startTangent(shaftRadius),
          'L', startTangent(-shaftRadius),
          'A', arcRadius - shaftRadius, arcRadius - shaftRadius, 0, 0, (if startAttach.y > 0 then 0 else 1), angleTangent(startBreak, -shaftRadius),
          'L', angleTangent(startBreak, shaftRadius)
          'A', arcRadius + shaftRadius, arcRadius + shaftRadius, 0, 0, (if startAttach.y < 0 then 0 else 1), startTangent(shaftRadius)
          'Z',
          'M', angleTangent(endBreak, shaftRadius),
          'L', angleTangent(endBreak, -shaftRadius),
          'A', arcRadius - shaftRadius, arcRadius - shaftRadius, 0, 0, (if startAttach.y > 0 then 0 else 1), endTangent(-shaftRadius),
          'L', endTangent(-headRadius),
          'L', endNormal(headLength),
          'L', endTangent(headRadius),
          'L', endTangent(shaftRadius),
          'A', arcRadius + shaftRadius, arcRadius + shaftRadius, 0, 0, (if startAttach.y < 0 then 0 else 1), angleTangent(endBreak, shaftRadius)
        ].join(' ')
      else
        [
          'M', startTangent(shaftRadius),
          'L', startTangent(-shaftRadius),
          'A', arcRadius - shaftRadius, arcRadius - shaftRadius, 0, 0, (if startAttach.y > 0 then 0 else 1), endTangent(-shaftRadius),
          'L', endTangent(-headRadius),
          'L', endNormal(headLength),
          'L', endTangent(headRadius),
          'L', endTangent(shaftRadius),
          'A', arcRadius + shaftRadius, arcRadius + shaftRadius, 0, 0, (if startAttach.y < 0 then 0 else 1), startTangent(shaftRadius)
        ].join(' ')

    @apex =
      x: cx,
      y: if cy > 0 then cy - arcRadius else cy + arcRadius