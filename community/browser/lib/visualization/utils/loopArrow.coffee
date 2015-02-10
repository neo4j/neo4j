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