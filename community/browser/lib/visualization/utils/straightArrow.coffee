class neo.utils.straightArrow

  constructor: (startRadius, endRadius, centreDistance, shaftRadius, headRadius, headHeight, captionLayout) ->

    @length = centreDistance - (startRadius + endRadius)

    @shaftLength = @length - headHeight
    startArrow = startRadius
    endShaft = startArrow + @shaftLength
    endArrow = startArrow + @length

    @midShaftPoint =
      x: startArrow + @shaftLength / 2
      y: 0

    @outline = () ->
      if captionLayout is "external"
        startBreak = startArrow + (@shaftLength - @shortCaptionLength) / 2
        endBreak = endShaft - (@shaftLength - @shortCaptionLength) / 2

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

  deflection: 0