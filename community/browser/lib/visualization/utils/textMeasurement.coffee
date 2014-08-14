neo.utils.measureText = do ->
  measureUsingCanvas = (text, font) ->
    canvasSelection = d3.select('canvas#textMeasurementCanvas').data([this])
    canvasSelection.enter().append('canvas')
      .attr('id', 'textMeasurementCanvas')
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

  return (text, fontFamily, fontSize) ->
    font = 'normal normal normal ' + fontSize + 'px/normal ' + fontFamily;
    cache(text + font, () ->
      measureUsingCanvas(text, font)
    )
