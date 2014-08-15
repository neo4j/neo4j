class neo.graphView
  constructor: (element, measureSize, graph, @style) ->
    layout = neo.layout.force()
    @viz = neo.viz(element, measureSize, graph, layout, @style)
    @callbacks = {}
    callbacks = @callbacks
    @viz.trigger = do ->
      (event, args...) ->
        callback.apply(null, args) for callback in (callbacks[event] or [])

  on: (event, callback) ->
    (@callbacks[event] ?= []).push(callback)
    @

  layout: (value) ->
    return layout unless arguments.length
    layout = value
    @

  grass: (value) ->
    return @style.toSheet() unless arguments.length
    @style.importGrass(value)
    @

  update: ->
    @viz.update()
    @

  resize: ->
    @viz.resize()
    @
