class neo.Renderer
  constructor: (opts = {})->
    neo.utils.extend(@, opts)
    @onGraphChange ?= ->
    @onTick ?= ->