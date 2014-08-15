neo.layout = do ->
  _layout = {}

  _layout.force = ->
    _force = {}

    _force.init = (render) ->
      forceLayout = {}

      linkDistance = 60

      d3force = d3.layout.force()
      .linkDistance(linkDistance)
      .charge(-1000)

      accelerateLayout = ->
        maxStepsPerTick = 100
        maxAnimationFramesPerSecond = 60
        maxComputeTime = 1000 / maxAnimationFramesPerSecond
        now = if window.performance
          () ->
            window.performance.now()
        else
          () ->
            Date.now()

        d3Tick = d3force.tick
        d3force.tick = () =>
          startTick = now()
          step = maxStepsPerTick
          while step-- and now() - startTick < maxComputeTime
            if d3Tick()
              maxStepsPerTick = 2
              return true
          render()
          false

      accelerateLayout()

      forceLayout.update = (graph, size) ->

        nodes         = neo.utils.cloneArray(graph.nodes())
        relationships = graph.relationships()

        radius = nodes.length * linkDistance / (Math.PI * 2)
        center =
          x: size[0] / 2
          y: size[1] / 2
        neo.utils.circularLayout(nodes, center, radius)

        d3force
        .nodes(nodes)
        .links(relationships)
        .size(size)
        .start()

      forceLayout.drag = d3force.drag
      forceLayout

    _force

  _layout