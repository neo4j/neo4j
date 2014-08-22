neo.viz = (el, measureSize, graph, layout, style) ->
  viz =
    style: style

  el = d3.select(el)
  geometry = new NeoD3Geometry(style)

  # Arbitrary dimension used to keep force layout aligned with
  # the centre of the svg view-port.
  layoutDimension = 200

  # To be overridden
  viz.trigger = (event, args...) ->

  _trigger = (args...) ->
    d3.event?.stopPropagation()
    viz.trigger.apply(null, args)

  onCanvasClick = (el) ->
    console.log el
    _trigger('canvasClicked', el)

  onNodeClick = (node) -> _trigger('nodeClicked', node)

  onNodeDblClick = (node) -> _trigger('nodeDblClicked', node)

  onRelationshipClick = (relationship) ->
    _trigger('relationshipClicked', relationship)

  onNodeMouseOver = (node) -> _trigger('nodeMouseOver', node)
  onNodeMouseOut = (node) -> _trigger('nodeMouseOut', node)

  onRelMouseOver = (rel) -> _trigger('relMouseOver', rel)
  onRelMouseOut = (rel) -> _trigger('relMouseOut', rel)

  render = ->
    geometry.onTick(graph)

    nodeGroups = el.selectAll('g.node')
    .attr('transform', (d) ->
          "translate(#{ d.x },#{ d.y })")

    for renderer in neo.renderers.node
      nodeGroups.call(renderer.onTick, viz)

    relationshipGroups = el.selectAll('g.relationship')
    .attr('transform', (d) ->
          "translate(#{ d.source.x } #{ d.source.y }) rotate(#{ d.naturalAngle + 180 })")

    for renderer in neo.renderers.relationship
      relationshipGroups.call(renderer.onTick, viz)

  force = layout.init(render)

  viz.update = ->
    return unless graph

    layers = el.selectAll("g.layer").data(["relationships", "nodes"])

    layers
    .enter().append("g")
    .attr("class", (d) -> "layer " + d )
    .on('click', onCanvasClick) # Background click event

    nodes         = graph.nodes()
    relationships = graph.relationships()

    relationshipGroups = el.select("g.layer.relationships")
    .selectAll("g.relationship").data(relationships, (d) -> d.id)

    relationshipGroups.enter().append("g")
    .attr("class", "relationship")
    .on("click", onRelationshipClick)
    .on('mouseover', onRelMouseOver)
    .on('mouseout', onRelMouseOut)

    geometry.onGraphChange(graph)

    for renderer in neo.renderers.relationship
      relationshipGroups.call(renderer.onGraphChange, viz)

    relationshipGroups.exit().remove();

    nodeGroups = el.select("g.layer.nodes")
    .selectAll("g.node").data(nodes, (d) -> d.id)

    nodeGroups.enter().append("g")
    .attr("class", "node")
    .call(force.drag)
    .call(clickHandler)
    .on('mouseover', onNodeMouseOver)
    .on('mouseout', onNodeMouseOut)

    for renderer in neo.renderers.node
      nodeGroups.call(renderer.onGraphChange, viz);

    nodeGroups.exit().remove();

    force.update(graph, [layoutDimension, layoutDimension])
    viz.resize()
    viz.trigger('updated')

  viz.resize = ->
    size = measureSize()
    el.attr('viewBox', [
      0, (layoutDimension - size.height) / 2, layoutDimension, size.height
    ].join(' '))

  clickHandler = neo.utils.clickHandler()
  clickHandler.on 'click', onNodeClick
  clickHandler.on 'dblclick', onNodeDblClick

  viz
