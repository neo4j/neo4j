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

neo.viz = (el, measureSize, graph, layout, style) ->
  viz =
    style: style

  root = d3.select(el)
  base_group = root.append('g').attr("transform", "translate(0,0)")
  rect = base_group.append("rect")
    .style("fill", "none")
    .style("pointer-events", "all")
    #Make the rect cover the whole surface
    .attr('x', '-2500')
    .attr('y', '-2500')
    .attr('width', '5000')
    .attr('height', '5000')

  container = base_group.append('g')
  geometry = new NeoD3Geometry(style)

  # This flags that a panning is ongoing and won't trigger
  # 'canvasClick' event when panning ends.
  panning = no

  # Arbitrary dimension used to keep force layout aligned with
  # the centre of the svg view-port.
  layoutDimension = 200

  # To be overridden
  viz.trigger = (event, args...) ->

  onNodeClick = (node) => viz.trigger('nodeClicked', node)

  onNodeDblClick = (node) => viz.trigger('nodeDblClicked', node)

  onNodeDragToggle = (node) -> viz.trigger('nodeDragToggle', node)

  onRelationshipClick = (relationship) =>
    d3.event.stopPropagation()
    viz.trigger('relationshipClicked', relationship)

  onNodeMouseOver = (node) -> viz.trigger('nodeMouseOver', node)
  onNodeMouseOut = (node) -> viz.trigger('nodeMouseOut', node)

  onRelMouseOver = (rel) -> viz.trigger('relMouseOver', rel)
  onRelMouseOut = (rel) -> viz.trigger('relMouseOut', rel)

  panned = ->
    panning = yes
    container.attr("transform", "translate(" + d3.event.translate + ")scale(1)")

  zoomBehavior = d3.behavior.zoom().on("zoom", panned)

  # Background click event
  # Check if panning is ongoing
  rect.on('click', ->
    if not panning then viz.trigger('canvasClicked', el)
  )

  base_group.call(zoomBehavior)
      .on("dblclick.zoom", null)
      #Single click is not panning
      .on("click.zoom", -> panning = no)
      .on("DOMMouseScroll.zoom", null)
      .on("wheel.zoom", null)
      .on("mousewheel.zoom", null)

  newStatsBucket = ->
    bucket =
      frameCount: 0
      geometry: 0
      relationshipRenderers: do ->
        timings = {}
        neo.renderers.relationship.forEach((r) ->
          timings[r.name] = 0
        )
        timings
    bucket.duration = ->
      bucket.lastFrame - bucket.firstFrame
    bucket.fps = ->
      (1000 * bucket.frameCount / bucket.duration()).toFixed(1)
    bucket.lps = ->
      (1000 * bucket.layout.layoutSteps / bucket.duration()).toFixed(1)
    bucket.top = ->
      renderers = []
      for name, time of bucket.relationshipRenderers
        renderers.push {
          name: name
          time: time
        }
      renderers.push
        name: 'forceLayout'
        time: bucket.layout.layoutTime
      renderers.sort (a, b) -> b.time - a.time
      totalRenderTime = renderers.reduce ((prev, current) -> prev + current.time), 0
      renderers.map((d) -> "#{d.name}: #{(100 * d.time / totalRenderTime).toFixed(1)}%").join(', ')
    bucket

  currentStats = newStatsBucket()

  now = if window.performance and window.performance.now
    () ->
      window.performance.now()
  else
    () ->
      Date.now()

  render = ->
    currentStats.firstFrame = now() unless currentStats.firstFrame
    currentStats.frameCount++
    startRender = now()
    geometry.onTick(graph)
    currentStats.geometry += (now() - startRender)

    nodeGroups = container.selectAll('g.node')
    .attr('transform', (d) ->
          "translate(#{ d.x },#{ d.y })")

    for renderer in neo.renderers.node
      nodeGroups.call(renderer.onTick, viz)

    relationshipGroups = container.selectAll('g.relationship')
    .attr('transform', (d) ->
          "translate(#{ d.source.x } #{ d.source.y }) rotate(#{ d.naturalAngle + 180 })")

    for renderer in neo.renderers.relationship
      startRenderer = now()
      relationshipGroups.call(renderer.onTick, viz)
      currentStats.relationshipRenderers[renderer.name] += (now() - startRenderer)

    currentStats.lastFrame = now()

  force = layout.init(render)
    
  #Add custom drag event listeners
  force.drag().on('dragstart.node', (d) -> 
    onNodeDragToggle(d)
  ).on('dragend.node', () ->
    onNodeDragToggle()
  )

  viz.collectStats = ->
    latestStats = currentStats
    latestStats.layout = force.collectStats()
    currentStats = newStatsBucket()
    latestStats

  viz.update = ->
    return unless graph

    layers = container.selectAll("g.layer").data(["relationships", "nodes"])
    layers.enter().append("g")
    .attr("class", (d) -> "layer " + d )

    nodes         = graph.nodes()
    relationships = graph.relationships()

    relationshipGroups = container.select("g.layer.relationships")
    .selectAll("g.relationship").data(relationships, (d) -> d.id)

    relationshipGroups.enter().append("g")
    .attr("class", "relationship")
    .on("mousedown", onRelationshipClick)
    .on('mouseover', onRelMouseOver)
    .on('mouseout', onRelMouseOut)

    relationshipGroups
    .classed("selected", (relationship) -> relationship.selected)

    geometry.onGraphChange(graph)

    for renderer in neo.renderers.relationship
      relationshipGroups.call(renderer.onGraphChange, viz)

    relationshipGroups.exit().remove();

    nodeGroups = container.select("g.layer.nodes")
    .selectAll("g.node").data(nodes, (d) -> d.id)

    nodeGroups.enter().append("g")
    .attr("class", "node")
    .call(force.drag)
    .call(clickHandler)
    .on('mouseover', onNodeMouseOver)
    .on('mouseout', onNodeMouseOut)

    nodeGroups
    .classed("selected", (node) -> node.selected)
  
    for renderer in neo.renderers.node
      nodeGroups.call(renderer.onGraphChange, viz);

    nodeGroups.exit().remove();

    force.update(graph, [layoutDimension, layoutDimension])

    viz.resize()
    viz.trigger('updated')

  viz.resize = ->
    size = measureSize()
    root.attr('viewBox', [
      0, (layoutDimension - size.height) / 2, layoutDimension, size.height
    ].join(' '))

  viz.boundingBox = ->
    container.node().getBBox()

  clickHandler = neo.utils.clickHandler()
  clickHandler.on 'click', onNodeClick
  clickHandler.on 'dblclick', onNodeDblClick

  viz
