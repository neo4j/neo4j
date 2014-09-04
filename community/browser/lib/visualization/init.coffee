do ->
  noop = ->

  nodeOutline = new neo.Renderer(
    onGraphChange: (selection, viz) ->
      circles = selection.selectAll('circle.outline').data(
        (node) -> [node]
      )

      circles.enter()
      .append('circle')
      .classed('outline', true)
      .attr
        cx: 0
        cy: 0

      circles
      .attr
        r: (node) -> node.radius
        fill: (node) -> viz.style.forNode(node).get('color')
        stroke: (node) -> viz.style.forNode(node).get('border-color')
        'stroke-width': (node) -> viz.style.forNode(node).get('border-width')

      circles.exit().remove()
    onTick: noop
  )

  nodeCaption = new neo.Renderer(
    onGraphChange: (selection, viz) ->
      text = selection.selectAll('text').data((node) -> node.caption)

      text.enter().append('text')
      .attr('text-anchor': 'middle')

      text
      .text((line) -> line.text)
      .attr('y', (line) -> line.baseline)
      .attr('font-size', (line) -> viz.style.forNode(line.node).get('font-size'))
      .attr('fill': (line) -> viz.style.forNode(line.node).get('text-color-internal'))

      text.exit().remove()

    onTick: noop
  )

  nodeOverlay = new neo.Renderer(
    onGraphChange: (selection) ->
      circles = selection.selectAll('circle.overlay').data((node) ->
        if node.selected then [node] else []
      )
      circles.enter()
      .insert('circle', '.outline')
      .classed('ring', true)
      .classed('overlay', true)
      .attr
        cx: 0
        cy: 0
        fill: '#f5F6F6'
        stroke: 'rgba(151, 151, 151, 0.2)'
        'stroke-width': '3px'

      circles
      .attr
        r: (node) -> node.radius + 6

      circles.exit().remove()
    onTick: noop
  )

  arrowPath = new neo.Renderer(
    onGraphChange: (selection, viz) ->
      paths = selection.selectAll('path.outline').data((rel) -> [rel])

      paths.enter()
      .append('path')
      .classed('outline', true)

      paths
      .attr('fill', (rel) -> viz.style.forRelationship(rel).get('color'))
      .attr('stroke', 'none')

      paths.exit().remove()

    onTick: (selection) ->
      selection.selectAll('path')
      .attr('d', (d) -> d.arrow.outline(d.shortCaptionLength))
  )

  relationshipType = new neo.Renderer(
    onGraphChange: (selection, viz) ->
      texts = selection.selectAll("text").data((rel) -> [rel])

      texts.enter().append("text")
      .attr("text-anchor": "middle")

      texts
      .attr('font-size', (rel) -> viz.style.forRelationship(rel).get('font-size'))
      .attr('fill', (rel) -> viz.style.forRelationship(rel).get('text-color-' + rel.captionLayout))

      texts.exit().remove()

    onTick: (selection, viz) ->
      selection.selectAll('text')
      .attr('x', (rel) -> rel.arrow.midShaftPoint.x)
      .attr('y', (rel) -> rel.arrow.midShaftPoint.y + parseFloat(viz.style.forRelationship(rel).get('font-size')) / 2 - 1)
      .attr('transform', (rel) ->
          if rel.naturalAngle < 90 or rel.naturalAngle > 270
            "rotate(180 #{ rel.arrow.midShaftPoint.x } #{ rel.arrow.midShaftPoint.y })"
          else
            null)
      .text((rel) -> rel.shortCaption)
  )

  relationshipOverlay = new neo.Renderer(
    onGraphChange: (selection) ->
      rects = selection.selectAll('path.overlay').data((rel) -> [rel])

      rects.enter()
        .append('path')
        .classed('overlay', true)
        .attr('fill', 'yellow')

      rects
        .attr('opacity', (rel) -> if rel.selected then 0.3 else 0)

      rects.exit().remove()

    onTick: (selection) ->
      band = 20

      selection.selectAll('path.overlay')
        .attr('d', (d) -> d.arrow.overlay(band))
  )

  neo.renderers.node.push(nodeOutline)
  neo.renderers.node.push(nodeCaption)
  neo.renderers.node.push(nodeOverlay)
  neo.renderers.relationship.push(arrowPath)
  neo.renderers.relationship.push(relationshipType)
  neo.renderers.relationship.push(relationshipOverlay)
