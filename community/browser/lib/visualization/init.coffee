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

do ->
  noop = ->

  nodeOutline = new neo.Renderer(
    onGraphChange: (selection, viz) ->
      circles = selection.selectAll('circle.outline').data((node) -> [node])

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
      .attr('pointer-events': 'none')

      text
      .text((line) -> line.text)
      .attr('y', (line) -> line.baseline)
      .attr('font-size', (line) -> viz.style.forNode(line.node).get('font-size'))
      .attr('fill': (line) -> viz.style.forNode(line.node).get('text-color-internal'))

      text.exit().remove()

    onTick: noop
  )

  nodeRing = new neo.Renderer(
    onGraphChange: (selection) ->
      circles = selection.selectAll('circle.ring').data((node) -> [node])
      circles.enter()
      .insert('circle', '.outline')
      .classed('ring', true)
      .attr
        cx: 0
        cy: 0
        'stroke-width': '8px'

      circles
      .attr
        r: (node) -> node.radius + 4

      circles.exit().remove()

    onTick: noop
  )

  arrowPath = new neo.Renderer(
    name: 'arrowPath'
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
    name: 'relationshipType'
    onGraphChange: (selection, viz) ->
      texts = selection.selectAll("text").data((rel) -> [rel])

      texts.enter().append("text")
      .attr("text-anchor": "middle")
      .attr('pointer-events': 'none')

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
    name: 'relationshipOverlay'
    onGraphChange: (selection) ->
      rects = selection.selectAll('path.overlay').data((rel) -> [rel])

      rects.enter()
        .append('path')
        .classed('overlay', true)

      rects.exit().remove()

    onTick: (selection) ->
      band = 16

      selection.selectAll('path.overlay')
        .attr('d', (d) -> d.arrow.overlay(band))
  )

  neo.renderers.node.push(nodeOutline)
  neo.renderers.node.push(nodeCaption)
  neo.renderers.node.push(nodeRing)
  neo.renderers.relationship.push(arrowPath)
  neo.renderers.relationship.push(relationshipType)
  neo.renderers.relationship.push(relationshipOverlay)
