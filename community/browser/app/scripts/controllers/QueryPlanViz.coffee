###!
Copyright (c) 2002-2014 "Neo Technology,"
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

angular.module('neo4jApp.controllers')
.controller('QueryPlanViz', [
    '$element'
    ($element) ->
      operatorHeight = 20
      operatorPadding = 70
      margin = 10

      @render = (queryPlan) ->
        operators = []
        links = []

        explore = (operator) ->
          operators.push operator
          for child in operator.children
            explore child
            links.push
              source: child
              target: operator
              value: Math.max(1, operator.Rows)

        explore queryPlan.root

        sankey = d3.sankey()
        .nodes(operators)
        .links(links);

        # do a minimal layout with arbitrary size, just to establish operator dimensions
        sankey
        .size([500, 500])
        .layout(1)

        # group operators into ranks
        ranks = {}
        for operator in operators
          rank = ranks[operator.x]
          unless rank
            rank = []
            ranks[operator.x] = rank
          rank.push operator

        width = d3.max(d3.values(ranks).map((rank) -> d3.sum(rank.map((operator) -> operator.dy + 70))))
        rankHeight = 50
        height = d3.values(ranks).length * rankHeight

        sankey
        .nodeWidth(operatorHeight)
        .nodePadding(operatorPadding)
        .size([height, width])
        .layout(32)

        svg = d3.select($element[0])
        .attr('viewBox', [-margin, -margin, width + margin * 2, height + margin * 2].join(' '))

        formatNumber = d3.format(",.0f")
        format = (d) ->
          formatNumber(d) + ' rows'
        color = d3.scale.category20()

        path = (d) ->
          dy = Math.max(1, d.dy)
          x0 = d.source.x + d.source.dx
          x1 = d.target.x
          xi = d3.interpolateNumber(x0, x1)
          curvature = .5
          x2 = xi(curvature)
          x3 = xi(1 - curvature)
          midSourceY = d.source.y + (d.source.dy / 2)

          [
            'M', (midSourceY + dy / 2), x0,
            'C', (midSourceY + dy / 2), x2,
            (d.target.y + dy + d.ty), x3,
            (d.target.y + dy + d.ty), x1,
            'L', (d.target.y + d.ty), x1,
            'C', (d.target.y + d.ty), x3,
            (midSourceY - dy / 2), x2,
            (midSourceY - dy / 2), x0,
            'Z'
          ].join(' ')

        linkElement = svg.append('g').selectAll('.link')
        .data(links)
        .enter().append('g')
        .attr('class', 'link')

        linkElement
        .append('path')
        .attr('d', path)

        linkElement
        .append('text')
        .attr('x', (d) ->
          d.source.y + d.source.dy / 2)
        .attr('y', (d) ->
          d.source.x + 40)
        .attr('text-anchor', 'middle')
        .text((d) ->
          format(d.value))

        operatorElement = svg.append('g').selectAll('.operator')
        .data(operators)
        .enter().append('g')
        .attr('class', 'operator')
        .attr('transform', (d) -> "translate(#{d.y},#{d.x})")

        operatorElement.append('rect')
        .attr('width', (d) -> Math.max(1, d.dy))
        .attr('height', sankey.nodeWidth())
        .style('fill', (d) -> d.color = color(d.operatorType))
        .style('stroke', (d) -> d3.rgb(d.color).darker(2))
        .append('title')
        .text((d) -> d.name + '\n' + format(d.value))

        textElement = operatorElement.append('text')
        .attr('y', 15)
        .attr('x', 0);

        textElement.append('tspan')
        .attr('class', 'operator-name')
        .text((d) -> d.operatorType)

        textElement.append('tspan')
        .attr('class', 'operator-identifiers')
        .attr('dx', 5)
        .text((d) -> d.IntroducedIdentifier)

      return @
  ])
