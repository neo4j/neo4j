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

neo.queryPlan = (element)->

  maxChildOperators = 2 # Fact we know about the cypher compiler
  maxComparableRows = 1000000 # link widths are comparable between plans if all operators are below this row count
  maxComparableDbHits = 1000000 # db hits are comparable between plans if all operators are below this db hit count

  operatorWidth = 180
  operatorCornerRadius = 4
  operatorHeaderHeight = 18
  operatorHeaderFontSize = 11
  operatorDetailHeight = 14
  maxCostHeight = 50
  detailFontSize = 10
  operatorMargin = 50
  operatorPadding = 3
  rankMargin = 50
  margin = 10
  standardFont = "'Helvetica Neue',Helvetica,Arial,sans-serif"
  fixedWidthFont = "Monaco,'Courier New',Terminal,monospace"
  linkColor = '#DFE1E3'
  costColor = '#F25A29'
  dividerColor = '#DFE1E3'
  operatorColors = colorbrewer.Blues[9].slice(2)

  operatorCategories =
    result: ['result']
    seek: ['scan', 'seek', 'argument']
    rows: ['limit', 'top', 'skip', 'sort', 'union', 'projection']
    other: []
    filter: ['select', 'filter', 'apply', 'distinct']
    expand: ['expand', 'product', 'join', 'optional', 'path']
    eager: ['eager']

  augment = (color) ->
    {
      color: color,
      'border-color': d3.rgb(color).darker(),
      'text-color-internal': if d3.hsl(color).l < 0.7 then '#FFFFFF' else '#000000'
    }

  colors =
    d3.scale.ordinal()
    .domain(d3.keys(operatorCategories))
    .range(operatorColors);

  color = (d) ->
    for name, keywords of operatorCategories
      for keyword in keywords
        if new RegExp(keyword, 'i').test(d)
          return augment(colors(name))
    augment(colors('other'))

  rows = (operator) ->
    operator.Rows ? operator.EstimatedRows ? 0

  plural = (noun, count) ->
    if count is 1 then noun else noun + 's'

  formatNumber = d3.format(",.0f")

  operatorDetails = (operator) ->
    return [] unless operator.expanded

    details = []

    wordWrap = (string, className) ->
      measure = (text) ->
        neo.utils.measureText(text, fixedWidthFont, 10)

      words = string.split(/([^a-zA-Z\d])/)

      firstWord = 0
      lastWord = 1
      while firstWord < words.length
        while lastWord < words.length and measure(words.slice(firstWord, lastWord + 1).join('')) < operatorWidth - operatorPadding * 2
          lastWord++
        details.push { className: className, value: words.slice(firstWord, lastWord).join('') }
        firstWord = lastWord
        lastWord = firstWord + 1

    if identifiers = operator.identifiers ? operator.KeyNames?.split(', ')
      wordWrap(identifiers.filter((d) -> not (/^  /.test(d))).join(', '), 'identifiers')
      details.push { className: 'padding' }

    if expression = operator.LegacyExpression ? operator.ExpandExpression ? operator.LabelName
      wordWrap(expression, 'expression')
      details.push { className: 'padding' }

    if operator.Rows? and operator.EstimatedRows?
      details.push { className: 'estimated-rows', key: 'estimated rows', value: formatNumber(operator.EstimatedRows)}
    if operator.DbHits? and not operator.alwaysShowCost
      details.push { className: 'db-hits', key: plural('db hit', operator.DbHits || 0), value: formatNumber(operator.DbHits || 0)}

    if details.length and details[details.length - 1].className == 'padding'
      details.pop()

    y = operatorDetailHeight
    for detail in details
      detail.y = y
      y += if detail.className == 'padding'
        operatorPadding * 2
      else
        operatorDetailHeight

    details

  transform = (queryPlan) ->
    operators = []
    links = []

    result =
      operatorType: 'Result'
      children: [queryPlan.root]

    collectLinks = (operator, rank) ->
      operators.push operator
      operator.rank = rank
      for child in operator.children
        child.parent = operator
        collectLinks child, rank + 1
        links.push
          source: child
          target: operator

    collectLinks result, 0

    [operators, links]

  layout = (operators, links) ->
    costHeight = do ->
      scale = d3.scale.log()
      .domain([1, Math.max(d3.max(operators, (operator) -> operator.DbHits or 0), maxComparableDbHits)])
      .range([0, maxCostHeight])
      (operator) ->
        scale((operator.DbHits ? 0) + 1)

    operatorHeight = (operator) ->
      height = operatorHeaderHeight
      if operator.expanded
        height += operatorDetails(operator).slice(-1)[0].y + operatorPadding * 2
      height += costHeight(operator)
      height

    linkWidth = do ->
      scale = d3.scale.log()
      .domain([1, Math.max(d3.max(operators, (operator) -> rows(operator) + 1), maxComparableRows)])
      .range([2, (operatorWidth - operatorCornerRadius * 2) / maxChildOperators])
      (operator) ->
        scale(rows(operator) + 1)

    for operator in operators
      operator.height = operatorHeight(operator)
      operator.costHeight = costHeight(operator)
      if operator.costHeight > operatorDetailHeight + operatorPadding
        operator.alwaysShowCost = true
      childrenWidth = d3.sum(operator.children, linkWidth)
      tx = (operatorWidth - childrenWidth) / 2
      for child in operator.children
        child.tx = tx
        tx += linkWidth(child)

    for link in links
      link.width = linkWidth(link.source)

    ranks = d3.nest()
    .key((operator) -> operator.rank)
    .entries(operators)

    currentY = 0

    for rank in ranks
      currentY -= (d3.max(rank.values, operatorHeight) + rankMargin)
      for operator in rank.values
        operator.x = 0
        operator.y = currentY

    width = d3.max(ranks.map((rank) -> rank.values.length * (operatorWidth + operatorMargin)))
    height = -currentY

    collide = ->
      for rank in ranks
        x0 = 0
        for operator in rank.values
          dx = x0 - operator.x
          if dx > 0
            operator.x += dx
          x0 = operator.x + operatorWidth + operatorMargin

        dx = x0 - operatorMargin - width
        if dx > 0
          lastOperator = rank.values[rank.values.length - 1]
          x0 = lastOperator.x -= dx
          for i in [rank.values.length - 2..0] by -1
            operator = rank.values[i]
            dx = operator.x + operatorWidth + operatorMargin - x0
            if dx > 0
              operator.x -= operatorWidth
              x0 = operator.x

    center = (operator) ->
      operator.x + operatorWidth / 2

    relaxUpwards = (alpha) ->
      for rank in ranks
        for operator in rank.values
          if operator.children.length
            x = d3.sum(operator.children, (child) -> linkWidth(child) * center(child)) / d3.sum(operator.children, linkWidth)
            operator.x += (x - center(operator)) * alpha

    relaxDownwards = (alpha) ->
      for rank in ranks.slice().reverse()
        for operator in rank.values
          if operator.parent
            operator.x += (center(operator.parent) - center(operator)) * alpha

    collide()
    iterations = 300
    alpha = 1
    while iterations--
      relaxUpwards(alpha)
      collide()
      relaxDownwards(alpha)
      collide()
      alpha *= .98

    width = d3.max(operators, (o) -> o.x) - d3.min(operators, (o) -> o.x) + operatorWidth

    [width, height]

  render = (operators, links, width, height, redisplay) ->
    svg = d3.select(element)

    svg.transition()
    .attr('width', width + margin * 2)
    .attr('height', height + margin * 2)
    .attr('viewBox', [d3.min(operators, (o) -> o.x) - margin, -margin - height, width + margin * 2, height + margin * 2].join(' '))

    join = (parent, children) ->
      for child in d3.entries(children)
        selection = parent.selectAll(child.key).data(child.value.data)
        child.value.selections(selection.enter(), selection, selection.exit())
        if child.value.children
          join(selection, child.value.children)

    join(svg, {
      'g.layer.links':
        data: [links]
        selections: (enter) ->
          enter.append('g')
          .attr('class', 'layer links')
        children:

          '.link':
            data: ((d) -> d),
            selections: (enter) ->
              enter.append('g')
              .attr('class', 'link')
            children:

              'path':
                data: (d) -> [d]
                selections: (enter, update) ->
                  enter
                  .append('path')
                  .attr('fill', linkColor)

                  update
                  .transition()
                  .attr('d', (d) ->
                    width = Math.max(1, d.width)
                    sourceX = d.source.x + operatorWidth / 2
                    targetX = d.target.x + d.source.tx

                    sourceY = d.source.y + d.source.height
                    targetY = d.target.y
                    yi = d3.interpolateNumber(sourceY, targetY)

                    curvature = .5
                    control1 = yi(curvature)
                    control2 = yi(1 - curvature)
                    controlWidth = Math.min(width / Math.PI, (targetY - sourceY) / Math.PI)
                    if sourceX > targetX + width / 2
                      controlWidth *= -1

                    [
                      'M', (sourceX + width / 2), sourceY,
                      'C', (sourceX + width / 2), control1 - controlWidth,
                      (targetX + width), control2 - controlWidth,
                      (targetX + width), targetY,
                      'L', targetX, targetY,
                      'C', targetX, control2 + controlWidth,
                      (sourceX - width / 2), control1 + controlWidth,
                      (sourceX - width / 2), sourceY,
                      'Z'
                    ].join(' '))

              'text':
                data: (d) ->
                  x = d.source.x + operatorWidth / 2
                  y = d.source.y + d.source.height + operatorDetailHeight
                  source = d.source
                  if source.Rows? or source.EstimatedRows?
                    [key, caption] = if source.Rows?
                      ['Rows', 'row']
                    else
                      ['EstimatedRows', 'estimated row']
                    [
                      { x: x, y: y, text: formatNumber(source[key]) + '\u00A0', anchor: 'end' }
                      { x: x, y: y, text: plural(caption, source[key]), anchor: 'start' }
                    ]
                  else
                    []
                selections: (enter, update) ->
                  enter
                  .append('text')
                  .attr('font-size', detailFontSize)
                  .attr('font-family', standardFont)

                  update
                  .transition()
                  .attr('x', (d) -> d.x)
                  .attr('y', (d) -> d.y)
                  .attr('text-anchor', (d) -> d.anchor)
                  .text((d) -> d.text)

      'g.layer.operators':
        data: [operators]
        selections: (enter) ->
          enter.append('g')
          .attr('class', 'layer operators')
        children:

          '.operator':
            data: ((d) -> d)
            selections: (enter, update) ->
              enter
              .append('g')
              .attr('class', 'operator')

              update
              .transition()
              .attr('transform', (d) -> "translate(#{d.x},#{d.y})")
            children:

              'rect.background':
                data: (d) -> [d]
                selections: (enter, update) ->
                  enter
                  .append('rect')
                  .attr('class', 'background')

                  update
                  .transition()
                  .attr('width', operatorWidth)
                  .attr('height', (d) -> d.height)
                  .attr('rx', operatorCornerRadius)
                  .attr('ry', operatorCornerRadius)
                  .attr('fill', 'white')
                  .style('stroke', 'none')

              'g.header':
                data: (d) -> [d]
                selections: (enter) ->
                  enter
                  .append('g')
                  .attr('class', 'header')
                  .attr('pointer-events', 'all')
                  .on('click', (d) ->
                    d.expanded = !d.expanded
                    redisplay()
                  )
                children:

                  'path.banner':
                    data: (d) -> [d]
                    selections: (enter, update) ->
                      enter
                      .append('path')
                      .attr('class', 'banner')

                      update
                      .attr('d', (d) ->
                        shaving =
                          if d.height <= operatorHeaderHeight
                            operatorCornerRadius
                          else if d.height < operatorHeaderHeight + operatorCornerRadius
                            operatorCornerRadius - Math.sqrt(Math.pow(operatorCornerRadius, 2) -
                                Math.pow(operatorCornerRadius - d.height + operatorHeaderHeight, 2))
                          else 0
                        [
                          'M', operatorWidth - operatorCornerRadius, 0
                          'A', operatorCornerRadius, operatorCornerRadius, 0, 0, 1, operatorWidth, operatorCornerRadius
                          'L', operatorWidth, operatorHeaderHeight - operatorCornerRadius
                          'A', operatorCornerRadius, operatorCornerRadius, 0, 0, 1, operatorWidth - shaving, operatorHeaderHeight
                          'L', shaving, operatorHeaderHeight
                          'A', operatorCornerRadius, operatorCornerRadius, 0, 0, 1, 0, operatorHeaderHeight - operatorCornerRadius
                          'L', 0, operatorCornerRadius
                          'A', operatorCornerRadius, operatorCornerRadius, 0, 0, 1, operatorCornerRadius, 0
                          'Z'
                        ].join(' '))
                      .style('fill', (d) -> color(d.operatorType).color)

                  'path.expand':
                    data: (d) -> if d.operatorType is 'Result' then [] else [d]
                    selections: (enter, update) ->
                      rotateForExpand = (d) ->
                        d3.transform()
                        "translate(#{operatorHeaderHeight / 2}, #{operatorHeaderHeight / 2}) " +
                        "rotate(#{if d.expanded then 90 else 0}) " +
                        "scale(0.5)"

                      enter
                      .append('path')
                      .attr('class', 'expand')
                      .attr('fill', (d) -> color(d.operatorType)['text-color-internal'])
                      .attr('d', 'M -5 -10 L 8.66 0 L -5 10 Z')
                      .attr('transform', rotateForExpand)

                      update
                      .transition()
                      .attrTween('transform', (d, i, a) ->
                        d3.interpolateString(a, rotateForExpand(d))
                      )

                  'text.title':
                    data: (d) -> [d]
                    selections: (enter) ->
                      enter
                      .append('text')
                      .attr('class', 'title')
                      .attr('font-size', operatorHeaderFontSize)
                      .attr('font-family', standardFont)
                      .attr('x', operatorHeaderHeight)
                      .attr('y', 13)
                      .attr('fill', (d) -> color(d.operatorType)['text-color-internal'])
                      .text((d) -> d.operatorType)

              'g.detail':
                data: operatorDetails
                selections: (enter, update, exit) ->
                  enter
                  .append('g')

                  update
                  .attr('class', (d) -> 'detail ' + d.className)
                  .attr('transform', (d) -> "translate(0, #{operatorHeaderHeight + d.y})")
                  .attr('font-family', (d) ->
                    if d.className is 'expression' or d.className is 'identifiers'
                      fixedWidthFont
                    else
                      standardFont)

                  exit.remove()
                children:

                  'text':
                    data: (d) ->
                      if d.key
                        [
                          { text: d.value + '\u00A0', anchor: 'end', x: operatorWidth / 2 }
                          { text: d.key, anchor: 'start', x: operatorWidth / 2 }
                        ]
                      else
                        [
                          { text: d.value, anchor: 'start', x: operatorPadding }
                        ]
                    selections: (enter, update, exit) ->
                      enter
                      .append('text')
                      .attr('font-size', detailFontSize)

                      update
                      .attr('x', (d) -> d.x)
                      .attr('text-anchor', (d) -> d.anchor)
                      .attr('fill', 'black')
                      .transition()
                      .each('end', ->
                        update
                        .text((d) -> d.text)
                      )

                      exit.remove()

                  'path.divider':
                    data: (d) ->
                      if (d.className == 'padding')
                        [d]
                      else
                        []
                    selections: (enter, update) ->
                      enter
                      .append('path')
                      .attr('class', 'divider')
                      .attr('visibility', 'hidden')

                      update
                      .attr('d', [
                            'M', 0, -operatorPadding * 2
                            'L', operatorWidth, -operatorPadding * 2
                          ].join(' '))
                      .attr('stroke', dividerColor)
                      .transition()
                      .each('end', ->
                        update
                        .attr('visibility', 'visible')
                      )

              'path.cost':
                data: (d) -> [d]
                selections: (enter, update) ->
                  enter
                  .append('path')
                  .attr('class', 'cost')
                  .attr('fill', costColor)

                  update
                  .transition()
                  .attr('d', (d) ->
                    if d.costHeight < operatorCornerRadius
                      shaving = operatorCornerRadius -
                          Math.sqrt(Math.pow(operatorCornerRadius, 2) - Math.pow(operatorCornerRadius - d.costHeight, 2))
                      [
                        'M', operatorWidth - shaving, d.height - d.costHeight
                        'A', operatorCornerRadius, operatorCornerRadius, 0, 0, 1, operatorWidth - operatorCornerRadius, d.height
                        'L', operatorCornerRadius, d.height
                        'A', operatorCornerRadius, operatorCornerRadius, 0, 0, 1, shaving, d.height - d.costHeight
                        'Z'
                      ].join(' ')
                    else
                      [
                        'M', 0, d.height - d.costHeight
                        'L', operatorWidth, d.height - d.costHeight
                        'L', operatorWidth, d.height - operatorCornerRadius
                        'A', operatorCornerRadius, operatorCornerRadius, 0, 0, 1, operatorWidth - operatorCornerRadius, d.height
                        'L', operatorCornerRadius, d.height
                        'A', operatorCornerRadius, operatorCornerRadius, 0, 0, 1, 0, d.height - operatorCornerRadius
                        'Z'
                      ].join(' ')

                  )

              'text.cost':
                data: (d) ->
                  if d.alwaysShowCost
                    y = d.height - d.costHeight + operatorDetailHeight
                    [
                      { text: formatNumber(d.DbHits) + '\u00A0', anchor: 'end', y: y }
                      { text: 'db hits', anchor: 'start', y: y }
                    ]
                  else
                    []
                selections: (enter, update) ->
                  enter
                  .append('text')
                  .attr('class', 'cost')
                  .attr('font-size', detailFontSize)
                  .attr('font-family', standardFont)
                  .attr('fill', 'white')

                  update
                  .attr('x', operatorWidth / 2)
                  .attr('text-anchor', (d) -> d.anchor)
                  .transition()
                  .attr('y', (d) -> d.y)
                  .each('end', ->
                    update
                    .text((d) -> d.text)
                  )

              'rect.outline':
                data: (d) -> [d]
                selections: (enter, update) ->
                  enter
                  .append('rect')
                  .attr('class', 'outline')

                  update
                  .transition()
                  .attr('width', operatorWidth)
                  .attr('height', (d) -> d.height)
                  .attr('rx', operatorCornerRadius)
                  .attr('ry', operatorCornerRadius)
                  .attr('fill', 'none')
                  .attr('stroke-width', 1)
                  .style('stroke', (d) -> color(d.operatorType)['border-color'])
    })

  display = (queryPlan) ->

    [operators, links] = transform(queryPlan)
    [width, height] = layout(operators, links)
    render(operators, links, width, height, -> display(queryPlan))
  @display = display
  @
