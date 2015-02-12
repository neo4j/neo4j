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

'use strict';

angular.module('neo4jApp.services')
.service 'SVGUtils', [
  ->
    @prepareForExport = ($element, dimensions) ->
      svg = d3.select(document.createElementNS('http://www.w3.org/2000/svg', 'svg'))
      svg.append('title').text('Neo4j Graph Visualization')
      svg.append('desc').text('Created using Neo4j (http://www.neo4j.com/)')
      d3.select($element.get(0)).selectAll('g.layer').each(->
        svg.node().appendChild($(this).clone().get(0))
      )
      svg.selectAll('.overlay, .ring').remove()
      svg.selectAll('text').attr('font-family', 'sans-serif')
      svg.attr('width', dimensions.width)
      svg.attr('height', dimensions.height)
      svg.attr('viewBox', dimensions.viewBox)
      svg

    @
]
