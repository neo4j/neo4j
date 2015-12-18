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

neo.collision = do ->
  collision = {}

  collide = (node) ->
    r = node.radius + 10
    nx1 = node.x - r
    nx2 = node.x + r
    ny1 = node.y - r
    ny2 = node.y + r
    return (quad, x1, y1, x2, y2) ->
      if (quad.point && (quad.point != node))
        x = node.x - quad.point.x
        y = node.y - quad.point.y
        l = Math.sqrt(x * x + y * y)
        r = node.radius + 10 + quad.point.radius
      if (l < r)
        l = (l - r) / l * .5
        node.x -= x *= l
        node.y -= y *= l
        quad.point.x += x
        quad.point.y += y
      x1 > nx2 || x2 < nx1 || y1 > ny2 || y2 < ny1

  collision.avoidOverlap = (nodes) ->
    q = d3.geom.quadtree(nodes)
    for n in nodes
      q.visit collide(n)

  collision