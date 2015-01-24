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