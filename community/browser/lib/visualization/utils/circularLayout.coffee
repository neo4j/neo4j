neo.utils.circularLayout = (nodes, center, radius) ->
  unlocatedNodes = []
  unlocatedNodes.push(node) for node in nodes when !(node.x? and node.y?)
  for n, i in unlocatedNodes
    n.x = center.x + radius * Math.sin(2 * Math.PI * i / unlocatedNodes.length)
    n.y = center.y + radius * Math.cos(2 * Math.PI * i / unlocatedNodes.length)
