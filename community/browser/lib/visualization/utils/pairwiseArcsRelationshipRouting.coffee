class neo.utils.pairwiseArcsRelationshipRouting
  constructor: (@style) ->

  measureRelationshipCaption: (relationship, caption) ->
    fontFamily = 'sans-serif'
    fontSize = parseFloat(@style.forRelationship(relationship).get('font-size'))
    padding = parseFloat(@style.forRelationship(relationship).get('padding'))
    neo.utils.measureText(caption, fontFamily, fontSize) + padding * 2

  captionFitsInsideArrowShaftWidth: (relationship) ->
    parseFloat(@style.forRelationship(relationship).get('shaft-width')) >
    parseFloat(@style.forRelationship(relationship).get('font-size'))

  measureRelationshipCaptions: (relationships) ->
    for relationship in relationships
      relationship.captionLength = @measureRelationshipCaption(relationship, relationship.type)
      relationship.captionLayout =
        if @captionFitsInsideArrowShaftWidth(relationship)
          "internal"
        else
          "external"

  shortenCaption: (relationship, caption, targetWidth) ->
    shortCaption = caption
    while true
      if shortCaption.length <= 2
        return ['', 0]
      shortCaption = shortCaption.substr(0, shortCaption.length - 2) + '\u2026'
      width = @measureRelationshipCaption(relationship, shortCaption)
      if width < targetWidth
        return [shortCaption, width]

  layoutRelationships: (graph) ->
    groupedRelationships = {}

    class NodePair

      constructor: (node1, node2) ->
        @relationships = []
        if node1.id < node2.id
          @nodeA = node1
          @nodeB = node2
        else
          @nodeA = node2
          @nodeB = node1

      toString: ->
        "#{@nodeA.id}:#{@nodeB.id}"

    for relationship in graph.relationships()
      delete relationship.arrow
      nodePair = new NodePair(relationship.source, relationship.target)
      nodePair = groupedRelationships[nodePair] or nodePair
      nodePair.relationships.push relationship
      groupedRelationships[nodePair] = nodePair

    for ignored, nodePair of groupedRelationships
      middleRelationshipIndex = (nodePair.relationships.length - 1) / 2

      for relationship, i in nodePair.relationships

        shaftRadius = (parseFloat(@style.forRelationship(relationship).get('shaft-width')) / 2) or 2
        headRadius = shaftRadius + 3
        headHeight = headRadius * 2

        dx = relationship.target.x - relationship.source.x
        dy = relationship.target.y - relationship.source.y
        relationship.naturalAngle = ((Math.atan2(dy, dx) / Math.PI * 180) + 180) % 360

        square = (distance) -> distance * distance
        centreDistance = Math.sqrt(square(dx) + square(dy))

        if i == middleRelationshipIndex
          relationship.arrow = new neo.utils.straightArrow(
              relationship.source.radius,
              relationship.target.radius,
              centreDistance,
              shaftRadius,
              headRadius,
              headHeight,
              relationship.captionLayout
          )
        else
          deflection = 30 * (i - middleRelationshipIndex)
          if nodePair.nodeA isnt relationship.source
            deflection *= -1

          relationship.arrow = new neo.utils.arcArrow(
              relationship.source.radius,
              relationship.target.radius,
              centreDistance,
              deflection,
              shaftRadius * 2,
              headRadius * 2,
              headHeight,
              relationship.captionLayout
          )

        [relationship.shortCaption, relationship.arrow.shortCaptionLength] = if relationship.arrow.shaftLength > relationship.captionLength
          [relationship.type, relationship.captionLength]
        else
          @shortenCaption(relationship, relationship.type, relationship.arrow.shaftLength)