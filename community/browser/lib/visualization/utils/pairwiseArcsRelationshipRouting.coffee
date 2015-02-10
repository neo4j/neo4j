class neo.utils.pairwiseArcsRelationshipRouting
  constructor: (@style) ->

  measureRelationshipCaption: (relationship, caption) ->
    fontFamily = 'sans-serif'
    padding = parseFloat(@style.forRelationship(relationship).get('padding'))
    neo.utils.measureText(caption, fontFamily, relationship.captionHeight) + padding * 2

  captionFitsInsideArrowShaftWidth: (relationship) ->
    parseFloat(@style.forRelationship(relationship).get('shaft-width')) > relationship.captionHeight

  measureRelationshipCaptions: (relationships) ->
    for relationship in relationships
      relationship.captionHeight = parseFloat(@style.forRelationship(relationship).get('font-size'))
      relationship.captionLength = @measureRelationshipCaption(relationship, relationship.caption)
      relationship.captionLayout =
        if @captionFitsInsideArrowShaftWidth(relationship) and not relationship.isLoop()
          "internal"
        else
          "external"

  shortenCaption: (relationship, caption, targetWidth) ->
    shortCaption = caption || 'caption'
    while true
      if shortCaption.length <= 2
        return ['', 0]
      shortCaption = shortCaption.substr(0, shortCaption.length - 2) + '\u2026'
      width = @measureRelationshipCaption(relationship, shortCaption)
      if width < targetWidth
        return [shortCaption, width]

  computeGeometryForNonLoopArrows: (nodePairs) ->
    square = (distance) -> distance * distance
    for nodePair in nodePairs
      if not nodePair.isLoop()
        dx = nodePair.nodeA.x - nodePair.nodeB.x
        dy = nodePair.nodeA.y - nodePair.nodeB.y
        angle = Math.atan2(dy, dx) / Math.PI * 180
        centreDistance = Math.sqrt(square(dx) + square(dy))
        for relationship in nodePair.relationships
          relationship.naturalAngle = if relationship.target is nodePair.nodeA
            (angle + 180) % 360
          else
            angle
          relationship.centreDistance = centreDistance

  distributeAnglesForLoopArrows: (nodePairs, relationships) ->
    for nodePair in nodePairs
      if nodePair.isLoop()
        angles = []
        node = nodePair.nodeA
        for relationship in relationships
          if not relationship.isLoop()
            if relationship.source is node
              angles.push relationship.naturalAngle
            if relationship.target is node
              angles.push relationship.naturalAngle + 180
        angles = angles.map((a) -> (a + 360) % 360).sort((a, b) -> a - b)
        if angles.length > 0
          biggestGap =
            start: 0
            end: 0
          for angle, i in angles
            start = angle
            end = if i == angles.length - 1
              angles[0] + 360
            else
              angles[i + 1]
            if end - start > biggestGap.end - biggestGap.start
              biggestGap.start = start
              biggestGap.end = end
          separation = (biggestGap.end - biggestGap.start) / (nodePair.relationships.length + 1)
          for relationship, i in nodePair.relationships
            relationship.naturalAngle = (biggestGap.start + (i + 1) * separation - 90) % 360
        else
          separation = 360 / nodePair.relationships.length
          for relationship, i in nodePair.relationships
            relationship.naturalAngle = i * separation

  layoutRelationships: (graph) ->
    nodePairs = graph.groupedRelationships()
    @computeGeometryForNonLoopArrows(nodePairs)
    @distributeAnglesForLoopArrows(nodePairs, graph.relationships())

    for nodePair in nodePairs
      for relationship in nodePair.relationships
        delete relationship.arrow

      middleRelationshipIndex = (nodePair.relationships.length - 1) / 2

      for relationship, i in nodePair.relationships

        shaftWidth = parseFloat(@style.forRelationship(relationship).get('shaft-width')) or 2
        headWidth = shaftWidth + 6
        headHeight = headWidth

        if nodePair.isLoop()
          spread = 30
          relationship.arrow = new neo.utils.loopArrow(
            relationship.source.radius,
            40,
            spread,
            shaftWidth,
            headWidth,
            headHeight,
            relationship.captionHeight
          )
        else
          if i == middleRelationshipIndex
            relationship.arrow = new neo.utils.straightArrow(
                relationship.source.radius,
                relationship.target.radius,
                relationship.centreDistance,
                shaftWidth,
                headWidth,
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
                relationship.centreDistance,
                deflection,
                shaftWidth,
                headWidth,
                headHeight,
                relationship.captionLayout
            )

        [relationship.shortCaption, relationship.shortCaptionLength] = if relationship.arrow.shaftLength > relationship.captionLength
          [relationship.caption, relationship.captionLength]
        else
          @shortenCaption(relationship, relationship.caption, relationship.arrow.shaftLength)
