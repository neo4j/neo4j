class neo.utils.circumferentialRelationshipRouting
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
    for relationship in graph.relationships()
      dx = relationship.target.x - relationship.source.x
      dy = relationship.target.y - relationship.source.y
      relationship.naturalAngle = ((Math.atan2(dy, dx) / Math.PI * 180) + 180) % 360
      delete relationship.arrow

    sortedNodes = graph.nodes().sort((a, b) ->
      b.relationshipCount(graph) - a.relationshipCount(graph))

    for node in sortedNodes
      relationships = []
      relationships.push(relationship) for relationship in graph.relationships() when relationship.source is node or relationship.target is node

      arrowAngles = { floating: {}, fixed: {} }
      relationshipMap = {}
      for relationship in relationships
        relationshipMap[relationship.id] = relationship

        if node == relationship.source
          if relationship.hasOwnProperty('arrow')
            arrowAngles.fixed[relationship.id] = relationship.naturalAngle + relationship.arrow.deflection
          else
            arrowAngles.floating[relationship.id] = relationship.naturalAngle
        if node == relationship.target
          if relationship.hasOwnProperty('arrow')
            arrowAngles.fixed[relationship.id] = (relationship.naturalAngle - relationship.arrow.deflection + 180) % 360
          else
            arrowAngles.floating[relationship.id] = (relationship.naturalAngle + 180) % 360

      distributedAngles = {}
      for id, angle of arrowAngles.floating
        distributedAngles[id] = angle
      for id, angle of arrowAngles.fixed
        distributedAngles[id] = angle

      if (relationships.length > 1)
        distributedAngles = neo.utils.distributeCircular(arrowAngles, 30)

      for id, angle of distributedAngles
        relationship = relationshipMap[id]
        if not relationship.hasOwnProperty('arrow')
          deflection = if node == relationship.source
            angle - relationship.naturalAngle
          else
            (relationship.naturalAngle - angle + 180) % 360

          shaftRadius = (parseFloat(@style.forRelationship(relationship).get('shaft-width')) / 2) or 2
          headRadius = shaftRadius + 3
          headHeight = headRadius * 2

          dx = relationship.target.x - relationship.source.x
          dy = relationship.target.y - relationship.source.y

          square = (distance) -> distance * distance
          centreDistance = Math.sqrt(square(dx) + square(dy))

          if Math.abs(deflection) < Math.PI / 180
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
