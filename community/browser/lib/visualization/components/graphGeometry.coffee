class NeoD3Geometry
  square = (distance) -> distance * distance

  constructor: (@style) ->
    @relationshipRouting = new neo.utils.pairwiseArcsRelationshipRouting(@style)

  addShortenedNextWord = (line, word, measure) ->
    until word.length <= 2
      word = word.substr(0, word.length - 2) + '\u2026'
      if measure(word) < line.remainingWidth
        line.text += " " + word
        break

  noEmptyLines = (lines) ->
    for line in lines
      if line.text.length is 0 then return false
    true

  fitCaptionIntoCircle = (node, style) ->
    template = style.forNode(node).get("caption")
    captionText = style.interpolate(template, node)
    fontFamily = 'sans-serif'
    fontSize = parseFloat(style.forNode(node).get('font-size'))
    lineHeight = fontSize
    measure = (text) ->
      neo.utils.measureText(text, fontFamily, fontSize)

    words = captionText.split(" ")

    emptyLine = (lineCount, iLine) ->
      baseline = (1 + iLine - lineCount / 2) * lineHeight
      constainingHeight = if iLine < lineCount / 2 then baseline - lineHeight else baseline
      lineWidth = Math.sqrt(square(node.radius) - square(constainingHeight)) * 2
      {
      node: node
      text: ''
      baseline: baseline
      remainingWidth: lineWidth
      }

    fitOnFixedNumberOfLines = (lineCount) ->
      lines = []
      iWord = 0;
      for iLine in [0..lineCount - 1]
        line = emptyLine(lineCount, iLine)
        while iWord < words.length and measure(" " + words[iWord]) < line.remainingWidth
          line.text += " " + words[iWord]
          line.remainingWidth -= measure(" " + words[iWord])
          iWord++
        lines.push line
      if iWord < words.length
        addShortenedNextWord(lines[lineCount - 1], words[iWord], measure)
      [lines, iWord]

    consumedWords = 0
    maxLines = node.radius * 2 / fontSize

    lines = [emptyLine(1, 0)]
    for lineCount in [1..maxLines]
      [candidateLines, candidateWords] = fitOnFixedNumberOfLines(lineCount)
      if noEmptyLines(candidateLines)
        [lines, consumedWords] = [candidateLines, candidateWords]
      if consumedWords >= words.length
        return lines
    lines

  formatNodeCaptions: (nodes) ->
    for node in nodes
      node.caption = fitCaptionIntoCircle(node, @style)

  formatRelationshipCaptions: (relationships) ->
    for relationship in relationships
      template = @style.forRelationship(relationship).get("caption")
      relationship.caption = @style.interpolate(template, relationship)

  setNodeRadii: (nodes) ->
    for node in nodes
      node.radius = parseFloat(@style.forNode(node).get("diameter")) / 2

  onGraphChange: (graph) ->
    @setNodeRadii(graph.nodes())
    @formatNodeCaptions(graph.nodes())
    @formatRelationshipCaptions(graph.relationships())
    @relationshipRouting.measureRelationshipCaptions(graph.relationships())

  onTick: (graph) ->
    @relationshipRouting.layoutRelationships(graph)
