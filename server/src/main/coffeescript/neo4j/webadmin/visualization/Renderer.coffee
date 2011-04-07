###
Renderer, forked from the halfviz library.
###

define(
  ['order!lib/jquery'
   'order!lib/arbor'
   'order!lib/arbor-graphics'
   'order!lib/arbor-tween'
   'order!lib/backbone'], 
  () ->
    class Renderer

      constructor : (canvas, @nodeStyler, @relationshipStyler) ->
        @canvas = $(canvas).get(0)
        @ctx = @canvas.getContext("2d")
        @gfx = arbor.Graphics(@canvas)

        _.extend(this, Backbone.Events)
    
      init : (system) =>
        @particleSystem = system
        @particleSystem.screenSize(@canvas.width, @canvas.height) 
        @particleSystem.screenStep(0.000)
        @particleSystem.screenPadding(40)

        @initMouseHandling()

      redraw : () =>
        if not @particleSystem then return

        @gfx.clear()

        # This contains the edge boxes, is populated by @renderNode and used by @renderEdge
        @nodeBoxes = {}
        @particleSystem.eachNode @renderNode
        @particleSystem.eachEdge @renderEdge
        
        # No need to save this data        
        @nodeBoxes = {}
        
      renderNode : (node, pt) =>
        # node: {mass:#, p:{x,y}, name:"", data:{}}
        # pt:   {x:#, y:#}  node position in screen coords

        style = @nodeStyler.getStyleFor(node)
        label = style.labelText

        # determine the box size and round off the coords if we'll be 
        # drawing a text label (awful alignment jitter otherwise...)
        w = @ctx.measureText(""+label).width + 10
        if not (""+label).match(/^[ \t]*$/)
          pt.x = Math.floor(pt.x)
          pt.y = Math.floor(pt.y)
        else
          label = null
        

        if style.nodeStyle.shape == 'dot' 
          @gfx.oval(pt.x-w/2, pt.y-w/2, w,w, style.nodeStyle)
          @nodeBoxes[node.name] = [pt.x-w/2, pt.y-w/2, w,w]
        else
          @gfx.rect(pt.x-w/2, pt.y-10, w,20, 4, style.nodeStyle)
          @nodeBoxes[node.name] = [pt.x-w/2, pt.y-11, w, 22]

        # draw the text
        if label 
          @ctx.font = style.labelStyle.font
          @ctx.textAlign = "center"
          @ctx.fillStyle = style.labelStyle.color

          @ctx.fillText(label||"", pt.x, pt.y+4)

      renderEdge : (edge, pt1, pt2) =>
        # edge: {source:Node, target:Node, length:#, data:{}}
        # pt1:  {x:#, y:#}  source position in screen coords
        # pt2:  {x:#, y:#}  target position in screen coords

        if @stopped is true
          return

        style = @relationshipStyler.getStyleFor(edge)

        # find the start point
        tail = @intersect_line_box(pt1, pt2, @nodeBoxes[edge.source.name])
        if tail is false
          return
        head = @intersect_line_box(tail, pt2, @nodeBoxes[edge.target.name])
        if head is false
          return
  
        @ctx.save() 
        
        @ctx.beginPath()
        @ctx.lineWidth = style.edgeStyle.width
        @ctx.strokeStyle = style.edgeStyle.color
        @ctx.fillStyle = "rgba(0, 0, 0, 0)"

        @ctx.moveTo(tail.x, tail.y)
        @ctx.lineTo(head.x, head.y)
        @ctx.stroke()
        
        @ctx.restore()

        # draw an arrowhead if this is a -> style edge
        if edge.data.directed
          @ctx.save()
          # move to the head position of the edge we just drew
          wt = style.edgeStyle.width
          arrowLength = 6 + wt
          arrowWidth = 2 + wt
          @ctx.fillStyle = style.edgeStyle.color
          @ctx.translate(head.x, head.y)
          @ctx.rotate(Math.atan2(head.y - tail.y, head.x - tail.x))

          # delete some of the edge that's already there (so the point isn't hidden)
          @ctx.clearRect(-arrowLength / 2,-wt / 2, arrowLength/2,wt)

          # draw the chevron
          @ctx.beginPath()
          @ctx.moveTo(-arrowLength, arrowWidth)
          @ctx.lineTo(0, 0)
          @ctx.lineTo(-arrowLength, -arrowWidth)
          @ctx.lineTo(-arrowLength * 0.8, -0)
          @ctx.closePath()
          @ctx.fill()
          @ctx.restore()

        # draw the text
        if style.labelText
          @ctx.save()
          @ctx.font = style.labelStyle.font

          @ctx.translate(head.x, head.y)

          dx = head.x - tail.x
          if dx < 0
            @ctx.textAlign = "left"
            @ctx.rotate(Math.atan2(head.y - tail.y, dx) - Math.PI)
            @ctx.translate(20, style.edgeStyle.width - 5)
          else
            @ctx.textAlign = "right"
            @ctx.rotate(Math.atan2(head.y - tail.y, dx))
            @ctx.translate(-20, style.edgeStyle.width - 5)

          @ctx.fillStyle = style.labelStyle.color
          @ctx.fillText(style.labelText||"", 0, 0)
          @ctx.restore()

      initMouseHandling : () =>
        # no-nonsense drag and drop (thanks springy.js)
        @selected = null
        @nearest = null
        @dragged = null

        $(@canvas).mousedown(@clicked)

      start : ->
        @stopped = false

      stop : ->
        @stopped = true
        
      clicked: (e) =>
        pos = $(@canvas).offset()
        @dragStart = x:e.pageX, y:e.pageY

        p = arbor.Point(e.pageX-pos.left, e.pageY-pos.top)
        @selected = @nearest = @dragged = @particleSystem.nearest(p)

        if @dragged.node? 
          @dragged.node.fixed = true
          #@particleSystem.stop()

        $(@canvas).bind('mousemove', @nodeDragged)
        $(window).bind('mouseup', @nodeDropped)

        return false

      nodeDragged : (e) =>
        old_nearest = @nearest && @nearest.node._id
        pos = $(@canvas).offset()
        s = arbor.Point(e.pageX-pos.left, e.pageY-pos.top)

        @ghostify(@dragged.node)

        if not @nearest then return

        if @dragged != null and @dragged.node != null
          p = @particleSystem.fromScreen(s)
          @dragged.node.p = p

        return false

      nodeDropped : (e) =>
        if @dragged is null or @dragged.node is undefined then return
        if @dragged.node != null then @dragged.node.fixed = @dragged.node.data.fixated
        @dragged.node.fixed = true
        @dragged.node.mass = 1

        if @dragged.node != null and @thesePointsAreReallyClose(@dragStart, {x:e.pageX, y:e.pageY})
          @trigger("node:click", @dragged.node)

        @particleSystem.start() # ABK: hack start up the springy-spring machine

        @dragged = null
        @selected = null
        $(@canvas).unbind('mousemove', @nodeDragged)
        $(window).unbind('mouseup', @nodeDropped)
        return false

      
      ghostify : (node) =>
        #node.mass = 10000.001
        node.fixed = true

      thesePointsAreReallyClose : (p1, p2) =>
        Math.abs(p1.x - p2.x) < 5 and Math.abs(p1.y - p2.y) < 5 

      intersect_line_line : (p1, p2, p3, p4) =>
        denom = ((p4.y - p3.y)*(p2.x - p1.x) - (p4.x - p3.x)*(p2.y - p1.y))
        if denom is 0 then return false # Lines are parallel
        ua = ((p4.x - p3.x)*(p1.y - p3.y) - (p4.y - p3.y)*(p1.x - p3.x)) / denom
        ub = ((p2.x - p1.x)*(p1.y - p3.y) - (p2.y - p1.y)*(p1.x - p3.x)) / denom

        if ua < 0 or ua > 1 or ub < 0 or ub > 1
          return false
        else      
          return arbor.Point(p1.x + ua * (p2.x - p1.x), p1.y + ua * (p2.y - p1.y))

      intersect_line_box : (p1, p2, boxTuple) =>
        p3 = {x:boxTuple[0], y:boxTuple[1]}
        w = boxTuple[2]
        h = boxTuple[3]

        tl = {x: p3.x, y: p3.y}
        tr = {x: p3.x + w, y: p3.y}
        bl = {x: p3.x, y: p3.y + h}
        br = {x: p3.x + w, y: p3.y + h}

        return @intersect_line_line(p1, p2, tl, tr) or @intersect_line_line(p1, p2, tr, br) or @intersect_line_line(p1, p2, br, bl) or @intersect_line_line(p1, p2, bl, tl) or false
)
