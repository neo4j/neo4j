###
Copyright (c) 2002-2011 "Neo Technology,"
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

define(
  ['order!lib/jquery'
   'order!lib/arbor'
   'order!lib/arbor-graphics'
   'order!lib/arbor-tween'
   'order!lib/backbone'],
  () ->
    class Renderer

      ###
      Based on the Halfviz renderer from Arbor.js
      ###

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
        if not @particleSystem or @stopped is true
          return

        @gfx.clear()

        # This contains the edge boxes, is populated by @renderNode and used by @renderEdge
        @nodeBoxes = {}
        @particleSystem.eachNode @renderNode
        @particleSystem.eachEdge @renderEdge

        ## # No need to save this data
        ## @nodeBoxes = {}

      renderNode : (node, pt) =>
        # node: {mass:#, p:{x,y}, name:"", data:{}}
        # pt:   {x:#, y:#}  node position in screen coords

        if node.data.hidden is true
          return

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

        ns = style.nodeStyle
        if @hovered and node._id == @hovered._id
          ns = {} # copy the style to not affect other nodes with same style
          for key of style.nodeStyle
            ns[key] = style.nodeStyle[key]
          ns.stroke = {r:0xff, g:0, b:0, a:node.data.alpha}

        if style.nodeStyle.shape == 'dot'
          @gfx.oval(pt.x-w/2, pt.y-w/2, w,w, ns)
          @nodeBoxes[node.name] = [pt.x-w/2, pt.y-w/2, w,w]
        else
          @gfx.rect(pt.x-w/2, pt.y-10, w,20, 4, ns)
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

        if edge.data.hidden is true
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
        @hovered = null

        $(@canvas).mousedown(@clicked)

      start : =>
        @stopped = false

      stop : =>
        @stopped = true

      clicked: (e) =>
        pos = $(@canvas).offset()
        @dragStart = x:e.pageX, y:e.pageY

        p = arbor.Point(e.pageX-pos.left, e.pageY-pos.top)
        @selected = @nearest = @dragged = @particleSystem.nearest(p)

        if @dragged.node?
          @dragged.node.fixed = true
          #@particleSystem.stop()
          @particleSystem.eachNode (node, pt) ->
            node.data.flow = node.fixed
            node.fixed = true

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

          intersecting = @intersectingNode(s)
          if intersecting and intersecting != @hovered
            intersecting.data.alpha=0
            @particleSystem.tweenNode(intersecting, 0.25, {alpha:1})
          @hovered = intersecting

        return false

      nodeDropped : (e) =>
        @hovered = null
        if @dragged is null or @dragged.node is undefined then return
        if @dragged.node != null then @dragged.node.fixed = @dragged.node.data.fixated
        @dragged.node.fixed = true
        @dragged.node.mass = 1

        if @dragged.node != null and @thesePointsAreReallyClose(@dragStart, {x:e.pageX, y:e.pageY})
          @trigger("node:click", @dragged.node, e)

        pos = $(@canvas).offset()
        p = arbor.Point(e.pageX-pos.left, e.pageY-pos.top)

        @particleSystem.eachNode (node, pt) ->
          node.fixed = node.data.flow

        nearest = @intersectingNode(p)
        if nearest
          @trigger("node:dropped", @dragged.node, nearest, e)

        @particleSystem.start()

        @dragged = null
        @selected = null
        $(@canvas).unbind('mousemove', @nodeDragged)
        $(window).unbind('mouseup', @nodeDropped)
        return false

      intersectingNode : (pos) =>
        nearest = {node:null, distance:null}
        dragged = @dragged.node
        @particleSystem.eachNode (node, pt) ->
          if node._id != dragged._id
            dist = pos.subtract(pt).magnitude()
            if nearest.distance is null or dist<nearest.distance
              nearest.node = node
              nearest.distance = dist
        if not (nearest.node is null)
          if @ptInBox(pos, @nodeBoxes[nearest.node.name])
            return nearest.node

      ptInBox : (pt, box) =>
        [x, y, w, h] = box; [w,h] = [w-2,h-2]
        delta = pt.subtract(arbor.Point(x,y))
        return Math.abs(delta.x) < w and Math.abs(delta.y) < h

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
