neo.utils.clickHandler = ->
  cc = (selection) ->

    # euclidean distance
    dist = (a, b) ->
      Math.sqrt Math.pow(a[0] - b[0], 2), Math.pow(a[1] - b[1], 2)
    down = undefined
    tolerance = 5
    last = undefined
    wait = null
    selection.on "mousedown", ->
      d3.event.target.__data__.fixed = yes
      down = d3.mouse(document.body)
      last = +new Date()

    selection.on "mouseup", ->
      if dist(down, d3.mouse(document.body)) > tolerance
        return
      else
        if wait
          window.clearTimeout wait
          wait = null
          event.dblclick d3.event.target.__data__
        else
          wait = window.setTimeout(((e) ->
            ->
              event.click e.target.__data__
              wait = null
          )(d3.event), 250)

  event = d3.dispatch("click", "dblclick")
  d3.rebind cc, event, "on"