class neo.utils.angleList

  constructor: (@list) ->

  getAngle: (index) ->
    @list[index].angle

  fixed: (index) ->
    @list[index].fixed

  totalLength: ->
    @list.length

  length: (run) ->
    if run.start < run.end
      run.end - run.start
    else
      run.end + @list.length - run.start

  angle: (run) ->
    if run.start < run.end
      @list[run.end].angle - @list[run.start].angle
    else
      360 - (@list[run.start].angle - @list[run.end].angle)

  wrapIndex: (index) ->
    if index == -1
      @list.length - 1
    else if index >= @list.length
      index - @list.length
    else
      index