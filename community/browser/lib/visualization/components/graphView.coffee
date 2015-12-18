###!
Copyright (c) 2002-2015 "Neo Technology,"
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

'use strict'

class neo.graphView
  constructor: (element, measureSize, @graph, @style) ->
    layout = neo.layout.force()
    @viz = neo.viz(element, measureSize, @graph, layout, @style)
    @callbacks = {}
    callbacks = @callbacks
    @viz.trigger = do ->
      (event, args...) ->
        callback.apply(null, args) for callback in (callbacks[event] or [])

  on: (event, callback) ->
    (@callbacks[event] ?= []).push(callback)
    @

  layout: (value) ->
    return layout unless arguments.length
    layout = value
    @

  grass: (value) ->
    return @style.toSheet() unless arguments.length
    @style.importGrass(value)
    @

  update: ->
    @viz.update()
    @

  resize: ->
    @viz.resize()
    @

  boundingBox: ->
    @viz.boundingBox()

  collectStats: ->
    @viz.collectStats()
