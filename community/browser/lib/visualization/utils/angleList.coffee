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