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

window.neo = window.neo || {}

neo.models = {}

neo.renderers =
  node: []
  relationship: []

neo.utils =
  # Note: quick n' dirty. Only works for serializable objects
  copy: (src) ->
    JSON.parse(JSON.stringify(src))

  extend: (dest, src) ->
    return if not neo.utils.isObject(dest) and neo.utils.isObject(src)
    dest[k] = v for own k, v of src
    return dest

  isArray: Array.isArray or (obj) ->
    Object::toString.call(obj) == '[object Array]';

  isObject: (obj) -> Object(obj) is obj
