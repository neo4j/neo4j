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

'use strict';

angular.module('neo4jApp.utils', [])
  .service('Utils', ['$timeout', ($timeout)->
    @argv = (input) ->
      rv = input.toLowerCase().split(' ')
      rv or []
    @debounce = (func, wait, immediate) ->
      result = undefined
      timeout = null
      ->
        context = @
        args = arguments
        later = ->
          timeout = null
          result = func.apply(context, args) unless immediate

        callNow = immediate and not timeout
        $timeout.cancel(timeout)
        timeout = $timeout(later, wait)
        result = func.apply(context, args) if callNow
        result

    @parseId = (resource = "") ->
      id = resource.substr(resource.lastIndexOf("/")+1)
      return parseInt(id, 10)

    @stripComments = (input) ->
      rows = input.split("\n")
      rv = []
      rv.push row for row in rows when row.indexOf('//') isnt 0
      rv.join("\n")

    @firstWord = (input) ->
      input.split(/\n| /)[0]

    @extendDeep = (dst) =>
      that = @
      angular.forEach(arguments, (obj) ->
        if (obj != dst)
          angular.forEach(obj, (value, key)  ->
            if (dst[key] && angular.isObject(dst[key]))
              that.extendDeep(dst[key], value)
            else if(!angular.isFunction(dst[key]))
              dst[key] = value
          )
      )
      dst

    @parseTimeMillis = ( timeWithOrWithoutUnit ) =>
      # Parses human-readable units like "12h", "2s" and returns milliseconds.
      # This maps to TimeUtil#parseTimeMillis in the main Neo4j code base, please ensure they are kept in sync
      unit = timeWithOrWithoutUnit.match /\D+/
      value = parseInt timeWithOrWithoutUnit

      if unit?.length is 1
        switch unit[0]
          when "ms" then return value
          when "s"  then return value * 1000
          when "m"  then return value * 1000 * 60
          else return 0
      else return value


    @ua2text = (ua) ->
      s = ''
      for i in [0..ua.length]
        s = s + "" + String.fromCharCode ua[i]
      s

    @
  ])
