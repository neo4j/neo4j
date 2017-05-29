###!
Copyright (c) 2002-2017 "Neo Technology,"
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

if global? then global.neo = global.neo || {};
if window? then window.neo = window.neo || {};

neo = global?.neo || window?.neo

class neo.helpers
  constructor: ->
    @argv = (input) ->
      rv = input.toLowerCase().split(' ')
      rv or []

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
      for index, obj of arguments
        if (obj != dst)
          for key, value of obj
            if (dst[key] && typeof dst[key] is 'object' && Object.getOwnPropertyNames(dst[key]).length > 0)
              that.extendDeep(dst[key], value)
            else if(typeof dst[key] isnt 'function')
              dst[key] = value
      dst

    @extend = (objects) ->
      extended = {}
      merge = (obj) ->
        for index, prop of obj
          if Object.prototype.hasOwnProperty.call obj, index
            extended[index] = obj[index]
      merge arguments[0]
      for i in [1...arguments.length]
        obj = arguments[i]
        merge obj
      extended

    @throttle = (func, wait) ->
      last_timestamp = null
      limit = wait
      ->
        context = @
        args = arguments
        now = Date.now()
        if !last_timestamp || now - last_timestamp >= limit
          last_timestamp = now
          func.apply(context, args)

    @parseTimeMillis = ( timeWithOrWithoutUnit ) =>
      timeWithOrWithoutUnit += '' #Cast to string

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
      else return value*1000

    @ua2text = (ua) ->
      s = ''
      for i in [0..ua.length]
        s = s + "" + String.fromCharCode ua[i]
      s

    @escapeHTML = (string) ->
      entityMap =
        "&": "&amp;"
        "<": "&lt;"
        ">": "&gt;"
        '"': '&quot;'
        "'": '&#39;'
        "/": '&#x2F;'
      String(string).replace(/[&<>"'\/]/g, (s) -> entityMap[s])

    @cleanHTML = (string) ->
      @stripNGAttributes @stripScripts string

    @stripScripts = (string = '') ->
      string = string.replace(/(\s+(on[^\s=]+)[^\s=]*\s*=\s*("[^"]*"|'[^']*'|[\w\-.:]+\s*))/ig, '')
      string.replace(/<script\b[^<]*(?:(?!<\/script>)<[^<]*)*(<\/script>)?/gi, '')

    @stripNGAttributes = (string = '') ->
      string.replace(/(\s+(ng|data|x)[^\s=]*\s*=\s*("[^"]*"|'[^']*'|[\w\-.:]+\s*))/ig, '')

    @hostIsAllowed = (hostname, whitelist, is_enterprise) ->
      return true if is_enterprise and (not whitelist or whitelist is '*')
      whitelisted_hosts = if is_enterprise then whitelist.split(",") else ['http://guides.neo4j.com', 'https://guides.neo4j.com', 'http://localhost', 'https://localhost']
      hostname in whitelisted_hosts
