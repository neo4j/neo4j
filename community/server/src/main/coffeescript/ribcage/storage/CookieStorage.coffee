###
Copyright (c) 2002-2018 "Neo Technology,"
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

define [], ->

  class CookieStorage
    ###
    A cookie manager based on Peter-Paul Kochs cookie code.
    ###
    get : (key) ->
      nameEQ = "#{key}="
      cookieStrings = document.cookie.split(';')
      for cookieStr in cookieStrings
        while cookieStr.charAt(0) is ' '
          cookieStr = cookieStr.substring(1,cookieStr.length)
        if cookieStr.indexOf(nameEQ) is 0
          return cookieStr.substring(nameEQ.length,cookieStr.length)

      return null

    set : (key, value, days=365) ->
      if days
        date = new Date()
        date.setTime(date.getTime()+(days*24*60*60*1000))
        expires = "; expires="+date.toGMTString()
      else 
        expires = ""
      document.cookie = "#{key}=#{value}#{expires}; path=/"

    remove : (key) ->
      @set key, "", -1
