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

define( 
  ['./splash'
   'ribcage/storage/CookieStorage'
   'lib/amd/jQuery'], 
  (template, CookieStorage, $) ->
    
    class Splash
      
      constructor : ->
        @cookies = new CookieStorage

      init : ->
        if not @hasBeenShownForThisSession()
          @show()

      hasBeenShownForThisSession : ->
        @cookies.get("splashShown1.6") != null

      show : ->
        @cookies.set("splashShown1.6", "1")
        # Show boot screen for flashiness
        splash = $(template())
        $("body").append(splash)
        
        hideSplash = ->
          splash.fadeOut(600)
        
        setTimeout hideSplash, 1500
)
