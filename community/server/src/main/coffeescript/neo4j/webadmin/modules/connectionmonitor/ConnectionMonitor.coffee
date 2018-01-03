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
  ['./connection_lost'
   'lib/amd/jQuery'], 
  (template, $) ->
    
    CONNECTION_LOST_EVENT = "web.connection_lost"

    class ConnectionMonitor

      visible : =>
        @connectionLostSplash.is(":visible")

      init : (appState) ->
        @db = appState.getServer()
        @db.bind CONNECTION_LOST_EVENT, @connectionLost
        
        # Empty heartbeat listener,
        # makes sure we continiously
        # check if the server is alive.
        @db.heartbeat.addListener ->

        # Make sure any images in the connection 
        # lost splash have been pre-loaded, so we
        # have them in case the server dies.
        splash = $(template())
        @connectionLostSplash = splash
        $("body").append(splash)
        setTimeout((-> splash.hide()),0)

      connectionLost : =>

        if not @visible()
          # @visible = true
          # connectionLostSplash = $(template())
          # $("body").append(connectionLostSplash)
          @connectionLostSplash.fadeIn(200)
          
          hideSplash = =>
            # @visible = false
            # connectionLostSplash.fadeOut 200, -> connectionLostSplash.remove()
            @connectionLostSplash.fadeOut 200

          @db.heartbeat.waitForPulse hideSplash
)
