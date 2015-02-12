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

angular.module("neo4jApp.animations", [])

  # Animation for creating and removing result frames
  #
  .animation(".frame-in", ["$window", ($window) ->
    enter: (element, done) ->
      element.css
        position: "absolute"
        top: "-100px"
        opacity: 0

      afterFirst = ->
        element.css position: "relative"
        element.animate
          opacity: 1
          top: 0
          maxHeight: element.height()
        ,
          duration: 400
          easing: "easeInOutCubic"
          complete: ->
            element.css maxHeight: 10000 # "remove" max-height
            done()

      # render object to get a size
      element.animate
        opacity: 0.01
        # FIX: nested .animate() not triggered when using animate-enhanced
        # in combination with AngularJS, so timeout to execute separately
      , 200, -> setTimeout(afterFirst, 0)

      ->

    leave: (element, done) ->
      element.css height: element.height()
      element.animate
        opacity: 0
        height: 0
      ,
        duration: 400
        easing: "easeInOutCubic"
        complete: done

      ->
  ])

  # Animation for message bar below editor
  #
  .animation(".intro-in", ["$window", ($window) ->
    enter: (element, done) ->
      element.css
        opacity: 0
        top: 0
        display: 'block'

      element.animate
        opacity: 1
        top: 0
      ,
        duration: 1600
        easing: "easeInOutCubic"
        complete: done
    leave: (element, done) ->
      element.animate
        opacity: 0
        top: 40
      ,
        duration: 400
        easing: "easeInOutCubic"
        complete: done
  ])

  # Animation for message bar below editor
  #
  .animation(".slide-down", ["$window", ($window) ->
    enter: (element, done) ->
      element.css
        maxHeight: 0
        display: 'block'

      element.animate
        maxHeight: 49
      ,
        duration: 400
        easing: "easeInOutCubic"
        complete: done

      ->

    leave: (element, done) ->
      element.animate
        height: 0
      ,
        duration: 400
        easing: "easeInOutCubic"
        complete: done

      ->
  ])
