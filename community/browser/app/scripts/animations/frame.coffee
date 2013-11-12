###!
Copyright (c) 2002-2013 "Neo Technology,"
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
  .animation("frame-out", ["$window", ($window) ->
    setup: (element) ->
      element.css height: element.height()

    start: (element, done) ->
      element.animate
        opacity: 0
        height: 0
      ,
        duration: 400
        easing: "easeInOutCubic"
        complete: done

  ]).animation("frame-in", ["$window", ($window) ->
    setup: (element) ->
      element.css
        position: "absolute"
        top: -100
        opacity: 0

    start: (element, done) ->
      afterFirst = () ->
        element.css position: "relative"

        element.animate
          opacity: 1
          top: 0
          maxHeight: element.height()
        ,
          duration: 400
          easing: "easeInOutCubic"
          complete: ->
            # remove max-height to be able to resize the frame when interacting
            element.css maxHeight: 'initial'
            done()

      # render object to get a size
      element.animate
        opacity: 0.01
      , 200, ->
        afterFirst()
  ])

  # Animation for message bar below editor
  #
  .animation("intro-out", ["$window", ($window) ->
    start: (element, done) ->
      element.animate
        opacity: 0
        top: 40
      ,
        duration: 400
        easing: "easeInOutCubic"
        complete: done

  ]).animation("intro-in", ["$window", ($window) ->
    setup: (element) ->
      element.css
        opacity: 0
        top: 0
        display: 'block'

    start: (element, done) ->
      element.animate
        opacity: 1
        top: 0
      ,
        duration: 1600
        easing: "easeInOutCubic"
        complete: done
  ])

  # Animation for message bar below editor
  #
  .animation("slide-down-out", ["$window", ($window) ->
    start: (element, done) ->
      element.animate
        height: 0
      ,
        duration: 400
        easing: "easeInOutCubic"
        complete: done

  ]).animation("slide-down-in", ["$window", ($window) ->
    setup: (element) ->
      element.css
        height: 0
        display: 'block'

    start: (element, done) ->
      element.animate
        height: 49
      ,
        duration: 400
        easing: "easeInOutCubic"
        complete: done
  ])
