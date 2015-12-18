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

angular.module('neo4jApp.directives')
  .directive 'overflowWithToggle', [ '$window', '$timeout', ($window, $timeout) ->
    restrict: 'A'
    link: (scope, element, attrs) ->

      onResize = () ->
        growing = element.parent().find('ul').height()
        oneline = parseInt(element.css('height'))

        if oneline*1.1 >= growing
          element.hide()
        else
          element.show()

      $timeout(->
        onResize()
      , 0)
      scope.$watch(-> 
        element.parent().width()
      , (old, newv) ->
        onResize()
      )

      if 'updateUi' of attrs
        scope.$watch(attrs.updateUi
        , (new_val, old_val) ->
          if new_val
            onResize()
        )
  ]
