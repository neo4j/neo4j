###!
Copyright (c) 2002-2016 "Neo Technology,"
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
.directive('mousemove', [
    '$parse',
    'Utils'
    ($parse, Utils) ->
      restrict: 'A',
      link: (scope, elem, attr) ->
        throttle = Utils.throttle(
          (e) ->
            exp = $parse(attr.mousemove)
            scope.$apply(->exp(scope, {'$event': e}))
        ,
          5000
        )
        elem.bind('mousemove', (e)->
          throttle(e)
        )
  ])
