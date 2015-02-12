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
  .factory('fullscreenService', [->
    root = angular.element('body')
    container = angular.element('<div class="fullscreen-container"></div>')
    container.hide().appendTo(root)
    return {
      display: (element) ->
        container.append(element).show()
      hide: -> container.hide()
    }
  ])


angular.module('neo4jApp.directives')
.directive('fullscreen', ['fullscreenService',
  (fullscreenService) ->
    restrict: 'A'
    controller: ['$scope', ($scope) ->
      $scope.toggleFullscreen = (state = !$scope.fullscreen) ->
        $scope.fullscreen = state
    ]
    link: (scope, element, attrs) ->
      parent = element.parent()
      scope.fullscreen = no
      scope.$watch 'fullscreen', (val, oldVal) ->
        return if val is oldVal
        if val
          fullscreenService.display(element)
        else
          # if parent[0].innerHTML is ""
          parent.append(element)
          fullscreenService.hide()
        scope.$emit 'layout.changed'
])
