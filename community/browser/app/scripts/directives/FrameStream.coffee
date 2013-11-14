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

'use strict'


angular.module('neo4jApp.directives')
.directive('frameStream', ['Frame','Editor','motdService',
  (Frame, Editor, motdService) ->
    restrict: 'A'
    priority: 0
    templateUrl: 'views/partials/stream.html'
    replace: false
    transclude: false
    scope: false
    controller: ['$scope', 'Frame', 'Editor', 'motdService', ($scope, Frame, Editor, motdService) ->
      $scope.frames = Frame
      $scope.motd = motdService
      $scope.editor = Editor
    ]
    link: (scope, element, attrs) ->
      # scope.editor.execScript(":play intro")
      # scope.frames.create({"input":":welcome"}).fullscreen
])
