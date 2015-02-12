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

angular.module('neo4jApp')
  .directive('editInPlace', ['$parse', '$timeout', ($parse, $timeout) ->
    restrict: "A"
    scope:
      value: "=editInPlace"
      callback: "&onBlur"
    replace: true
    template: '<div ng-class=" {editing: editing} " class="edit-in-place"><form ng-submit="save()"><span ng-bind="value" ng-hide="editing"></span><input ng-show="editing" ng-model="value" class="edit-in-place-input"><div ng-click="edit($event)" ng-hide="editing" class="edit-in-place-trigger"></div></form></div>'
    link: (scope, element, attrs) ->
      scope.editing = false
      inputElement = element.find('input')

      scope.edit = (e) ->
        e.preventDefault()
        e.stopPropagation()
        scope.editing = true

        $timeout(->
          inputElement.focus()
        , 0, false)

      scope.save  = ->
        scope.editing = false
        scope.callback() if scope.callback

      inputElement.bind "blur", (e) ->
        scope.save()
        scope.$apply() unless scope.$$phase
  ])
