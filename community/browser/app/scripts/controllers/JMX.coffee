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

'use strict'

angular.module('neo4jApp.controllers')
  .controller 'JMXCtrl', [
    '$scope'
    'Server'
    ($scope, Server) ->

      parseName = (str) ->
        str.split('=').pop()

      parseSection = (str) ->
        str.split('/')[0]

      Server.jmx(["*:*"]).success((response) ->
        sections = {}
        for r in response
          r.name = parseName(r.name)
          section = parseSection(r.url)
          sections[section] ?= {}
          sections[section][r.name] = r
        $scope.sections = sections
        $scope.currentItem = sections[section][r.name]
      )

      $scope.stringify = (val) ->
        if angular.isString(val) then val else JSON.stringify(val, null, ' ')

      $scope.selectItem = (section, name) ->
        $scope.currentItem = $scope.sections[section][name]

      # Filters
      $scope.simpleValues = (item) -> !$scope.objectValues(item)

      $scope.objectValues = (item) -> angular.isObject(item.value)
  ]
