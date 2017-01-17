###!
Copyright (c) 2002-2017 "Neo Technology,"
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
  .controller 'DatabaseDrawerCtrl', [
    '$scope'
    '$rootScope'
    ($scope, $rootScope) ->
      seedList = {}
      $scope.limitStep = 50
      $scope.labels = null
      $scope.relationships = null
      $scope.propertyKeys = null

      $scope.showMore = (type) -> load(type, $scope[type].showing + $scope.limitStep)
      $scope.showAll = (type) -> load(type, $scope[type].total)

      setSeedList = (type, list) -> 
        seedList[type] = [].concat(list).sort()

      setup = (type) ->
        $scope.$watch(
          -> $rootScope[type]
          , 
          -> 
            numToShow = $scope.limitStep
            if $scope[type] and $scope[type].showing >= $scope.limitStep 
              numToShow = $scope[type].showing
            setSeedList type, $rootScope[type]
            load(type, numToShow)
          ,
          yes
        )

      load = (type, num = $scope.limitStep) ->
        list = seedList[type]?.slice(0, num)
        showing = list?.length || 0
        total = seedList[type]?.length
        $scope[type] =
          list: list
          showing: showing
          total: total
          nextStepSize: (if total - showing < $scope.limitStep then total - showing else $scope.limitStep) 

      setup('labels')
      setup('relationships')
      setup('propertyKeys')
  ]
