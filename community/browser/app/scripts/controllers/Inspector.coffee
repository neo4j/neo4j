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
  .controller 'InspectorCtrl', [
    '$scope',
    'GraphStyle'
    ($scope, GraphStyle) ->
      $scope.sizes = GraphStyle.defaultSizes()
      $scope.arrowWidths = GraphStyle.defaultArrayWidths()
      $scope.colors = GraphStyle.defaultColors()
      $scope.style =
        color: $scope.colors[0].color
        'border-color': $scope.colors[0]['border-color']
        diameter: $scope.sizes[0].diameter

      $scope.$watch 'selectedGraphItem', (item) ->
        return unless item
        $scope.item = item
        $scope.style = GraphStyle.forEntity(item).props
        if $scope.style.caption
          $scope.selectedCaption = $scope.style.caption.replace(/\{([^{}]*)\}/, "$1")

      $scope.selectSize = (size) ->
        $scope.style.diameter = size.diameter
        $scope.saveStyle()

      $scope.selectArrowWidth = (arrowWidth) ->
        $scope.style['shaft-width'] = arrowWidth['shaft-width']
        $scope.saveStyle()

      $scope.selectScheme = (color) ->
        $scope.style.color = color.color
        $scope.style['border-color'] = color['border-color']
        $scope.style['text-color-internal'] = color['text-color-internal']
        $scope.saveStyle()

      $scope.selectCaption  = (caption) ->
        $scope.selectedCaption = caption
        $scope.style.caption = '{' + caption + '}'
        $scope.saveStyle()

      $scope.saveStyle = ->
        GraphStyle.change($scope.item, $scope.style)

  ]
