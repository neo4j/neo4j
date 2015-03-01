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
    'Collection'
    '$timeout'
    ($scope, graphStyle, Collection, $timeout) ->
      $scope.sizes = graphStyle.defaultSizes()
      $scope.arrowWidths = graphStyle.defaultArrayWidths()
      $scope.colors = graphStyle.defaultColors()
      $scope.currentItem = null
      $scope.inspectorContracted = yes
      $scope.inspectorChanged = no
      $scope.inspectorFixed = no

      inspectorItem = (item, type) ->
        data: item
        type: type
        tmpl: "inspector/#{type}.html"

      triggerInspectorUIUpdate = () ->
        $timeout(->
          $scope.inspectorChanged = no
        , 0)

        $timeout(->
          $scope.inspectorChanged = yes
        , 0)

      $scope.onNodeDragToggle = (node) ->
        $scope.inspectorFixed = !!node

      $scope.onItemClick = (item, type) ->
        if item
          $scope.currentItem = inspectorItem(item, type)
          $scope.Inspector.reset($scope.currentItem)
        else
          $scope.currentItem = null
          $scope.Inspector.reset()
        triggerInspectorUIUpdate()

      $scope.onItemHover = (item, type) ->
        return if $scope.inspectorFixed

        if item
          $scope.Inspector.reset(inspectorItem(item, type))
        else
          $scope.Inspector.reset($scope.currentItem)
        triggerInspectorUIUpdate()

      $scope.styleForItem = (item) ->
        style = graphStyle.forEntity(item)
        {
          'background-color': style.props.color
          'color': style.props['text-color-internal']
        }

      $scope.styleForLabel = (label) ->
        item =
          labels: [label]
          isNode: true
        $scope.styleForItem(item)

      $scope.sizeLessThan = (a, b) ->
        a = if a then a.replace('px', '') else 0
        b = if b then b.replace('px', '') else 0
        +a <= +b

      $scope.Inspector = new Collection()

      $scope.close = -> Inspector.visible = no

      $scope.toggleInspector = ->
        Inspector.visible = !Inspector.visible

      $scope.selectArrowWidth = (item, size) ->
        item.style = graphStyle.changeForSelector(item.style.selector, size)

      $scope.selectCaption = (item, caption) ->
        item.style = graphStyle.changeForSelector(item.style.selector, { caption: caption})

      $scope.isSelectedCaption = (item, caption) ->
        grassProps = item.style.props
        grassProps.caption is "#{caption}" or (!grassProps.caption and caption in ["<id>", "<type>"])

      $scope.selectScheme = (item, scheme) ->
        item.style = graphStyle.changeForSelector(item.style.selector, angular.copy(scheme))

      $scope.selectSize = (item, size) ->
        item.style = graphStyle.changeForSelector(item.style.selector, size)

      arrowDisplayWidths = ("#{5 + 3*i}px" for i in [0..10])
      $scope.arrowDisplayWidth = (idx) ->
        width: arrowDisplayWidths[idx]

      nodeDisplaySizes = ("#{12 + 2*i}px" for i in [0..10])
      $scope.nodeDisplaySize = (idx) ->
        width: nodeDisplaySizes[idx]
        height: nodeDisplaySizes[idx]

  ]
