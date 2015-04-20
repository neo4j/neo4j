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
  .directive('neoGraph', [
    'exportService'
    'SVGUtils'
    (exportService, SVGUtils)->
      dir =
        require: 'ngController'
        restrict: 'A'
      dir.link = (scope, elm, attr, ngCtrl) ->
        unbinds = []
        watchGraphData = scope.$watch attr.graphData, (graph) ->
          return unless graph
          ngCtrl.render(graph)

          listenerExportSVG = scope.$on('export.graph.svg', ->
            svg = SVGUtils.prepareForExport elm, dir.getDimensions(ngCtrl.getGraphView())
            exportService.download('graph.svg', 'image/svg+xml', new XMLSerializer().serializeToString(svg.node()))
            svg.remove()
          )
          listenerExportPNG = scope.$on('export.graph.png', ->
            svg = SVGUtils.prepareForExport elm, dir.getDimensions(ngCtrl.getGraphView())
            exportService.downloadPNGFromSVG(svg, 'graph')
            svg.remove()
          )
          listenerResetFrame = scope.$on('reset.frame.views', ->
            for unbind in unbinds
              unbind()
            unbinds = []
            $(elm[0]).empty()
            dir.link(scope, elm, attr, ngCtrl)
          )
          unbinds.push listenerExportSVG
          unbinds.push listenerExportPNG
          unbinds.push listenerResetFrame
          watchGraphData()

      dir.getDimensions = (view) ->
        boundingBox = view.boundingBox()
        dimensions =
          width: boundingBox.width
          height: boundingBox.height
          viewBox: [boundingBox.x, boundingBox.y, boundingBox.width, boundingBox.height].join(' ')
        dimensions

      dir
  ])
