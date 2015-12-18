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
  .directive('neoPlan', [
    'exportService'
    'SVGUtils'
    (exportService, SVGUtils)->
      dir =
        restrict: 'A'
      dir.link = (scope, elm, attr) ->
        unbinds = []
        watchQueryPlan = scope.$watch attr.queryPlan, (originalPlan) ->
          return unless originalPlan
          plan = JSON.parse(JSON.stringify(originalPlan))
          display = () ->
            neo.queryPlan(elm.get(0)).display(plan)
          display()

          scope.toggleExpanded = (expanded) ->
            visit = (operator) ->
              operator.expanded = expanded
              if operator.children
                for child in operator.children
                  visit(child)
            visit plan.root
            display()

          listenerExportSVG = scope.$on('export.plan.svg', ->
            svg = SVGUtils.prepareForExport(elm, dir.getDimensions(elm.get(0)))
            svgData = new XMLSerializer().serializeToString(svg.node())
            svgData = svgData.replace(/&nbsp;/g, "&#160;") #Workaround for non-breaking space support
            exportService.download('plan.svg', 'image/svg+xml', svgData)
            svg.remove()
          )
          listenerExportPNG = scope.$on('export.plan.png', ->
            svg = SVGUtils.prepareForExport elm, dir.getDimensions(elm.get(0))
            exportService.downloadPNGFromSVG(svg, 'plan')
            svg.remove()
          )
          listenerResetFrame = scope.$on('reset.frame.views', ->
            for unbind in unbinds
              unbind()
            unbinds = []
            $(elm[0]).empty()
            dir.link(scope, elm, attr)
          )
          unbinds.push listenerExportSVG
          unbinds.push listenerExportPNG
          unbinds.push listenerResetFrame
          watchQueryPlan()

      dir.getDimensions = (element)->
        node = d3.select(element)
        dimensions =
          width: node.attr('width')
          height: node.attr('height')
          viewBox: node.attr('viewBox')
        dimensions
      dir
  ])
