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

angular.module('neo.exportable', ['neo.csv'])
  .service('exportService', [
    '$window'
    'Canvg'
    'Utils'
    ($window, Canvg, Utils) ->
      download: (filename, mime, data) ->
        if !!navigator.userAgent.match(/Version\/[\d\.]+.*Safari/)
          # Safari doesn't support window.saveAs(); just open in a new window instead
          if typeof(data) == 'object'
            data = Utils.ua2text data
          else
            data = unescape(encodeURIComponent(data))
          $window.open("data:#{mime};base64," + btoa(data))
          return true
        blob = new Blob([data], {type: mime})
        $window.saveAs(blob, filename)

      downloadWithDataURI: (filename, dataURI) ->
        byteString = null
        if dataURI.split(',')[0].indexOf('base64') >= 0
            byteString = atob(dataURI.split(',')[1])
        else
            byteString = unescape(dataURI.split(',')[1])
        mimeString = dataURI.split(',')[0].split(':')[1].split(';')[0]

        ia = new Uint8Array(byteString.length)
        for i in [0..byteString.length]
            ia[i] = byteString.charCodeAt(i)
        @download filename, mimeString, ia

      downloadPNGFromSVG: (svgObj, filename) ->
        svgData = new XMLSerializer().serializeToString(svgObj.node())
        svgData = svgData.replace(/&nbsp;/g, "&#160;") #Workaround for non-breaking space support
        canvas = document.createElement("canvas")
        canvas.width = svgObj.attr('width')
        canvas.height = svgObj.attr('height')
        Canvg(canvas, svgData)
        png = canvas.toDataURL("image/png")
        @downloadWithDataURI("#{filename}.png", png)

  ])
  .directive('exportable', [->
    restrict: 'A'
    controller: [
      '$scope',
      'CSV',
      'exportService'
      ($scope, CSV, exportService) ->

        $scope.exportGraphSVG = ->
          $scope.$emit('frame.export.graph.svg')
          true

        $scope.exportPlanSVG = ->
          $scope.$emit('frame.export.plan.svg')
          true

        $scope.exportGraphPNG = ->
          $scope.$emit('frame.export.graph.png')
          true

        $scope.exportPlanPNG = ->
          $scope.$emit('frame.export.plan.png')
          true

        $scope.exportJSON = (data) ->
          return unless data
          exportService.download('result.json', 'application/json', JSON.stringify(data))

        $scope.exportCSV = (data) ->
          return unless data
          csv = new CSV.Serializer()
          csv.columns(data.columns())
          for row in data.rows()
            csv.append(row)

          exportService.download('export.csv', 'text/csv;charset=utf-8', csv.output())

        $scope.exportText = (data) ->
          return unless data
          exportService.download('result.txt', 'text/plain', data)

        $scope.exportGraSS = (data) ->
          exportService.download('graphstyle.grass', 'text/plain', data)

        $scope.exportScript = (data) ->
          exportService.download('script.cypher', 'text/plain', data)

    ]
  ])
