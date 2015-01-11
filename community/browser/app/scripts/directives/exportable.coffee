###!
Copyright (c) 2002-2014 "Neo Technology,"
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
    ($window) ->
      download: (filename, mime, data) ->
        if !!navigator.userAgent.match(/Version\/[\d\.]+.*Safari/)
          # Safari doesn't support window.saveAs(); just open in a new window instead
          $window.open("data:#{mime};base64," + btoa(unescape(encodeURIComponent(data))))
          return true
        blob = new Blob([data], {type: mime})
        $window.saveAs(blob, filename)
  ])
  .directive('exportable', [->
    restrict: 'A'
    controller: [
      '$scope',
      'CSV',
      'exportService'
      ($scope, CSV, exportService) ->

        $scope.exportSVG = ->
          $scope.$emit('frame.export.svg')
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

        $scope.exportGraSS = (data) ->
          exportService.download('graphstyle.grass', 'text/plain', data)

        $scope.exportScript = (data) ->
          exportService.download('script.cypher', 'text/plain', data)

    ]
  ])
