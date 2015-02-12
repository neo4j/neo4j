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

describe 'Controller: StreamCtrl', () ->

  # load the controller's module
  beforeEach module 'neo4jApp.services', 'neo4jApp.controllers'

  Frame = {}
  Folder = {}
  StreamCtrl = {}
  scope = {}
  timer = {}


  # Initialize the controller and a mock scope
  beforeEach ->
    module (FrameProvider) ->
      FrameProvider.interpreters.push
        type: ':help'
        matches: ':help'
        templateUrl: 'dummy.html'
        exec: ->
          (input) -> return true

      return

    inject ($controller, $rootScope, _Folder_, _Frame_, $timeout) ->
      scope = $rootScope.$new()
      Folder = _Folder_
      Frame = _Frame_
      timer = $timeout
      # Reset storage
      Folder.save([])

      # Instantiate
      StreamCtrl = $controller 'StreamCtrl', { $scope: scope }
      scope.$digest()
