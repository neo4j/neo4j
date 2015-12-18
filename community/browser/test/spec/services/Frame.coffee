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

describe 'Service: Frame', () ->
  [backend, view] = [null, null]

  # load the service's module
  beforeEach module 'neo4jApp.services'

  # instantiate service
  Frame = {}
  Settings = {}
  $rootScope = null

  beforeEach ->
    module (FrameProvider) ->
      FrameProvider.interpreters.push
        type: 'string'
        templateUrl: 'dummy.html'
        matches: 'help'
        exec: -> ->
      FrameProvider.interpreters.push
        type: 'array'
        matches: ['command1', 'command2']
        exec: -> ->

      FrameProvider.interpreters.push
        type: 'complex'
        matches: (input) ->
          input.match('complex command')
        exec: -> ->

      return

    inject (_Frame_, _Settings_, _$rootScope_) ->
      Frame = _Frame_
      Settings = _Settings_
      $rootScope = _$rootScope_

  describe "interpreterFor", ->
    it 'should not return anything when no match', ->
      expect(Frame.interpreterFor('unmatched')).toBeFalsy()

    it 'should handle string matcher', ->
      expect(Frame.interpreterFor('help').type).toBe 'string'

    it 'should handle array matcher', ->
      expect(Frame.interpreterFor('command1').type).toBe 'array'
      expect(Frame.interpreterFor('command2').type).toBe 'array'

    it 'should handle function matcher', ->
      expect(Frame.interpreterFor('complex command').type).toBe 'complex'

  describe "create", ->
    it "should invoke the exec function when when match", ->
      intr = Frame.interpreterFor('help')
      spyOn(intr, 'exec').and.callThrough()
      Frame.create(input: 'help').exec()
      expect(intr.exec).toHaveBeenCalled()

    it 'should return a new frame when templateUrl is given', ->
      frame = Frame.create(input: 'help')
      expect(frame).toBeTruthy()

    it 'should not return a frame is there is not templateUrl for interpreter', ->
      frame = Frame.create(input: 'command1')
      expect(frame).toBeUndefined()

    it 'should not create more frames than specified in Settings', ->
      for i in [0..Settings.maxFrames]
        Frame.create(input: 'help')

      $rootScope.$apply()
      firstFrame = Frame.first()

      Frame.create(input: 'help')

      $rootScope.$apply()
      expect(Frame.length).toBe Settings.maxFrames
      expect(Frame.get(firstFrame)).toBeFalsy()
