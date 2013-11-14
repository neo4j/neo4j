'use strict'

describe 'Service: Frame', () ->
  [backend, view] = [null, null]

  # load the service's module
  beforeEach module 'neo4jApp.services'

  # instantiate service
  Frame = {}
  Settings = {}
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

    inject (_Frame_, _Settings_) ->
      Frame = _Frame_
      Settings = _Settings_

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
      spyOn(intr, 'exec').andCallThrough()
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

      firstFrame = Frame.first()

      Frame.create(input: 'help')

      expect(Frame.length).toBe Settings.maxFrames
      expect(Frame.get(firstFrame)).toBeFalsy()
