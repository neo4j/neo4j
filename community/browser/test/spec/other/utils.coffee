describe 'Utils: firstWord', () ->



  # load the service's module
  beforeEach module 'neo4jApp.utils'

  # instantiate service
  Utils = {}
  beforeEach ->
    inject (_Utils_) ->
      Utils = _Utils_

  it 'should get first word in a multiword line', ->
    text = "multiple words here on one line"
    expect(Utils.firstWord text).toBe 'multiple'

  it 'should get first word in a multiline string', ->
    text = """
          cypher queries
          will often be more
          legible on multiple lines
          than squashed onto a single line
          """
    expect(Utils.firstWord text).toBe 'cypher'

  it 'should get first word when it is alone in a multiline string', ->
    text = """
          alone
          on the first line but
          still extractable
          """
    expect(Utils.firstWord text).toBe 'alone'

  describe '#updateAverage', ->
    it 'creates new average with only one parameter', ->
      expect(Utils.updateAverage(100)).toBe 100

    it 'updates existing average', ->
      expect(Utils.updateAverage(100, 100, 2)).toBe(100)
