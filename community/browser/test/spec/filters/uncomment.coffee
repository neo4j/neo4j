describe 'Filter: uncomment', () ->

  # load the filter's module
  beforeEach module 'neo4jApp.filters'

  # initialize a new instance of the filter before each test
  uncomment = {}
  beforeEach inject ($filter) ->
    uncomment = $filter 'uncomment'

  it 'should remove comments', ->
    text = """
// System info
:GET /db/manage/server/jmx/domain/org.neo4j"""

    expect(uncomment text).toBe ':GET /db/manage/server/jmx/domain/org.neo4j'
