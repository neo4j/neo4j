'use strict'

describe 'Directive: neoTable', () ->
  beforeEach module 'neo4jApp.directives'

  it 'should escape HTML characters', inject ($rootScope, $compile) ->
    scope = $rootScope.$new()
    element = angular.element '<neo-table table-data="val"></neo-table>'
    element = $compile(element)(scope)
    scope.val =
      rows: -> [['<script>']]
      displayedSize: 1
      columns: -> ['col']
    console.log element.html()
    scope.$apply()
    expect(element.html()).toContain('&lt;script&gt;')
