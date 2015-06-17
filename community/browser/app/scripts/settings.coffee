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

#baseURL = 'http://localhost:7474'
baseURL = ''
restAPI = "#{baseURL}/db/data"

angular.module('neo4jApp.settings', ['neo4jApp.utils'])
  .constant('Settings', {
    cmdchar: ':'
    endpoint:
      console: "#{baseURL}/db/manage/server/console"
      version: "#{baseURL}/db/manage/server/version"
      jmx: "#{baseURL}/db/manage/server/jmx/query"
      rest: restAPI
      cypher: "#{restAPI}/cypher"
      transaction: "#{restAPI}/transaction"
      authUser: "#{baseURL}/user"
    host: baseURL
    maxExecutionTime: 3600 # in seconds
    heartbeat: 60 # in seconds
    maxFrames: 50
    maxHistory: 100
    maxNeighbours: 100
    maxNodes: 1000
    maxRows: 1000
    filemode: false
    maxRawSize: 5000 # bytes
    scrollToTop: yes # When new frames are inserted in to the stream
    showVizDiagnostics: false
    acceptsReplies: false
    enableMotd: true
    initCmd: ":play start"
    refreshInterval: 10 # in seconds
    userName: "Graph Friend"
    storeCredentials: yes
  })

angular.module('neo4jApp.settings')
.service('SettingsStore', ['$rootScope', 'localStorageService','Settings', 'Utils'
  ($rootScope, localStorageService, Settings, Utils) ->
    originalSettings = angular.copy(Settings)
    load: ->
      settings = localStorageService.get('settings')
      if angular.isObject(settings)
        Utils.extendDeep(Settings, settings)

    reset: ->
      localStorageService.remove('settings')
      angular.extend(Settings, originalSettings)

    save: ->
      localStorageService.set('settings', angular.copy(Settings))
      $rootScope.$emit('settings:saved')

])

angular.module('neo4jApp.settings')
.run(['SettingsStore', (SettingsStore) ->
  SettingsStore.load()
])
