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

angular.module('neo4jApp')
.config([
  'FrameProvider'
  'Settings'
  (FrameProvider, Settings) ->

    cmdchar = Settings.cmdchar

    # convert a string into a topical keyword
    topicalize = (input) ->
      if input?
        input.toLowerCase().trim().replace /\s+/g, '-'
      else
        null

    argv = (input) ->
      rv = input?.toLowerCase().split(' ')
      rv or []

    error = (msg, exception = "Error", data) ->
      errors: [
        message: msg
        code: exception
        data: data
      ]

    FrameProvider.interpreters.push
      type: 'clear'
      matches: "#{cmdchar}clear"
      exec: ['Frame', (Frame) ->
        (input) ->
          Frame.reset()
          true
      ]

    FrameProvider.interpreters.push
      type: 'style'
      matches: "#{cmdchar}style"
      exec: [
        '$rootScope', 'exportService', 'GraphStyle',
        ($rootScope, exportService, GraphStyle) ->
          (input, q) ->
            switch argv(input)[1]
              when 'reset'
                GraphStyle.resetToDefault()
              when 'export'
                exportService.download('graphstyle.grass', 'text/plain;charset=utf-8', GraphStyle.toString())
              else
                $rootScope.togglePopup('styling')
            true
      ]

    # Show command history
    FrameProvider.interpreters.push
      type: 'history'
      matches: "#{cmdchar}history"
      templateUrl: 'views/frame-history.html'
      exec: [
        'HistoryService',
        (HistoryService) ->
          (input, q) ->
            q.resolve(angular.copy(HistoryService.history))
            q.promise
      ]

    # FrameProvider.interpreters.push
    #   type: 'keys'
    #   templateUrl: 'views/frame-keys.html'
    #   matches: "#{cmdchar}keys"
    #   exec: ['$rootScope', ($rootScope) ->
    #     (input) -> true
    #   ]

    # Generic shell commands
    FrameProvider.interpreters.push
      type: 'shell'
      templateUrl: 'views/frame-rest.html'
      matches: "#{cmdchar}schema"
      exec: ['Server', (Server) ->
        (input, q) ->
          Server.console(input.substr(1))
          .then(
            (r) ->
              response = r.data[0]
              if response.match('Unknown')
                q.reject(error("Unknown action", null, response))
              else
                q.resolve(response)
          )
          q.promise
      ]


    # play handler
    FrameProvider.interpreters.push
      type: 'play'
      templateUrl: 'views/frame-guide.html'
      matches: "#{cmdchar}play"
      exec: ['$http', ($http) ->
        step_number = 1
        (input, q) ->
          topic = topicalize(input[('play'.length+1)..]) or 'start'
          url = "content/guides/#{topic}.html"
          $http.get(url)
          .then(
            ->
              q.resolve(page: url)
          ,
            (r)->
              q.reject(r)
          )
          q.promise
      ]

    # Shorthand for ":play sysinfo"
    FrameProvider.interpreters.push
      type: 'play'
      matches: "#{cmdchar}sysinfo"
      exec: ['Frame', (Frame) ->
        (input, q) ->
          Frame.create {input: "#{Settings.cmdchar}play sysinfo"}
          return true
      ]

    # Help/man handler
    FrameProvider.interpreters.push
      type: 'help'
      templateUrl: 'views/frame-help.html'
      matches: ["#{cmdchar}help", "#{cmdchar}man"]
      exec: ['$http', ($http) ->
        (input, q) ->
          topic = topicalize(input[('help'.length+1)..]) or 'help'
          url = "content/help/#{topic}.html"
          $http.get(url)
          .then(
            ->
              q.resolve(page: url)
            ,
            (r)->
              q.reject(r)
          )
          q.promise
      ]

    FrameProvider.interpreters.push
      type: 'config'
      templateUrl: 'views/frame-config.html'
      matches: ["#{cmdchar}config"]
      exec: ['Settings', 'SettingsStore', (Settings, SettingsStore) ->
        (input, q) ->
          # special command for reset
          if argv(input)[1] is "reset"
            SettingsStore.reset()
            q.resolve(Settings)
            return q.promise

          matches = /^[^\w]*config\s+([^:]+):?([\S\s]+)?$/.exec(input)
          if (matches?)
            [key, value] = [matches[1], matches[2]]
            if (value?)
              value = try eval(value)

              Settings[key] = value
              # Persist new config
              SettingsStore.save()
            else
              value = Settings[key]

            property = {}
            property[key] = value
            q.resolve(property)
          else
            q.resolve(Settings)

          q.promise

      ]

    # about handler
    # FrameProvider.interpreters.push
    #   type: 'info'
    #   templateUrl: 'views/frame-info.html'
    #   matches: "#{cmdchar}about"
    #   exec: ->
    #     (input, q) ->
    #       page: "content/help/about.html"

    # sysinfo handler
    # FrameProvider.interpreters.push
    #   type: 'info'
    #   templateUrl: 'views/frame-info.html'
    #   matches: "#{cmdchar}sysinfo"
    #   exec: ->
    #     (input, q) ->
    #       page: "content/guides/sysinfo.html"

    # HTTP Handler
    FrameProvider.interpreters.push
      type: 'http'
      templateUrl: 'views/frame-rest.html'
      matches: ["#{cmdchar}get", "#{cmdchar}post", "#{cmdchar}delete", "#{cmdchar}put", "#{cmdchar}head"]
      exec: ['Server', (Server) ->
        (input, q) ->
          regex = /^[^\w]*(get|GET|put|PUT|post|POST|delete|DELETE|head|HEAD)\s+(\S+)\s*([\S\s]+)?$/i
          result = regex.exec(input)

          try
            [verb, url, data] = [result[1], result[2], result[3]]
          catch e
            q.reject(error("Unparseable http request", 'Request error'))
            return q.promise

          verb = verb?.toLowerCase()
          if not verb
            q.reject(error("Invalid verb, expected 'GET, PUT, POST, HEAD or DELETE'", 'Request error'))
            return q.promise

          if not url?.length > 0
            q.reject(error("Missing path", 'Request error'))
            return q.promise

          if (verb is 'post' or verb is 'put')
            if data
              # insist that data is parseable JSON
              try
                JSON.parse(data.replace(/\n/g, ""))
              catch e
                q.reject(error("Payload does not seem to be valid data.", 'Request payload error'))
                return q.promise

          Server[verb]?(url, data)
          .then(
            (r) ->
              q.resolve(r.data)
            ,
            (r) ->
              q.reject(error("Error: #{r.status} - #{r.statusText}", 'Request error'))
          )

          q.promise
      ]

    FrameProvider.interpreters.push
      type: 'auth'
      fullscreenable: false
      templateUrl: 'views/frame-connect.html'
      matches: (input) ->
        pattern = new RegExp("^#{cmdchar}server connect")
        input.match(pattern)
      exec: ['AuthService', (AuthService) ->
        (input, q) -> q.resolve()
      ]

    FrameProvider.interpreters.push
      type: 'auth'
      fullscreenable: false
      templateUrl: 'views/frame-disconnect.html'
      matches:  (input) ->
        pattern = new RegExp("^#{cmdchar}server disconnect")
        input.match(pattern)
      exec: ['Settings', 'AuthService', (Settings, AuthService) ->
        (input, q) ->
          q.resolve()
      ]

    FrameProvider.interpreters.push
      type: 'auth'
      fullscreenable: false
      templateUrl: 'views/frame-server-status.html'
      matches:  (input) ->
        pattern = new RegExp("^#{cmdchar}server status")
        input.match(pattern)
      exec: ['AuthService', 'ConnectionStatusService', (AuthService, ConnectionStatusService) ->
        (input, q) ->
          AuthService.hasValidAuthorization()
          .then(
            (r) ->
              q.resolve r
            ,
            (r) ->
              q.reject r
            )
          q.promise
      ]

    FrameProvider.interpreters.push
      type: 'auth'
      fullscreenable: false
      templateUrl: 'views/frame-change-password.html'
      matches:  (input) ->
        pattern = new RegExp("^#{cmdchar}server change-password")
        input.match(pattern)
      exec: ['AuthService', (AuthService) ->
        (input, q) ->
          q.resolve()
          q.promise
      ]


    # Profile a cypher command
    # FrameProvider.interpreters.push
    #   type: 'cypher'
    #   matches: "#{cmdchar}profile"
    #   templateUrl: 'views/frame-rest.html'
    #   exec: ['Cypher', (Cypher) ->
    #     (input, q) ->
    #       input = input.substr(8)
    #       if input.length is 0
    #         q.reject(error("missing query"))
    #       else
    #         Cypher.profile(input).then(q.resolve, q.reject)
    #       q.promise
    #   ]

    extractGraphModel = (response, CypherGraphModel) ->
      graph = new neo.models.Graph()
      graph.addNodes(response.nodes.map(CypherGraphModel.convertNode()))
      graph.addRelationships(response.relationships.map(CypherGraphModel.convertRelationship(graph)))
      graph

    # Cypher handler
    FrameProvider.interpreters.push
      type: 'cypher'
      matches: (input) ->
        pattern = new RegExp("^[^#{cmdchar}]")
        input.match(pattern)
      templateUrl: 'views/frame-cypher.html'
      exec: ['Cypher', 'CypherGraphModel', 'CypherParser', 'Timer', (Cypher, CypherGraphModel, CypherParser, Timer) ->
        # Return the function that handles the input
        (input, q) ->
          current_transaction = Cypher.transaction()
          commit_fn = () ->
            timer = Timer.start()
            current_transaction.commit(input).then(
              (response) ->
                if response.size > Settings.maxRows
                  response.displayedSize = Settings.maxRows
                q.resolve(
                  responseTime: timer.stop().time()
                  table: response
                  graph: extractGraphModel(response, CypherGraphModel)
                )
              ,
              (r) ->
                q.reject(r)
            )

          #Periodic commits cannot be sent to an open transaction.
          if CypherParser.isPeriodicCommit input
            commit_fn()
          #All other queries should be sent through an open transaction
          #so they can be canceled.
          else
            r = current_transaction.begin().then(
              (begin_response) ->
                commit_fn()
              ,
              (r) ->
                q.reject(r)
            )

          q.promise.transaction = current_transaction
          q.promise.reject = q.reject
          q.promise
      ]

    # Fallback interpretor
    # offer some advice
    #  FrameProvider.interpreters.push
    #    type: 'help'
    #    matches: -> true
    #    templateUrl: 'views/frame-help.html'
    #    exec: ['$http', ($http) ->
    #      (input, q) ->
    #        url = "content/help/unknown.html"
    #        $http.get(url)
    #        .success(->q.resolve(page: url))
    #        .error(->q.reject(error("No such help section")))
    #        q.promise
    #    ]

])
