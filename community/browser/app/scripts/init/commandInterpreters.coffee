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
      rv = input.toLowerCase().split(' ')
      rv or []

    error = (msg, exception = "Error", data) ->
      message: msg
      exception: exception
      data: data

    FrameProvider.interpreters.push
      type: 'clear'
      matches: "#{cmdchar}clear"
      exec: ['$rootScope', 'Frame', ($rootScope, Frame) ->
        (input) ->
          Frame.reset()
          true
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
      templateUrl: 'views/frame-help.html'
      matches: "#{cmdchar}play"
      exec: ['$http', ($http) ->
        step_number = 1
        (input, q) ->
          topic = topicalize(input[('play'.length+1)..]) or 'welcome'
          url = "content/guides/#{topic}.html"
          $http.get(url)
          .success(->q.resolve(page: url))
          .error(->q.reject(error("No such topic to play")))
          q.promise
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
          .success(->q.resolve(page: url))
          .error(->q.reject(error("No such help topic")))
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
      matches: ["#{cmdchar}get", "#{cmdchar}post", "#{cmdchar}delete", "#{cmdchar}put"]
      exec: ['Server', (Server) ->
        (input, q) ->
          regex = /^[^\w]*(get|GET|put|PUT|post|POST|delete|DELETE)\s+(\S+)\s*([\S\s]+)?$/i
          result = regex.exec(input)

          try
            [verb, url, data] = [result[1], result[2], result[3]]
          catch e
            q.reject(error("Unparseable http request"))
            return q.promise

          verb = verb?.toLowerCase()
          if not verb
            q.reject(error("Invalid verb, expected 'GET, PUT, POST or DELETE'"))
            return q.promise

          if not url?.length > 0
            q.reject(error("Missing path"))
            return q.promise

          if (verb is 'post' or verb is 'put')
            if not data
              q.reject(error("Method needs data"))
              return q.promise
            else
              # insist that data is parseable JSON
              try
                data = JSON.parse(data.replace(/\n/g, ""))
              catch e
                q.reject(error("Payload does not seem to be valid data."))
                return q.promise

          Server[verb]?(url, data)
          .then(
            (r) ->
              q.resolve(r.data)
            ,
            (r) ->
              q.reject(error("Server responded #{r.status}"))
          )

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


    # Cypher handler
    FrameProvider.interpreters.push
      type: 'cypher'
      matches: ['cypher', 'start', 'match', 'create', 'drop', 
        'return', 'set', 'remove', 'delete', 'merge', 'optional',
        'where', 'foreach', 'with'
      ]
      templateUrl: 'views/frame-cypher.html'
      exec: ['Cypher', 'GraphModel', (Cypher, GraphModel) ->
        # Return the function that handles the input
        (input, q) ->
          Cypher.transaction().commit(input).then(
            (response) ->
              if response.size > Settings.maxRows
                q.reject(error("Resultset too large (over #{Settings.maxRows} rows)"))
              else
                q.resolve(
                  table: response
                  graph: new GraphModel(response)
                )
          ,
          q.reject
          )

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
