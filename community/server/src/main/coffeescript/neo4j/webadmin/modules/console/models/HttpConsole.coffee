###
Copyright (c) 2002-2018 "Neo Technology,"
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

define ['./Console'], (Console) ->
  
  class HttpConsole extends Console

    statementRegex : /^((GET)|(PUT)|(POST)|(DELETE)) ([^ ]+)( (.+))?$/i

    initialize : (opts) =>
      @server = opts.server
      @lang = opts.lang
      @setPromptPrefix "#{@lang}> "
      @set {"showPrompt":true},{silent:true}
  
    executeStatement : (statement) ->
      if @statementRegex.test statement
        result = @statementRegex.exec statement
        [method, url, data] = [result[1], result[6], result[8]]
        if data
          try
            @server.web.ajax method, url, JSON.parse(data), @callSucceeded, @callFailed
          catch e
            @setResult ["Invalid JSON data."]
        else
          @server.web.ajax method, url, @callSucceeded, @callFailed
      else
        @setResult ["Invalid statement."]
        
    setResult : (lines) ->
      @set {"showPrompt":true},{silent:true}
      @pushLines lines
      
    callSucceeded : (responseData, type, response) =>
      status = [response.status + " " + response.statusText]
      lines = response.responseText.split "\n"
      @setResult status.concat lines
      
    callFailed : (response) =>
      @callSucceeded null, null, arguments[0].req
        
      
      
      
