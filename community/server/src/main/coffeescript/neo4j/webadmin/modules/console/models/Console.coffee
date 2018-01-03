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

define ['ribcage/Model'], (Model) ->
  
  class Console extends Model
    
    defaults : 
      lines : []
      history : []
      historyIndex : 0
      showPrompt : false
      prompt : ""
      promptPrefix : ""

    initialize : (opts) =>
      @server = opts.server
      @lang = opts.lang
      @setPromptPrefix "#{@lang}> "
      @setStatement "init()"
      @eval false, false
      
    # Set the current statement (== current prompt input)
    setStatement : (str, opts={}) =>
      @set {prompt :  str}, opts
      
    getPromptPrefix : -> @get "promptPrefix" 
    setPromptPrefix : (p) -> @set "promptPrefix" : p

    # Evaluate the current statement line 
    # (the current prompt input)
    eval : (showStatement=true, includeInHistory=true, prepend = @lang) =>
      statement = @get 'prompt'
      @set {"showPrompt":false, prompt:""}, {silent:true}
      if showStatement
        @pushLines [statement], @getPromptPrefix()
      
      if includeInHistory and statement isnt ''
        @pushHistory statement
        
      @executeStatement statement
      
    executeStatement : (statement) ->
      @server.manage.console.exec statement, @lang, @parseEvalResult   

    prevHistory : =>
      history = @get "history"
      historyIndex = @get "historyIndex"
      if history.length > historyIndex
        historyIndex++
        historyItem = history[history.length - historyIndex]
        @set {historyIndex:historyIndex, prompt:historyItem}

    nextHistory : =>
      history = @get "history"
      historyIndex = @get "historyIndex"
      if historyIndex > 1
        historyIndex--
        historyItem = history[history.length - historyIndex]
        @set {historyIndex:historyIndex, prompt:historyItem}
      else if historyIndex is 1
        historyIndex--
        @set {historyIndex:historyIndex, prompt:""}
      else
        @set {prompt:""}

    parseEvalResult : (response) =>
      [result,promptPrefix] = response
      if _(result).isString() && result.length > 0
        if result.substr(-1) is "\n"
          result = result.substr(0, result.length - 1)
        result = result.split "\n"
      else
        result = []
        
      if promptPrefix isnt null
        @set "promptPrefix":promptPrefix

      @set {"showPrompt":true},{silent:true}
      @pushLines result
      
    pushHistory : (statement) =>
      if statement.length > 0
        history = @get("history")
        if history.length is 0 or history[history.length-1] != statement
          @set({history : history.concat([statement]), historyIndex:0}, {silent:true})

    pushLines : (lines, prepend="==> ") =>

      lines = for line in lines
        prepend + line

      @set("lines" : @get("lines").concat(lines))
