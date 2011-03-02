
define ['neo4j/webadmin/security/HtmlEscaper','lib/backbone'], (HtmlEscaper) ->
  
  class Console extends Backbone.Model
    
    defaults : 
      lines : []
      history : []
      historyIndex : 0
      showPrompt : false
      prompt : ""

    initialize : (opts) =>
      @server = opts.server
      @eval("init()", false, false)
      @htmlEscaper = new HtmlEscaper

    eval : (statement, showStatement=true, includeInHistory=true) =>
      @set {"showPrompt":false, prompt:""}, {silent:true}
      if showStatement
        @pushLines [statement], "gremlin> "
      
      if includeInHistory
        @pushHistory statement

      @server.manage.console.exec statement, "awesome", @parseEvalResult   

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

    parseEvalResult : (result) =>
      if _(result).isString() && result.length > 0
        if result.substr(-1) is "\n"
          result = result.substr(0, result.length - 1)
        result = result.split "\n"
      else
        result = []

      @set {"showPrompt":true},{silent:true}
      @pushLines result
      
    pushHistory : (statement) =>
      if statement.length > 0
        history = @get("history")
        if history.length is 0 or history[history.length-1] != statement
          @set({history : history.concat([statement]), historyIndex:0}, {silent:true})

    pushLines : (lines, prepend="==> ") =>

      lines = for line in lines
        @htmlEscaper.escape(prepend + line)

      @set("lines" : @get("lines").concat(lines))
