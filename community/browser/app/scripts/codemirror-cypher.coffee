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

wordRegexp = (words) ->
  new RegExp("^(?:" + words.join("|") + ")$", "i")
CodeMirror.defineMode "cypher", (config) ->
  tokenBase = (stream, state) ->
    ch = stream.next()
    curPunc = null
    if ch is "\"" or ch is "'"
      stream.match /.+?["']/
      return "string"
    if /[{}\(\),\.;\[\]]/.test(ch)
      curPunc = ch
      "node"
    else if ch is "/" and stream.eat("/")
      stream.skipToEnd()
      "comment"
    else if operatorChars.test(ch)
      stream.eatWhile operatorChars
      null
    else
      stream.eatWhile /[_\w\d]/
      if stream.eat(":")
        stream.eatWhile /[\w\d_\-]/
        return "atom"
      word = stream.current()
      type = undefined
      return "builtin"  if funcs.test(word)
      if preds.test(word)
        "def"
      else if keywords.test(word)
        "keyword"
      else
        "variable"
  tokenLiteral = (quote) ->
    (stream, state) ->
      escaped = false
      ch = undefined
      while (ch = stream.next())?
        if ch is quote and not escaped
          state.tokenize = tokenBase
          break
        escaped = not escaped and ch is "\\"
      "string"
  pushContext = (state, type, col) ->
    state.context =
      prev: state.context
      indent: state.indent
      col: col
      type: type
  popContext = (state) ->
    state.indent = state.context.indent
    state.context = state.context.prev
  indentUnit = config.indentUnit
  curPunc = undefined
  funcs = wordRegexp(["str", "min", "labels", "max", "type", "lower", "upper", "length", "type", "id", "coalesce", "head", "last", "nodes", "relationships", "extract", "filter", "tail", "range", "reduce", "abs", "round", "sqrt", "sign", "replace", "substring", "left", "right", "ltrim", "rtrim", "trim", "collect", "distinct", "split", "toInt", "toFloat"])
  preds = wordRegexp(["all", "any", "none", "single", "not", "in", "has", "and", "or"])
  keywords = wordRegexp(["start", "merge", "load", "csv", "using", "periodic commit", "on create", "on match", "match", "index on", "drop", "where", "with", "limit", "skip", "order", "by", "return", "create", "delete", "set", "unique", "unwind"])
  operatorChars = /[*+\-<>=&|~]/
  startState: (base) ->
    tokenize: tokenBase
    context: null
    indent: 0
    col: 0

  token: (stream, state) ->
    if stream.sol()
      state.context.align = false  if state.context and not state.context.align?
      state.indent = stream.indentation()
    return null  if stream.eatSpace()
    style = state.tokenize(stream, state)
    state.context.align = true  if style isnt "comment" and state.context and not state.context.align? and state.context.type isnt "pattern"
    if curPunc is "("
      pushContext state, ")", stream.column()
    else if curPunc is "["
      pushContext state, "]", stream.column()
    else if curPunc is "{"
      pushContext state, "}", stream.column()
    else if /[\]\}\)]/.test(curPunc)
      popContext state  while state.context and state.context.type is "pattern"
      popContext state  if state.context and curPunc is state.context.type
    else if curPunc is "." and state.context and state.context.type is "pattern"
      popContext state
    else if /atom|string|variable/.test(style) and state.context
      if /[\}\]]/.test(state.context.type)
        pushContext state, "pattern", stream.column()
      else if state.context.type is "pattern" and not state.context.align
        state.context.align = true
        state.context.col = stream.column()
    style

  indent: (state, textAfter) ->
    firstChar = textAfter and textAfter.charAt(0)
    context = state.context
    context = context.prev  while context and context.type is "pattern"  if /[\]\}]/.test(firstChar)
    closing = context and firstChar is context.type
    unless context
      0
    else if context.type is "keywords"
      newlineAndIndent
    else if context.align
      context.col + ((if closing then 0 else 1))
    else
      context.indent + ((if closing then 0 else indentUnit))

CodeMirror.modeExtensions["cypher"] = autoFormatLineBreaks: (text) ->
  lines = text.split("\n")
  reProcessedPortion = /\s+\b(return|where|order by|match|with|skip|limit|create|delete|set)\b\s/g
  i = 0

  while i < lines.length
    lines[i] = lines[i].replace(reProcessedPortion, " \n$1 ").trim()
    i++
  lines.join "\n"

CodeMirror.defineMIME "application/x-cypher-query", "cypher"
