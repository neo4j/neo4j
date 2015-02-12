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

'use strict'

neo.style = do ->
  _style = (storage) ->
    new GraphStyle(storage)

  _style.defaults =
    autoColor: yes
    colors: [
      { color: '#DFE1E3', 'border-color': '#D4D6D7', 'text-color-internal': '#000000' }
      { color: '#F25A29', 'border-color': '#DC4717', 'text-color-internal': '#FFFFFF' }
      { color: '#AD62CE', 'border-color': '#9453B1', 'text-color-internal': '#FFFFFF' }
      { color: '#30B6AF', 'border-color': '#46A39E', 'text-color-internal': '#FFFFFF' }
      { color: '#FF6C7C', 'border-color': '#EB5D6C', 'text-color-internal': '#FFFFFF' }
      { color: '#FCC940', 'border-color': '#F3BA25', 'text-color-internal': '#000000' }
      { color: '#4356C0', 'border-color': '#3445A2', 'text-color-internal': '#FFFFFF' }
    ]
    style:
      'node':
        'diameter': '40px'
        'color': '#DFE1E3'
        'border-color': '#D4D6D7'
        'border-width': '2px'
        'text-color-internal': '#000000'
        'caption': '{id}'
        'font-size': '10px'
      'relationship':
        'color': '#D4D6D7'
        'shaft-width': '1px'
        'font-size': '8px'
        'padding': '3px'
        'text-color-external': '#000000'
        'text-color-internal': '#FFFFFF'
    sizes: [
      { diameter: '10px' }
      { diameter: '20px' }
      { diameter: '30px' }
      { diameter: '50px' }
      { diameter: '80px' }
    ]
    arrayWidths: [
      { 'shaft-width': '1px' }
      { 'shaft-width': '2px' }
      { 'shaft-width': '3px' }
      { 'shaft-width': '5px' }
      { 'shaft-width': '8px' }
      { 'shaft-width': '13px' }
      { 'shaft-width': '25px' }
      { 'shaft-width': '38px' }
    ]

  class Selector
    constructor: (selector) ->
      [@tag, @klass] = if selector.indexOf('.') > 0
        selector.split('.')
      else
        [selector, undefined]

    toString: ->
      str = @tag
      str += ".#{@klass}" if @klass?
      str

  class StyleRule
    constructor: (@selector, @props) ->

    matches: (selector) ->
      if @selector.tag is selector.tag
        if @selector.klass is selector.klass or not @selector.klass
          return yes
      return no

    matchesExact: (selector) ->
      @selector.tag is selector.tag and @selector.klass is selector.klass

  class StyleElement
    constructor: (selector, @data) ->
      @selector = selector
      @props = {}

    applyRules: (rules) ->
      # Two passes
      for rule in rules when rule.matches(@selector)
        neo.utils.extend(@props, rule.props)
        break
      for rule in rules when rule.matchesExact(@selector)
        neo.utils.extend(@props, rule.props)
        break
      @

    get: (attr) ->
      @props[attr] or ''


  class GraphStyle
    constructor: (@storage) ->
      @rules = []
      @loadRules()

    # Generate a selector string from an object (node or rel)
    selector: (item) ->
      if item.isNode
        @nodeSelector(item)
      else if item.isRelationship
        @relationshipSelector(item)

    #
    # Methods for calculating applied style for elements
    #
    calculateStyle: (selector, data) ->
      new StyleElement(selector, data).applyRules(@rules)

    forEntity: (item) ->
      @calculateStyle(@selector(item), item)

    forNode: (node = {}) ->
      selector = @nodeSelector(node)
      if node.labels?.length > 0
        @setDefaultStyling(selector)
      @calculateStyle(selector, node)

    forRelationship: (rel) ->
      @calculateStyle(@relationshipSelector(rel), rel)

    findAvailableDefaultColor: () ->
      usedColors = {}
      for rule in @rules
        if rule.props.color?
          usedColors[rule.props.color] = yes

      for defaultColor in _style.defaults.colors
        if !usedColors[defaultColor.color]?
          return neo.utils.copy(defaultColor)

      return neo.utils.copy(_style.defaults.colors[0])

    setDefaultStyling: (selector) ->
      rule = @findRule(selector)

      if _style.defaults.autoColor and not rule?
        rule = new StyleRule(selector, @findAvailableDefaultColor())
        @rules.push(rule)
        @persist()

    #
    # Methods for getting and modifying rules
    #
    change: (item, props) ->
      selector = @selector(item)
      rule = @findRule(selector)

      if not rule?
        rule = new StyleRule(selector, {})
        @rules.push(rule)
      neo.utils.extend(rule.props, props)
      @persist()
      rule

    destroyRule: (rule) ->
      idx = @rules.indexOf(rule)
      @rules.splice(idx, 1) if idx?
      @persist()

    findRule: (selector) ->
      rule = r for r in @rules when r.matchesExact(selector)
      rule

    #
    # Selector helpers
    #
    nodeSelector: (node = {}) ->
      selector = 'node'
      if node.labels?.length > 0
        selector += ".#{node.labels[0]}"
      new Selector(selector)

    relationshipSelector: (rel = {}) ->
      selector = 'relationship'
      selector += ".#{rel.type}" if rel.type?
      new Selector(selector)

    #
    # Import/export
    #

    importGrass: (string) ->
      try
        rules = @parse(string)
        @loadRules(rules)
        @persist()
      catch e
        return

    loadRules: (data) ->
      data = _style.defaults.style unless neo.utils.isObject(data)
      @rules.length = 0
      for rule, props of data
        @rules.push(new StyleRule(new Selector(rule), neo.utils.copy(props)))
      @

    parse: (string)->
      chars = string.split('')
      insideString = no
      insideProps = no
      keyword = ""
      props = ""

      rules = {}

      for c in chars
        skipThis = yes
        switch c
          when "{"
            if not insideString
              insideProps = yes
            else
              skipThis = no
          when "}"
            if not insideString
              insideProps = no
              rules[keyword] = props
              keyword = ""
              props = ""
            else
              skipThis = no
          when "'", "\"" then insideString ^= true
          else skipThis = no

        continue if skipThis

        if insideProps
          props += c
        else
          keyword += c unless c.match(/[\s\n]/)

      for k, v of rules
        rules[k] = {}
        for prop in v.split(';')
          [key, val] = prop.split(':')
          continue unless key and val
          rules[k][key?.trim()] = val?.trim()

      rules

    persist: ->
      @storage?.add('grass', JSON.stringify(@toSheet()))

    resetToDefault: ->
      @loadRules()
      @persist()

    toSheet: ->
      sheet = {}
      sheet[rule.selector.toString()] = rule.props for rule in @rules
      sheet

    toString: ->
      str = ""
      for r in @rules
        str += r.selector.toString() + " {\n"
        for k, v of r.props
          v = "'#{v}'" if k == "caption"
          str += "  #{k}: #{v};\n"
        str += "}\n\n"
      str

    #
    # Misc.
    #
    nextDefaultColor: 0
    defaultColors: -> neo.utils.copy(_style.defaults.colors)
    interpolate: (str, id, properties) ->
      # Supplant
      # http://javascript.crockford.com/remedial.html
      str.replace(
        /\{([^{}]*)\}/g,
        (a, b) ->
          r = properties[b] or id
          return if (typeof r is 'string' or typeof r is 'number') then r else a
      )

  _style