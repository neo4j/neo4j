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

'use strict';

angular.module('neo4jApp.services')
  .provider 'GraphStyle', [->
    provider = @

    # Default style
    @defaultStyle =
      'node':
        'diameter': '50px'
        'color': '#A5ABB6'
        'border-color': '#9AA1AC'
        'border-width': '2px'
        'text-color-internal': '#FFFFFF'
        'font-size': '10px'
      'relationship':
        'color': '#A5ABB6'
        'shaft-width': '1px'
        'font-size': '8px'
        'padding': '3px'
        'text-color-external': '#000000'
        'text-color-internal': '#FFFFFF'
        'caption': '<type>'

    # Default node sizes that user can choose from
    @defaultSizes = [
      { diameter: '10px' }
      { diameter: '20px' }
      { diameter: '50px' }
      { diameter: '65px' }
      { diameter: '80px' }
    ]

    # Default arrow widths that user can choose from
    @defaultArrayWidths = [
      { 'shaft-width': '1px' }
      { 'shaft-width': '2px' }
      { 'shaft-width': '3px' }
      { 'shaft-width': '5px' }
      { 'shaft-width': '8px' }
      { 'shaft-width': '13px' }
      { 'shaft-width': '25px' }
      { 'shaft-width': '38px' }
    ]

    # Default node colors that user can choose from
    @defaultColors = [
      { color: '#A5ABB6', 'border-color': '#9AA1AC', 'text-color-internal': '#FFFFFF' }
      { color: '#68BDF6', 'border-color': '#5CA8DB', 'text-color-internal': '#FFFFFF' }
      { color: '#6DCE9E', 'border-color': '#60B58B', 'text-color-internal': '#FFFFFF' }
      { color: '#FF756E', 'border-color': '#E06760', 'text-color-internal': '#FFFFFF' }
      { color: '#DE9BF9', 'border-color': '#BF85D6', 'text-color-internal': '#FFFFFF' }
      { color: '#FB95AF', 'border-color': '#E0849B', 'text-color-internal': '#FFFFFF' }
      { color: '#FFD86E', 'border-color': '#EDBA39', 'text-color-internal': '#604A0E' }
    ]

    class Selector
      constructor: (@tag, @classes = []) ->

      toString: ->
        str = @tag
        for classs in @classes
          if classs?
            str += ".#{classs}"
        str

    class StyleRule
      constructor: (@selector, @props) ->

      matches: (selector) ->
        return no unless @selector.tag is selector.tag
        for classs in @selector.classes
          if classs? and selector.classes.indexOf(classs) is -1
            return no
        yes

      matchesExact: (selector) ->
        @matches(selector) and @selector.classes.length is selector.classes.length

    class StyleElement
      constructor: (selector) ->
        @selector = selector
        @props = {}

      applyRules: (rules) ->
        for rule in rules
          if rule.matches(@selector)
            angular.extend(@props, rule.props)
        @

      get: (attr) ->
        @props[attr] or ''

    class GraphStyle
      constructor: (@storage) ->
        @rules = []
        try
          @loadRules(@storage?.get('grass'))
        catch e

      # Generate a selector string from an object (node or rel)
      selector: (item) ->
        if item.isNode
          @nodeSelector(item)
        else if item.isRelationship
          @relationshipSelector(item)

      newSelector: (tag, classes) ->
        new Selector(tag, classes)

      #
      # Methods for calculating applied style for elements
      #
      calculateStyle: (selector) ->
        new StyleElement(selector).applyRules(@rules)

      forEntity: (item) ->
        @calculateStyle(@selector(item))

      forNode: (node = {}) ->
        selector = @nodeSelector(node)
        if node.labels?.length > 0
          @setDefaultNodeStyling(selector, node)
        @calculateStyle(selector)

      forRelationship: (rel) ->
        selector = @relationshipSelector(rel)
        @calculateStyle(selector)

      findAvailableDefaultColor: () ->
        usedColors = {}
        for rule in @rules
          if rule.props.color?
            usedColors[rule.props.color] = yes

        for defaultColor in provider.defaultColors
          if !usedColors[defaultColor.color]?
            return defaultColor

        return provider.defaultColors[0]

      setDefaultNodeStyling: (selector, item) ->
        defaultColor = yes
        defaultCaption = yes
        for rule in @rules
          if rule.selector.classes.length > 0 and rule.matches(selector)
            if rule.props.hasOwnProperty('color')
              defaultColor = no
            if rule.props.hasOwnProperty('caption')
              defaultCaption = no

        minimalSelector = new Selector(selector.tag, selector.classes.sort().slice(0, 1))
        if defaultColor
          @changeForSelector(minimalSelector, @findAvailableDefaultColor())
        if defaultCaption
          @changeForSelector(minimalSelector, @getDefaultNodeCaption(item))

      getDefaultNodeCaption: (item) ->
        return {caption: '<id>'} if not item or not item.propertyList?.length > 0
        default_caption = {caption: "{#{item.propertyList?[0].key}}"}
        default_caption

      changeForSelector: (selector, props) ->
        rule = @findRule(selector)
        if not rule?
          rule = new StyleRule(selector, {})
          @rules.push(rule)
        angular.extend(rule.props, props)
        @persist()
        rule

      destroyRule: (rule) ->
        idx = @rules.indexOf(rule)
        @rules.splice(idx, 1) if idx?
        @persist()

      findRule: (selector) ->
        for r in @rules
          if r.matchesExact(selector)
            return r
        undefined

      nodeSelector: (node = {}) ->
        classes = if node.labels?
          node.labels
        else
          []
        new Selector('node', classes)

      relationshipSelector: (rel = {}) ->
        classes = if rel.type?
          [rel.type]
        else
          []
        new Selector('relationship', classes)

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

      parseSelector = (key) ->
        tokens = key.split('.')
        new Selector(tokens[0], tokens.slice(1))

      loadRules: (data) ->
        data = provider.defaultStyle unless angular.isObject(data)
        @rules.length = 0
        for key, props of data
          @rules.push(new StyleRule(parseSelector(key), angular.copy(props)))
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
      defaultSizes: -> provider.defaultSizes
      defaultArrayWidths: -> provider.defaultArrayWidths
      defaultColors: -> angular.copy(provider.defaultColors)
      interpolate: (str, item) ->
        ips = str.replace( #Caption from user set properties as {property} 
          /\{([^{}]*)\}/g,
          (a, b) ->
            r = item.propertyMap[b]
            if typeof r is 'object'
              r = r.join(', ')
            return if (typeof r is 'string' or typeof r is 'number') then r else ''
        )
        
        #Backwards compability
        ips = '<type>' if ips.length < 1 and str == "{type}" and item.isRelationship
        ips = '<id>' if ips.length < 1 and str == "{id}" and item.isNode

        ips.replace( #<id> and <type> from item properties
          /^<(id|type)>$/,
          (a,b) ->
            r = item[b]
            return if (typeof r is 'string' or typeof r is 'number') then r else ''
        )


    @$get = ['localStorageService', (localStorageService) ->
      new GraphStyle(localStorageService)
    ]
    @
  ]


