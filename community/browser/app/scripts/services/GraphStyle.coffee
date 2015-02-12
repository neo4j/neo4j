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
        'diameter': '40px'
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

    # Default node sizes that user can choose from
    @defaultSizes = [
      { diameter: '10px' }
      { diameter: '20px' }
      { diameter: '30px' }
      { diameter: '50px' }
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
      constructor: (selector) ->
        @selector = selector
        @props = {}

      applyRules: (rules) ->
        # Two passes
        for rule in rules when rule.matches(@selector)
          angular.extend(@props, rule.props)
          break
        for rule in rules when rule.matchesExact(@selector)
          angular.extend(@props, rule.props)
          break
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

      #
      # Methods for calculating applied style for elements
      #
      calculateStyle: (selector) ->
        new StyleElement(selector).applyRules(@rules)

      forEntity: (item) ->
        @calculateStyle(@selector(item))

      forNode: (node = {}, idx = 0) ->
        selector = @nodeSelector(node, idx)
        if node.labels?.length > 0
          @setDefaultStyling(selector, node)
        @calculateStyle(selector)

      forRelationship: (rel) ->
        selector = @relationshipSelector(rel)
        @setDefaultRelationshipStyling(selector, rel)
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

      setDefaultRelationshipStyling: (selector, relationship) ->
        rule = @findRule(selector)

        if not rule?
          rule = new StyleRule(selector, angular.extend(provider.defaultStyle.relationship, @getDefaultRelationshipCaption()))
          @rules.push(rule)
          @persist()
        if not rule.props.caption?
          default_caption = @getDefaultRelationshipCaption()
          angular.extend(rule.props, default_caption)
          @persist()

      setDefaultStyling: (selector, item) ->
        rule = @findRule(selector)

        if not rule?
          rule = new StyleRule(selector, angular.extend(@findAvailableDefaultColor(), @getDefaultCaption(item)))
          @rules.push(rule)
          @persist()
        if not rule.props.caption?
          default_caption = @getDefaultCaption(item)
          angular.extend(rule.props, default_caption)
          @rules.push(rule)
          @persist()

      getDefaultCaption: (item) ->
        return {caption: '<id>'} if not item or not item.propertyList?.length > 0
        default_caption = {caption: "{#{item.propertyList?[0].key}}"}
        default_caption

      getDefaultRelationshipCaption: (item) ->
        return {caption: '<type>'} 

      #
      # Methods for getting and modifying rules
      #
      change: (item, props) ->
        selector = @selector(item)
        @changeForSelector(selector, props)

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
        rule = r for r in @rules when r.matchesExact(selector)
        rule

      #
      # Selector helpers
      #
      # FIXME: until we support styling nodes with multiple labels separately.
      # Provide an option to select which label to use
      nodeSelector: (node = {}, labelIdx = 0) ->
        selector = 'node'
        if node.labels?.length > 0
          selector += ".#{node.labels[labelIdx]}"
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
        data = provider.defaultStyle unless angular.isObject(data)
        @rules.length = 0
        for rule, props of data
          @rules.push(new StyleRule(new Selector(rule), angular.copy(props)))
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


