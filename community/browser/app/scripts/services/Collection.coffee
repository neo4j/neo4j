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

#
# A generic collection data type with ID awareness
#

angular.module('neo4jApp.services')
  .factory 'Collection', [
    () ->
      class Collection
        constructor: (items, @_model) ->
          @_reset()
          @add(items) if items?

        #
        # Instance methods
        #
        add: (items, opts = {}) ->
          return unless items?
          items = if angular.isArray(items) then items else [items]
          itemsToAdd = []
          for i in items
            # Convert to model if defined
            if @_model and not (i instanceof @_model)
              i = new @_model(i)
            continue if not i? or @get(i)
            @_byId[if i.id? then i.id else i] = i
            itemsToAdd.push(i)

          if itemsToAdd.length
            if angular.isNumber(opts.at)
              [].splice.apply(@items, [opts.at, 0].concat(itemsToAdd));
            else
              [].push.apply(@items, itemsToAdd)
            @length += itemsToAdd.length
          @

        all: ->
          @items

        first: ->
          @items[0]

        get: (id) ->
          return undefined unless id?
          id = if id.id? then id.id else id
          @_byId[id]

        indexOf: (item) ->
          @items.indexOf item

        last: ->
          @items[@length-1]

        next: (item) ->
          idx = @indexOf(item)
          return unless idx?
          @items[++idx]

        remove: (items) ->
          itemsToRemove = if angular.isArray(items) then items else [items]
          for item in itemsToRemove
            item = @get(item);
            continue unless item
            delete @_byId[item.id];
            index = @items.indexOf(item);
            @items.splice(index, 1);
            @length--
          items

        reset: (items) ->
          @_reset()
          @add(items)

        pluck: (attr) ->
          return undefined unless angular.isString(attr)
          i[attr] for i in @items

        prev: (item) ->
          idx = @indexOf(item)
          return unless idx?
          @items[--idx]

        pop: ->
          item = @items.pop()
          @length = @items.length
          item

        push: (items) ->
          itemsToPush = if angular.isArray(items) then items else [items]
          @items.push.apply(@items, itemsToPush)
          @length = @items.length
          @

        where: (attrs) ->
          rv = []
          return rv unless angular.isObject(attrs)

          numAttrs = Object.keys(attrs).length

          for item in @items
            matches = 0
            for key, val of attrs
              matches++ if item[key] is val

            rv.push item if numAttrs is matches

          rv

        #
        # Proxied model methods for persistance
        # (from eg. Persistable class)
        #

        save: ->
          return unless @_model or angular.isFunction(@_model.save)
          @_model.save(@all())
          @

        fetch: ->
          return unless @_model or angular.isFunction(@_model.fetch)
          @reset(@_model.fetch())
          @

        #
        # Internal methods
        #

        _reset: ->
          @items = []
          @_byId = {}
          @length = 0

      Collection
]
