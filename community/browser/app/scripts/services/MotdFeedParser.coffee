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

class MotdFeedParser

  constructor: ->

  explodeTags: (tags) ->
    out = {}
    return out unless tags.length
    for pair in tags
      parts = pair.split('=')
      out[parts[0]] = parts[1]
    out

  getFirstMatch: (feed, match_filter) ->
    that = @
    items = feed.filter (x) ->
      return true if not Object.keys(match_filter).length
      tags = that.explodeTags(x.t)
      for k, v of match_filter
        return false unless v tags[k]
      true
    items[0] or {}

angular.module('neo4jApp.services').service 'motdFeedParser', [MotdFeedParser]
