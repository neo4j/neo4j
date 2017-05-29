###!
Copyright (c) 2002-2017 "Neo Technology,"
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
  .service 'CypherParser', [
     'Utils'
      'Cypher'
     (Utils, Cypher) ->
        class CypherParser
          constructor: () ->
            @last_checked_query = ''

          isPeriodicCommit: (input)->
            pattern = /^\s*USING\sPERIODIC\sCOMMIT/i
            clean_input = Utils.stripComments input
            pattern.test clean_input

          isProfileExplain: (input) ->
            pattern = /^\s*(PROFILE|EXPLAIN)\s/i
            clean_input = Utils.stripComments input
            pattern.test clean_input

          runHints: (editor, cb) ->
            input = editor.getValue()
            if not input then return editor.clearGutter 'cypher-hints'
            return if input is @last_checked_query
            return if @isPeriodicCommit(input) or @isProfileExplain(input)
            @last_checked_query = input
            that = @
            p = Cypher.transaction().commit("EXPLAIN #{input}")
            p.then(
              (res) ->
                cb null, res
              ,
              ->
                cb yes, null
                that.last_checked_query = ''
            )


        new CypherParser()
  ]
