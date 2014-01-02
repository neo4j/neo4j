###!
Copyright (c) 2002-2014 "Neo Technology,"
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
  .factory 'motdService', [
    '$log'
    'rssFeedService'
    ($log, rssFeedService) ->
      class Motd

        choices =
          quotes: [
            { 'text':'When you label me, you negate me.', 'author':'Soren Kierkegaard' }
            { 'text':'In the beginning was the command line.', 'author':'Neal Stephenson' }
            { 'text':'Remember, all I\'m offering is the truth – nothing more.', 'author':'Morpheus'}
            { 'text':'Testing can show the presence of bugs, but never their absence.', 'author':'Edsger W. Dijkstra'}
            { 'text':'We think your graph is a special snowflake.', 'author':'Neo4j'}
            { 'text':'Still he\'d see the matrix in his sleep, bright lattices of logic unfolding across that colorless void.', 'author':'William Gibson'}
            { 'text':'Eventually everything connects.', 'author':'Charles Eames'}
            { 'text':'To develop a complete mind: study the science of art. Study the art of science. Develop your senses - especially learn how to see. Realize that everything connects to everything else.', 'author':'Leonardo da Vinci'}
          ],
          tips: [
            'Use <shift-return> for multi-line, <cmd-return> to evaluate command'
            'Navigate history with <ctrl- up/down arrow>'
            'When in doubt, ask for :help'
          ],
          unrecognizable: [
            "Interesting. How does this make you feel?"
            "Even if I squint, I can't make out what that is. Is it an elephant?"
            "This one time, at bandcamp..."
            "Ineffable, enigmatic, possibly transcendent. Also quite good looking."
            "I'm not (yet) smart enough to understand this."
            "Oh I agree. Kaaviot ovat suuria!"
          ],
          emptiness: [
            "No nodes. Know nodes?"
            "Waiting for the big bang of data."
            "Ready for anything."
            "Every graph starts with the first node."
          ],
          disconnected: [
            "Please check if the cord is unplugged."
          ],
          callToAction: [
            {
              'd': "Every good graph starts with Neo4j."
              'u':'http://neo4j.org'
            }
          ]

        quote: ""

        tip: ""

        unrecognized: ""

        emptiness: ""

        constructor: ->
          @refresh()

        refresh: ->
          @quote = @pickRandomlyFrom(choices.quotes)
          @tip = @pickRandomlyFrom(choices.tips)
          @unrecognized = @pickRandomlyFrom(choices.unrecognizable)
          @emptiness = @pickRandomlyFrom(choices.emptiness)
          @disconnected = @pickRandomlyFrom(choices.disconnected)
          @callToAction = @pickRandomlyFrom(choices.callToAction)
          rssFeedService.get().then (feed) =>
            if feed[0]
              @callToAction = feed[0]


        pickRandomlyFrom: (fromThis) ->
          return fromThis[Math.floor(Math.random() * fromThis.length)]

      new Motd
]
