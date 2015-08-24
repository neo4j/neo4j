/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.semantics.v2_3.parser

import org.neo4j.cypher.internal.semantics.v2_3.test_helpers.CypherFunSuite

class LikePatternParserTest extends CypherFunSuite {

  test("parse a string containing normal characters") {
    LikePatternParser("this is a string").ops should equal(List(MatchText("this is a string")))
  }

  test("parse a string containing wildcard") {
    LikePatternParser("abcd%") should equal(ParsedLikePattern(List(MatchText("abcd"), MatchMany)))
  }

  test("parse a string containing a single character match") {
    LikePatternParser("ab_d").ops should equal(List(MatchText("ab"), MatchSingle, MatchText("d")))
  }

  test("parse an escaped string") {
    LikePatternParser("the richest 1\\% of adults alone owned 40\\% of global assets").ops should
      equal(List(MatchText("the richest 1% of adults alone owned 40% of global assets")))
    }

  test("combining wildcard and escaped percent") {
    LikePatternParser("""%\%_%""").ops should equal(List(MatchMany, MatchText("%"), MatchSingle, MatchMany))
  }

  test("combining wildcard and escaped underscore") {
    LikePatternParser("""%\_%""").ops should equal(List(MatchMany, MatchText("_"), MatchMany))
  }
}
