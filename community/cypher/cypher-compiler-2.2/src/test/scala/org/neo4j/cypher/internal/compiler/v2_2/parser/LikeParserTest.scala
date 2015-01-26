/**
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
package org.neo4j.cypher.internal.compiler.v2_2.parser

import org.neo4j.cypher.internal.commons.CypherFunSuite

class LikeParserTest extends CypherFunSuite {

  test("parse a string containing normal characters") {
    LikeParser("this is a string").ops should equal(List(StringSegment("this is a string")))
  }

  test("parse a string containing wildcard") {
    LikeParser("abcd%") should equal(ParsedLike(Seq(StringSegment("abcd"), MatchAll)))
  }

  test("parse a string containing a single character match") {
    LikeParser("ab_d").ops should equal(List(StringSegment("ab"), MatchSingleChar, StringSegment("d")))
  }

  test("parse a string containing an option") {
    LikeParser("[Aa]bcd").ops should equal(List(SetMatch(Seq(RawCharacter("A"), RawCharacter("a"))), StringSegment("bcd")))
  }
}
