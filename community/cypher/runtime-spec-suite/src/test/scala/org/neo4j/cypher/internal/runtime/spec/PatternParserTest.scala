/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.spec

import org.neo4j.cypher.internal.ir.v4_0.VarPatternLength
import org.neo4j.cypher.internal.runtime.spec.PatternParser.Pattern
import org.neo4j.cypher.internal.v4_0.expressions.RelTypeName
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}
import org.neo4j.cypher.internal.v4_0.util.InputPosition.NONE
import org.neo4j.cypher.internal.v4_0.util.test_helpers.{CypherFunSuite, TestName}

class PatternParserTest extends CypherFunSuite with TestName
{


  test("(a)--(b)") {
    PatternParser.parse(testName) should be(Pattern("a", BOTH, Seq.empty, "", "b", VarPatternLength(1, Some(1))))
  }

  test("(a)-->(b)") {
    PatternParser.parse(testName) should be(Pattern("a", OUTGOING, Seq.empty, "", "b", VarPatternLength(1, Some(1))))
  }

  test("(a)<--(b)") {
    PatternParser.parse(testName) should be(Pattern("a", INCOMING, Seq.empty, "", "b", VarPatternLength(1, Some(1))))
  }

  test("(a)-[r]-(b)") {
    PatternParser.parse(testName) should be(Pattern("a", BOTH, Seq.empty, "r", "b", VarPatternLength(1, Some(1))))
  }

  test("(a)-[:R]-(b)") {
    PatternParser.parse(testName) should be(Pattern("a", BOTH, Seq(RelTypeName("R")(NONE)), "", "b", VarPatternLength(1, Some(1))))
  }

  test("(a)-[r:R]-(b)") {
    PatternParser.parse(testName) should be(Pattern("a", BOTH, Seq(RelTypeName("R")(NONE)), "r", "b", VarPatternLength(1, Some(1))))
  }

  test("(a)-[r:R|T]-(b)") {
    PatternParser.parse(testName) should be(Pattern("a", BOTH, Seq(RelTypeName("R")(NONE), RelTypeName("T")(NONE)), "r", "b", VarPatternLength(1, Some(1))))
  }

  test("(a)-[:R*]-(b)") {
    PatternParser.parse(testName) should be(Pattern("a", BOTH, Seq(RelTypeName("R")(NONE)), "", "b", VarPatternLength(0, None)))
  }

  test("(a)-[:R*2]-(b)") {
    PatternParser.parse(testName) should be(Pattern("a", BOTH, Seq(RelTypeName("R")(NONE)), "", "b", VarPatternLength(2, Some(2))))
  }

  test("(a)-[:R*1..2]-(b)") {
    PatternParser.parse(testName) should be(Pattern("a", BOTH, Seq(RelTypeName("R")(NONE)), "", "b", VarPatternLength(1, Some(2))))
  }

  test("(a)-[:R*..2]-(b)") {
    PatternParser.parse(testName) should be(Pattern("a", BOTH, Seq(RelTypeName("R")(NONE)), "", "b", VarPatternLength(0, Some(2))))
  }
}
