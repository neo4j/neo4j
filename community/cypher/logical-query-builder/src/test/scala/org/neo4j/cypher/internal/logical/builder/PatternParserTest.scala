/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.logical.builder

import org.neo4j.cypher.internal.ir.{SimplePatternLength, VarPatternLength}
import org.neo4j.cypher.internal.logical.builder.PatternParser.Pattern
import org.neo4j.cypher.internal.v4_0.expressions.RelTypeName
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}
import org.neo4j.cypher.internal.v4_0.util.InputPosition.NONE
import org.neo4j.cypher.internal.v4_0.util.test_helpers.{CypherFunSuite, TestName}

class PatternParserTest extends CypherFunSuite with TestName
{
  val patternParser = new PatternParser

  test("(a)--(b)") {
    patternParser.parse(testName) should be(Pattern("a", BOTH, Seq.empty, "UNNAMED1", "b", SimplePatternLength))
  }

  test("(a)-->(b)") {
    patternParser.parse(testName) should be(Pattern("a", OUTGOING, Seq.empty, "UNNAMED2", "b", SimplePatternLength))
  }

  test("(a)<--(b)") {
    patternParser.parse(testName) should be(Pattern("a", INCOMING, Seq.empty, "UNNAMED3", "b", SimplePatternLength))
  }

  test("(a)-[r]-(b)") {
    patternParser.parse(testName) should be(Pattern("a", BOTH, Seq.empty, "r", "b", SimplePatternLength))
  }

  test("(a)-[:R]-(b)") {
    patternParser.parse(testName) should be(Pattern("a", BOTH, Seq(RelTypeName("R")(NONE)), "UNNAMED4", "b", SimplePatternLength))
  }

  test("(a)-[r:R]-(b)") {
    patternParser.parse(testName) should be(Pattern("a", BOTH, Seq(RelTypeName("R")(NONE)), "r", "b", SimplePatternLength))
  }

  test("(a)-[r:R|T]-(b)") {
    patternParser.parse(testName) should be(Pattern("a", BOTH, Seq(RelTypeName("R")(NONE), RelTypeName("T")(NONE)), "r", "b", SimplePatternLength))
  }

  test("(p)-[investigated:IS_BEING_INVESTIGATED|WAS_INVESTIGATED]->(agent)") {
    patternParser.parse(testName) should be(Pattern("p", OUTGOING, Seq(RelTypeName("IS_BEING_INVESTIGATED")(NONE), RelTypeName("WAS_INVESTIGATED")(NONE)), "investigated", "agent", SimplePatternLength))
  }

  test("(a)-[*]-(b)") {
    patternParser.parse(testName) should be(Pattern("a", BOTH, Seq.empty, "UNNAMED5", "b", VarPatternLength(0, None)))
  }

  test("(a)-[:R*]-(b)") {
    patternParser.parse(testName) should be(Pattern("a", BOTH, Seq(RelTypeName("R")(NONE)), "UNNAMED6", "b", VarPatternLength(0, None)))
  }

  test("(a)-[:R*2]-(b)") {
    patternParser.parse(testName) should be(Pattern("a", BOTH, Seq(RelTypeName("R")(NONE)), "UNNAMED7", "b", VarPatternLength(2, Some(2))))
  }

  test("(a)-[:R*1..2]-(b)") {
    patternParser.parse(testName) should be(Pattern("a", BOTH, Seq(RelTypeName("R")(NONE)), "UNNAMED8", "b", VarPatternLength(1, Some(2))))
  }

  test("(a)-[:R*..2]-(b)") {
    patternParser.parse(testName) should be(Pattern("a", BOTH, Seq(RelTypeName("R")(NONE)), "UNNAMED9", "b", VarPatternLength(0, Some(2))))
  }
}
