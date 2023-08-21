/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.logical.builder

import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.logical.builder.PatternParser.Pattern
import org.neo4j.cypher.internal.util.InputPosition.NONE
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.TestName

class PatternParserTest extends CypherFunSuite with TestName {
  private def patternParser = new PatternParser

  test("(a)--(b)") {
    patternParser.parse(testName) should be(Pattern("a", BOTH, Seq.empty, "UNNAMED1", "b", SimplePatternLength))
  }

  test("(a)-->(b)") {
    patternParser.parse(testName) should be(Pattern("a", OUTGOING, Seq.empty, "UNNAMED1", "b", SimplePatternLength))
  }

  test("(a)<--(b)") {
    patternParser.parse(testName) should be(Pattern("a", INCOMING, Seq.empty, "UNNAMED1", "b", SimplePatternLength))
  }

  test("(a)-[r]-(b)") {
    patternParser.parse(testName) should be(Pattern("a", BOTH, Seq.empty, "r", "b", SimplePatternLength))
  }

  test("(a)-[:R]-(b)") {
    patternParser.parse(testName) should be(Pattern(
      "a",
      BOTH,
      Seq(RelTypeName("R")(NONE)),
      "UNNAMED1",
      "b",
      SimplePatternLength
    ))
  }

  test("(a)-[r:R]-(b)") {
    patternParser.parse(testName) should be(Pattern(
      "a",
      BOTH,
      Seq(RelTypeName("R")(NONE)),
      "r",
      "b",
      SimplePatternLength
    ))
  }

  test("(a)-[r2:R2]->(x2)") {
    patternParser.parse(testName) should be(Pattern(
      "a",
      OUTGOING,
      Seq(RelTypeName("R2")(NONE)),
      "r2",
      "x2",
      SimplePatternLength
    ))
  }

  test("(a)-[r:R|T]-(b)") {
    patternParser.parse(testName) should be(Pattern(
      "a",
      BOTH,
      Seq(RelTypeName("R")(NONE), RelTypeName("T")(NONE)),
      "r",
      "b",
      SimplePatternLength
    ))
  }

  test("(p)-[investigated:IS_BEING_INVESTIGATED|WAS_INVESTIGATED]->(agent)") {
    patternParser.parse(testName) should be(Pattern(
      "p",
      OUTGOING,
      Seq(RelTypeName("IS_BEING_INVESTIGATED")(NONE), RelTypeName("WAS_INVESTIGATED")(NONE)),
      "investigated",
      "agent",
      SimplePatternLength
    ))
  }

  test("(a)-[*]-(b)") {
    patternParser.parse(testName) should be(Pattern("a", BOTH, Seq.empty, "UNNAMED1", "b", VarPatternLength(1, None)))
  }

  test("(a)-[:R*]-(b)") {
    patternParser.parse(testName) should be(Pattern(
      "a",
      BOTH,
      Seq(RelTypeName("R")(NONE)),
      "UNNAMED1",
      "b",
      VarPatternLength(1, None)
    ))
  }

  test("(a)-[:R*2]-(b)") {
    patternParser.parse(testName) should be(Pattern(
      "a",
      BOTH,
      Seq(RelTypeName("R")(NONE)),
      "UNNAMED1",
      "b",
      VarPatternLength(2, Some(2))
    ))
  }

  test("(a)-[:R*1..2]-(b)") {
    patternParser.parse(testName) should be(Pattern(
      "a",
      BOTH,
      Seq(RelTypeName("R")(NONE)),
      "UNNAMED1",
      "b",
      VarPatternLength(1, Some(2))
    ))
  }

  test("(a)-[:R*..2]-(b)") {
    patternParser.parse(testName) should be(Pattern(
      "a",
      BOTH,
      Seq(RelTypeName("R")(NONE)),
      "UNNAMED1",
      "b",
      VarPatternLength(1, Some(2))
    ))
  }

  test("(a)-[:R*2..]-(b)") {
    patternParser.parse(testName) should be(Pattern(
      "a",
      BOTH,
      Seq(RelTypeName("R")(NONE)),
      "UNNAMED1",
      "b",
      VarPatternLength(2, None)
    ))
  }

  test("(`anon_32`)--(anon_45)") {
    patternParser.parse(testName) should be(Pattern(
      "anon_32",
      BOTH,
      Seq.empty,
      "UNNAMED1",
      "anon_45",
      SimplePatternLength
    ))
  }
}
