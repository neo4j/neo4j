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
package org.neo4j.cypher.internal.compiler.planner

import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.ir.{PatternRelationship, QueryGraph, SimplePatternLength}
import org.neo4j.cypher.internal.v4_0.ast.UsingIndexHint
import org.neo4j.cypher.internal.v4_0.expressions.{LabelName, PropertyKeyName, Variable}
import org.neo4j.cypher.internal.v4_0.util.InputPosition

class QueryGraphTest extends CypherFunSuite {
  val x = "x"
  val n = "n"
  val m = "m"
  val c = "c"
  val r1 = "r1"
  val r2 = "r2"
  val r3 = "r3"

  private val pos = InputPosition.NONE
  private val hint1 = UsingIndexHint(Variable("n")(pos), LabelName("Label")(pos), Seq(PropertyKeyName("prop1")(pos)))(pos)
  private val hint2 = UsingIndexHint(Variable("m")(pos), LabelName("Label")(pos), Seq(PropertyKeyName("prop2")(pos)))(pos)

  test("returns no pattern relationships when the query graph doesn't contain any") {
    val rels: Set[PatternRelationship] = Set.empty
    val qg = QueryGraph(patternRelationships = rels)

    qg.findRelationshipsEndingOn(x) shouldBe empty
  }

  test("finds single pattern relationship") {
    val r = PatternRelationship(r1, (n, m), BOTH, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(patternRelationships = Set(r))

    qg.findRelationshipsEndingOn(x) shouldBe empty
    qg.findRelationshipsEndingOn(n) should equal(Set(r))
    qg.findRelationshipsEndingOn(m) should equal(Set(r))
  }

  test("finds multiple pattern relationship") {
    val pattRel1 = PatternRelationship(r1, (n, m), BOTH, Seq.empty, SimplePatternLength)
    val pattRel2 = PatternRelationship(r2, (m, c), BOTH, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(patternRelationships = Set(pattRel1, pattRel2))

    qg.findRelationshipsEndingOn(x) shouldBe empty
    qg.findRelationshipsEndingOn(n) should equal(Set(pattRel1))
    qg.findRelationshipsEndingOn(m) should equal(Set(pattRel1, pattRel2))
    qg.findRelationshipsEndingOn(c) should equal(Set(pattRel2))
  }

  test("addHints should add new hints") {
    val qg1 = QueryGraph(hints = Set(hint1))
    val qg2 = QueryGraph(hints = Set(hint1, hint2))

    qg1.addHints(Set(hint2)) should equal(qg2)
  }

  test("addHint should not add already existing hint") {
    val qg1 = QueryGraph(hints = Set(hint1))
    val qg2 = QueryGraph(hints = Set(hint1, hint2))

    qg1.addHints(Set(hint1, hint2)) should equal(qg2)
  }

  test("withoutHints should remove hints") {
    val qg1 = QueryGraph(hints = Set(hint1, hint2))
    val qg2 = QueryGraph(hints = Set(hint1))

    qg1.withoutHints(Set(hint2)) should equal(qg2)
  }

  test("should not get duplicate hints when combining query graphs") {
    val hint3 = UsingIndexHint(Variable("o")(pos), LabelName("Label")(pos), Seq(PropertyKeyName("prop3")(pos)))(pos)
    val qg1 = QueryGraph(hints = Set(hint1, hint2))
    val qg2 = QueryGraph(hints = Set(hint1, hint3))
    val qg3 = QueryGraph(hints = Set(hint1, hint2, hint3))

    qg1 ++ qg2 should equal(qg3)
  }
}
