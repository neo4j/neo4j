/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.planner.{QueryGraph, LogicalPlanningTestSupport}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.graphdb.Direction

class ExpandOptionsTest extends CypherFunSuite with LogicalPlanningTestSupport {

  import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.LogicalPlanProducer._

  val A = IdName("a")
  val B = IdName("b")
  val C = IdName("c")
  val X = IdName("x")
  val R1 = PatternRelationship(IdName("r1"), (A, B), Direction.OUTGOING, Seq.empty, SimplePatternLength)
  val R2 = PatternRelationship(IdName("r2"), (B, C), Direction.OUTGOING, Seq.empty, SimplePatternLength)
  val R3 = PatternRelationship(IdName("r3"), (B, A), Direction.OUTGOING, Seq.empty, SimplePatternLength)
  val R4 = PatternRelationship(IdName("r4"), (A, A), Direction.OUTGOING, Seq.empty, SimplePatternLength)
  val R5 = PatternRelationship(IdName("r5"), (A, X), Direction.OUTGOING, Seq.empty, SimplePatternLength)
  val R6 = PatternRelationship(IdName("r6"), (B, X), Direction.OUTGOING, Seq.empty, SimplePatternLength)
  val R7 = PatternRelationship(IdName("r7"), (C, X), Direction.OUTGOING, Seq.empty, SimplePatternLength)

  test("match a-[r1]->b") {
    // GIVEN
    val qg = QueryGraph(patternNodes = Set(A, B), patternRelationships = Set(R1))
    val qgA = QueryGraph(patternNodes = Set(A))
    val qgB = QueryGraph(patternNodes = Set(B))
    val lpA = newMockedLogicalPlan("a")
    val lpB = newMockedLogicalPlan("b")
    val cache = Map(qgA -> lpA, qgB -> lpB)

    // WHEN
    val solutions = expandOptions(qg, cache)

    // THEN
    solutions should equal(Seq(
      planSimpleExpand(lpA, A, Direction.OUTGOING, B, R1, ExpandAll),
      planSimpleExpand(lpB, B, Direction.INCOMING, A, R1, ExpandAll)
    ))
  }

  test("match a-[r1]->a") {
    // GIVEN
    val qg = QueryGraph(patternNodes = Set(A, A), patternRelationships = Set(R4))
    val qgA = QueryGraph(patternNodes = Set(A))
    val lpA = newMockedLogicalPlan("a")
    val cache = Map(qgA -> lpA)

    // WHEN
    val solutions = expandOptions(qg, cache)

    // THEN
    solutions should equal(Seq(
      planSimpleExpand(lpA, A, Direction.OUTGOING, A, R4, ExpandInto)
    ))
  }

  test("match a-[r1]->b-[r2]->c") {
    // GIVEN
    val qg = QueryGraph(patternNodes = Set(A, B, C), patternRelationships = Set(R1, R2))
    val qgR1 = QueryGraph(patternNodes = Set(A, B), patternRelationships = Set(R1))
    val qgR2 = QueryGraph(patternNodes = Set(B, C), patternRelationships = Set(R2))
    val lpR1 = newMockedLogicalPlan("a", "r1", "b")
    val lpR2 = newMockedLogicalPlan("b", "r2", "c")
    val cache = Map(qgR1 -> lpR1, qgR2 -> lpR2)

    // WHEN
    val solutions = expandOptions(qg, cache)

    // THEN
    solutions should equal(Seq(
      planSimpleExpand(lpR1, B, Direction.OUTGOING, C, R2, ExpandAll),
      planSimpleExpand(lpR2, B, Direction.INCOMING, A, R1, ExpandAll)
    ))
  }

}
