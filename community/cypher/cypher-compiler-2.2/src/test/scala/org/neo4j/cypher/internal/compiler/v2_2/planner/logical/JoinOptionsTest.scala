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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.planner.{QueryGraph, LogicalPlanningTestSupport}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.{SimplePatternLength, PatternRelationship, IdName}
import org.neo4j.graphdb.Direction

class JoinOptionsTest extends CypherFunSuite with LogicalPlanningTestSupport {

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

  test("match a-[r1]->b-[r2]->c") {
    // GIVEN
    val qg = QueryGraph(patternNodes = Set(A, B, C), patternRelationships = Set(R1, R2))
    val lpR1 = newMockedLogicalPlanWithPatterns(Set("a", "b"), Seq(R1))
    val lpR2 = newMockedLogicalPlanWithPatterns(Set("b", "c"), Seq(R2))
    val cache = ExhaustivePlanTable(lpR1, lpR2)

    // WHEN
    val solutions = joinOptions(qg, cache)

    // THEN
    solutions should equal(Seq(
      planNodeHashJoin(Set(B), lpR1, lpR2),
      planNodeHashJoin(Set(B), lpR2, lpR1)
    ))
  }

  test("match a-[r5]->x, b-[r6]->x, c-[r7]->x") {
    // GIVEN
    val qg = QueryGraph(patternNodes = Set(A, B, C, X), patternRelationships = Set(R5, R6, R7))

    val lpR5 = newMockedLogicalPlanWithPatterns(Set("a", "x"), Seq(R5))
    val lpR6 = newMockedLogicalPlanWithPatterns(Set("b", "x"), Seq(R6))
    val lpR7 = newMockedLogicalPlanWithPatterns(Set("c", "x"), Seq(R7))
    val lpR5R6 = newMockedLogicalPlanWithPatterns(Set("a", "b", "x"), Seq(R5,R6))
    val lpR5R7 = newMockedLogicalPlanWithPatterns(Set("a", "c", "x"), Seq(R5,R7))
    val lpR6R7 = newMockedLogicalPlanWithPatterns(Set("b", "c", "x"), Seq(R6,R7))
    val cache = ExhaustivePlanTable(lpR5, lpR6, lpR7, lpR5R6, lpR5R7, lpR6R7)

    // WHEN
    val solutions = joinOptions(qg, cache)

    // THEN
    solutions.toSet should equal(Set(
      planNodeHashJoin(Set(X), lpR5, lpR6R7),
      planNodeHashJoin(Set(X), lpR6, lpR5R7),
      planNodeHashJoin(Set(X), lpR7, lpR5R6),
      planNodeHashJoin(Set(X), lpR6R7, lpR5),
      planNodeHashJoin(Set(X), lpR5R7, lpR6),
      planNodeHashJoin(Set(X), lpR5R6, lpR7)
    ))
  }
}
