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
import org.neo4j.cypher.internal.compiler.v2_2.ast.Equals
import org.neo4j.cypher.internal.compiler.v2_2.planner.{LogicalPlanningTestSupport, Selections, QueryGraph}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.{SimplePatternLength, PatternRelationship, IdName}
import org.neo4j.graphdb.Direction

import scala.util.Random

class QueryGraphDifferenceTest extends CypherFunSuite with LogicalPlanningTestSupport {
  val A = IdName("a")
  val B = IdName("b")
  val C = IdName("c")
  val D = IdName("d")
  val X = IdName("x")
  val R1 = PatternRelationship(IdName("r1"), (A, B), Direction.OUTGOING, Seq.empty, SimplePatternLength)
  val R2 = PatternRelationship(IdName("r2"), (B, C), Direction.OUTGOING, Seq.empty, SimplePatternLength)
  val R3 = PatternRelationship(IdName("r3"), (B, A), Direction.OUTGOING, Seq.empty, SimplePatternLength)
  val R4 = PatternRelationship(IdName("r4"), (A, A), Direction.OUTGOING, Seq.empty, SimplePatternLength)
  val R5 = PatternRelationship(IdName("r5"), (A, X), Direction.OUTGOING, Seq.empty, SimplePatternLength)
  val R6 = PatternRelationship(IdName("r6"), (B, X), Direction.OUTGOING, Seq.empty, SimplePatternLength)
  val R7 = PatternRelationship(IdName("r7"), (C, X), Direction.OUTGOING, Seq.empty, SimplePatternLength)
  val P1 = Equals(ident("a"), ident("b"))(pos)
  val P2 = Equals(ident("b"), ident("c"))(pos)

  val tests = Seq(
    // pattern,  size          expectedResult
    (QueryGraph(patternNodes = Set(A, B, C), patternRelationships = Set(R1, R2)),
     QueryGraph(patternNodes = Set(A, B), patternRelationships = Set(R1)),
     QueryGraph(patternNodes = Set(B, C), patternRelationships = Set(R2))),

    (QueryGraph(patternNodes = Set(A, B), patternRelationships = Set(R1, R3)),
     QueryGraph(patternNodes = Set(A, B), patternRelationships = Set(R1)),
     QueryGraph(patternNodes = Set(A, B), patternRelationships = Set(R3))),

    (QueryGraph(patternNodes = Set(A, B, C), patternRelationships = Set(R1, R2), selections = Selections.from(P1, P2)),
     QueryGraph(patternNodes = Set(A, B), patternRelationships = Set(R1), selections = Selections.from(P1)),
     QueryGraph(patternNodes = Set(B, C), patternRelationships = Set(R2), selections = Selections.from(P2)))
  )

  import ExhaustiveQueryGraphSolver._

  (tests zipWithIndex).foreach {
    case ((original, subQG1, subQG2), idx) =>
      test(idx.toString) {
        original -- subQG1 should equal(subQG2)
      }
  }
}
