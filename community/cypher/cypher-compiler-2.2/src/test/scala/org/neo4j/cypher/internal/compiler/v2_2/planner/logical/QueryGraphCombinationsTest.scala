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
import org.neo4j.cypher.internal.compiler.v2_2.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.{SimplePatternLength, PatternRelationship, IdName}
import org.neo4j.graphdb.Direction

class QueryGraphCombinationsTest extends CypherFunSuite {
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

  val singleNode = (
    "MATCH a",
    QueryGraph(
      patternNodes = Set(A)))

  val cartesianProduct = (
    "MATCH a, b",
    QueryGraph(
      patternNodes = Set(A, B)))

  val singleRel = (
    "MATCH a-[r1]->b",
    QueryGraph(
      patternNodes = Set(A, B),
      patternRelationships = Set(R1)))

  val selfLoop = (
    "MATCH a-[r4]->a",
    QueryGraph(
      patternNodes = Set(A, A),
      patternRelationships = Set(R4)))

  val twoRels = (
    "MATCH a-[r1]->b-[r2]->c",
    QueryGraph(
      patternNodes = Set(A, B, C),
      patternRelationships = Set(R1, R2)
    ))

  val twoRelsWithLoop = (
    "MATCH a-[r1]->b-[r3]->a",
    QueryGraph(
      patternNodes = Set(A, B),
      patternRelationships = Set(R1, R3)
    ))

  val starPattern = (
    "MATCH a-[r5]->x, b-[r6]->x, c-[r7]->x",
    QueryGraph(
      patternNodes = Set(A, B, C, X),
      patternRelationships = Set(R5, R6, R7)
    ))

  val tests = Seq(
    // pattern,  size          expectedResult
    (singleNode,       0, Set(
      QueryGraph(patternNodes = Set(A)))),

    (cartesianProduct, 0, Set(
      QueryGraph(patternNodes = Set(A)),
      QueryGraph(patternNodes = Set(B)))),

    (singleRel,        0, Set(
      QueryGraph(patternNodes = Set(A)),
      QueryGraph(patternNodes = Set(B)))),

    (selfLoop,         0, Set(
      QueryGraph(patternNodes = Set(A)))),

    (twoRels,          0, Set(
      QueryGraph(patternNodes = Set(A)),
      QueryGraph(patternNodes = Set(B)),
      QueryGraph(patternNodes = Set(C)))),

    (twoRelsWithLoop,  0, Set(
      QueryGraph(patternNodes = Set(A)),
      QueryGraph(patternNodes = Set(B)))),

    (starPattern,      0, Set(
      QueryGraph(patternNodes = Set(A)),
      QueryGraph(patternNodes = Set(B)),
      QueryGraph(patternNodes = Set(C)),
      QueryGraph(patternNodes = Set(X)))),

    (singleRel,        1, Set(
      QueryGraph(
        patternNodes = Set(A, B),
        patternRelationships = Set(R1)))),

    (selfLoop,         1, Set(
      QueryGraph(
        patternNodes = Set(A),
        patternRelationships = Set(R4)))),

    (twoRels,          1, Set(
      QueryGraph(
        patternNodes = Set(A, B),
        patternRelationships = Set(R1)),
      QueryGraph(
        patternNodes = Set(B, C),
        patternRelationships = Set(R2)))),

    (twoRelsWithLoop,  1, Set(
      QueryGraph(
        patternNodes = Set(A, B),
        patternRelationships = Set(R1)),
      QueryGraph(
        patternNodes = Set(A, B),
        patternRelationships = Set(R3)))),

    (starPattern,      1, Set(
      QueryGraph(
        patternNodes = Set(A, X),
        patternRelationships = Set(R5)),
      QueryGraph(
        patternNodes = Set(B, X),
        patternRelationships = Set(R6)),
      QueryGraph(
        patternNodes = Set(C, X),
        patternRelationships = Set(R7)))),

    (twoRels,          2, Set(
      QueryGraph(
        patternNodes = Set(A, B, C),
        patternRelationships = Set(R1, R2)))),

    (twoRelsWithLoop,   2, Set(
      QueryGraph(
        patternNodes = Set(A, B),
        patternRelationships = Set(R1, R3)))),

    (starPattern,       2, Set(
      QueryGraph(
        patternNodes = Set(A, B, X),
        patternRelationships = Set(R5, R6)),
      QueryGraph(
        patternNodes = Set(B, C, X),
        patternRelationships = Set(R6, R7)),
      QueryGraph(
        patternNodes = Set(A, C, X),
        patternRelationships = Set(R5, R7)))),

    (starPattern,       3, Set(
      QueryGraph(
        patternNodes = Set(A, B, C, X),
        patternRelationships = Set(R5, R6, R7))))
  )

  import ExhaustiveQueryGraphSolver._

  tests.foreach {
    case ((name, qg), size, expectedResult) =>
      test("size " + size + " query " + name) {
        qg.combinations(size).toSet should equal(expectedResult)
      }
  }
}
