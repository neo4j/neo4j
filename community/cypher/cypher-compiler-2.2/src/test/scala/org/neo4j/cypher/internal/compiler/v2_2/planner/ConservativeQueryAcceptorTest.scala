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
package org.neo4j.cypher.internal.compiler.v2_2.planner

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.{SimplePatternLength, VarPatternLength, IdName, PatternRelationship}
import org.neo4j.graphdb.Direction

class ConservativeQueryAcceptorTest extends CypherFunSuite with LogicalPlanningTestSupport2 {
  test("should accept empty query graphs") {
    val qg = QueryGraph.empty

    val query = UnionQuery(Seq(PlannerQuery(graph = qg)), false)

    conservativeQueryAcceptor(query) should be(true)
  }

  test("should accept queries with no pattern") {
    // MATCH a, b
    val qg = QueryGraph(
      patternNodes = Set("a", "b"),
      patternRelationships = Set.empty)

    shouldAccept(qg)
  }

  test("should accept simple non-cyclic non-varlength queries") {
    // MATCH a-[r1]->b
    val qg = QueryGraph(
      patternNodes = Set("a", "b"),
      patternRelationships =
        Set(PatternRelationship("r", nodes = ("a", "b"),
          Direction.OUTGOING, Seq.empty, SimplePatternLength)))

    shouldAccept(qg)
  }

  test("should not accept varlength queries") {
    // MATCH a-[r1*]->b
    val qg = QueryGraph(
      patternNodes = Set("a", "b"),
      patternRelationships =
        Set(PatternRelationship("r", nodes = ("a", "b"),
          Direction.OUTGOING, Seq.empty, VarPatternLength(1, Some(5)))))

    shouldReject(qg)
  }

  test("should not accept varlength queries in tail") {
    // MATCH a-[r1]->b WITH * MATCH a-[r1*]->b
    val qg1 = QueryGraph(
      patternNodes = Set("a", "b"),
      patternRelationships =
        Set(PatternRelationship("r", nodes = ("a", "b"),
          Direction.OUTGOING, Seq.empty, SimplePatternLength)))

    val qg2 = QueryGraph(
      patternNodes = Set("a", "b"),
      patternRelationships =
        Set(PatternRelationship("r", nodes = ("a", "b"),
          Direction.OUTGOING, Seq.empty, VarPatternLength(1, Some(5)))))

    val query = UnionQuery(Seq(PlannerQuery(graph = qg1, tail = Some(PlannerQuery(graph = qg2)))), false)

    conservativeQueryAcceptor(query) should be(false)
  }

  test("should not accept queries with simple cycles in them") {
    // MATCH a-[r1]->a
    val qg1 = QueryGraph(
      patternNodes = Set("a"),
      patternRelationships =
        Set(PatternRelationship("r", nodes = ("a", "a"),
          Direction.OUTGOING, Seq.empty, SimplePatternLength)))

    shouldReject(qg1)
  }

  test("should not accept queries with cycles in them") {
    // MATCH a-->b-->c-->d-->e, c-->e, d-->b
    val qg1 = QueryGraph(
      patternNodes = Set("a", "b", "c", "d", "e"),
      patternRelationships =
        Set(PatternRelationship("r1", nodes = ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship("r2", nodes = ("b", "c"), Direction.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship("r3", nodes = ("c", "d"), Direction.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship("r4", nodes = ("c", "e"), Direction.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship("r5", nodes = ("d", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength)))

    shouldReject(qg1)
  }

  test("should not accept queries with disconnected graph with cycle") {
    // match a-->b-->c, d-->e-->d
    val qg1 = QueryGraph(
      patternNodes = Set("a", "b", "c", "d", "e"),
      patternRelationships =
        Set(PatternRelationship("r1", nodes = ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship("r2", nodes = ("b", "c"), Direction.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship("r3", nodes = ("d", "e"), Direction.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship("r4", nodes = ("e", "d"), Direction.OUTGOING, Seq.empty, SimplePatternLength)))

    shouldReject(qg1)
  }

  test("should accept queries with disconnected graph without cycle") {
    // MATCH a-->b-->c, d-->e-->f
    val qg1 = QueryGraph(
      patternNodes = Set("a", "b", "c", "d", "e", "f"),
      patternRelationships =
        Set(PatternRelationship("r1", nodes = ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship("r2", nodes = ("b", "c"), Direction.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship("r3", nodes = ("d", "e"), Direction.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship("r4", nodes = ("e", "f"), Direction.OUTGOING, Seq.empty, SimplePatternLength)))

    shouldAccept(qg1)
  }

  test("should not accept queries with overlap") {
    // MATCH a-[:T1]->b , a-[:T2]->b
    val qg1 = QueryGraph(
      patternNodes = Set("a", "b"),
      patternRelationships =
        Set(PatternRelationship("r1", nodes = ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship("r2", nodes = ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength)))

    shouldReject(qg1)
  }

  test("should accept queries that have star patterns") {
    // MATCH a-[:T1]->b , a-[:T2]->c
    val qg1 = QueryGraph(
      patternNodes = Set("a", "b", "c"),
      patternRelationships =
        Set(
          PatternRelationship("r1", nodes = ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship("r2", nodes = ("a", "c"), Direction.OUTGOING, Seq.empty, SimplePatternLength)))

    shouldAccept(qg1)
  }

  test("should accept long paths") {
    // MATCH a-[:T1]->b-[:T2]->c-[:T3]->d-[:T4]->e
    val qg1 = QueryGraph(
      patternNodes = Set("foo", "bar", "baz", "qux", "quux"),
      patternRelationships =
        Set(
          PatternRelationship("r3", nodes = ("baz", "qux"), Direction.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship("r4", nodes = ("qux", "quux"), Direction.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship("r1", nodes = ("foo", "bar"), Direction.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship("r2", nodes = ("bar", "baz"), Direction.OUTGOING, Seq.empty, SimplePatternLength)
        )
    )

    shouldAccept(qg1)
  }

  private def shouldAccept(qg: QueryGraph) {
    val query = UnionQuery(Seq(PlannerQuery(graph = qg)), false)
    conservativeQueryAcceptor(query) should be(true)
  }

  private def shouldReject(qg: QueryGraph) {
    val query = UnionQuery(Seq(PlannerQuery(graph = qg)), false)
    conservativeQueryAcceptor(query) should be(false)
  }
}
