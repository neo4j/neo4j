/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_3.planner

import org.neo4j.cypher.internal.frontend.v3_4.SemanticDirection.BOTH
import org.neo4j.cypher.internal.frontend.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.ir.v3_3.{IdName, PatternRelationship, QueryGraph, SimplePatternLength}

class QueryGraphTest extends CypherFunSuite {
  val x = IdName("x")
  val n = IdName("n")
  val m = IdName("m")
  val c = IdName("c")
  val r1 = IdName("r1")
  val r2 = IdName("r2")
  val r3 = IdName("r3")

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

  test("finds shortest path starting from a single element with a single node in the QG") {
    val qg = QueryGraph(patternNodes = Set(n))

    qg.smallestGraphIncluding(Set(n)) should equal(Set(n))
  }

  test("finds shortest path starting from a single element with a single relationship in the QG") {
    val r = PatternRelationship(r1, (n, m), BOTH, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(patternRelationships = Set(r), patternNodes = Set(n, m))

    qg.smallestGraphIncluding(Set(n)) should equal(Set(n))
  }

  test("finds shortest path starting from two nodes with a single relationship in the QG") {
    val r = PatternRelationship(r1, (n, m), BOTH, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(patternRelationships = Set(r), patternNodes = Set(n, m))

    qg.smallestGraphIncluding(Set(n, m)) should equal(Set(n, m, r1))
  }

  test("finds shortest path starting from two nodes with two relationships in the QG") {
    val pattRel1 = PatternRelationship(r1, (n, m), BOTH, Seq.empty, SimplePatternLength)
    val pattRel2 = PatternRelationship(r2, (m, c), BOTH, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(patternRelationships = Set(pattRel1, pattRel2), patternNodes = Set(n, m, c))

    qg.smallestGraphIncluding(Set(n, m)) should equal(Set(n, m, r1))
  }

  test("finds shortest path starting from two nodes with two relationships between the same nodes in the QG") {
    val pattRel1 = PatternRelationship(r1, (n, m), BOTH, Seq.empty, SimplePatternLength)
    val pattRel2 = PatternRelationship(r2, (n, m), BOTH, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(patternRelationships = Set(pattRel1, pattRel2), patternNodes = Set(n, m))

    val result = qg.smallestGraphIncluding(Set(n, m))
    result should contain(n)
    result should contain(m)
    result should contain oneOf (r1, r2)
  }

  test("finds shortest path starting from two nodes with an intermediate relationship in the QG") {
    val pattRel1 = PatternRelationship(r1, (n, m), BOTH, Seq.empty, SimplePatternLength)
    val pattRel2 = PatternRelationship(r2, (m, c), BOTH, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(patternRelationships = Set(pattRel1, pattRel2), patternNodes = Set(n, m, c))

    qg.smallestGraphIncluding(Set(n, c)) should equal(
      Set(n, m, c, r1, r2))
  }

  test("find smallest graph that connect three nodes") { // MATCH (n)-[r1]-(m), (n)-[r2]->(c), (n)-[r3]->(x)
    val pattRel1 = PatternRelationship(r1, (n, m), BOTH, Seq.empty, SimplePatternLength)
    val pattRel2 = PatternRelationship(r2, (n, c), BOTH, Seq.empty, SimplePatternLength)
    val pattRel3 = PatternRelationship(r3, (n, x), BOTH, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(
      patternRelationships = Set(pattRel1, pattRel2, pattRel3),
      patternNodes = Set(n, m, c, x))

    qg.smallestGraphIncluding(Set(n, m, c)) should equal(
      Set(n, m, c, r1, r2))
  }

  test("querygraphs containing only nodes") {
    val qg = QueryGraph(patternNodes = Set(n, m))

    qg.smallestGraphIncluding(Set(n, m)) should equal(Set(n, m))
  }
}
