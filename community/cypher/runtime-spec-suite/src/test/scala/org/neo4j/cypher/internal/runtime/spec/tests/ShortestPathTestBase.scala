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
package org.neo4j.cypher.internal.runtime.spec.tests

import java.util

import org.neo4j.cypher.internal.runtime.spec._
import org.neo4j.cypher.internal.{CypherRuntime, RuntimeContext}
import org.neo4j.graphdb.Direction.{INCOMING, OUTGOING}
import org.neo4j.graphdb.Label

abstract class ShortestPathTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  protected  val sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("shortest path in a sine graph") {
    // given
    val (start, end, rels) = given {
      val g = sineGraph()
      (g.start, g.end, Seq(g.startMiddle, g.endMiddle))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y", "path")
      .shortestPath("(x)-[r*]-(y)", Some("path"))
      .cartesianProduct()
      .|.nodeByLabelScan("y", "END")
      .nodeByLabelScan("x", "START")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      Array(start, util.Arrays.asList(rels: _*), end, TestPath(start, rels))
    )

    runtimeResult should beColumns("x", "r", "y", "path").withRows(expected)
  }

  test("all shortest paths in a lollipop graph") {
    // given
    val (start, end, r1, r2, r3) = given {
      val (Seq(n1, _, n3), Seq(r1, r2, r3)) = lollipopGraph()
      n3.addLabel(Label.label("END"))
      (n1, n3, r1, r2, r3)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y", "path")
      .shortestPath("(x)-[r*]-(y)", Some("path"), all = true)
      .cartesianProduct()
      .|.nodeByLabelScan("y", "END")
      .nodeByLabelScan("x", "START")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      Array(start, util.Arrays.asList(r1, r3), end, TestPath(start, Seq(r1, r3))),
      Array(start, util.Arrays.asList(r2, r3), end, TestPath(start, Seq(r2, r3)))
    )

    runtimeResult should beColumns("x", "r", "y", "path").withRows(expected)
  }

  // EXPANSION FILTERING, RELATIONSHIP TYPE

  test("should filter on relationship type A") {
    // given
    val (start, end, rels) = given {
      val g = sineGraph()
      val r2 = g.ea1.getRelationships(INCOMING).iterator().next()
      val r3 = g.ea1.getRelationships(OUTGOING).iterator().next()
      (g.start, g.end, Seq(g.startMiddle, r2, r3))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y", "path")
      .shortestPath("(x)-[r:A*]->(y)", Some("path"))
      .cartesianProduct()
      .|.nodeByLabelScan("y", "END")
      .nodeByLabelScan("x", "START")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "r", "y", "path").withRows(Array(
      Array(start, util.Arrays.asList(rels: _*), end, TestPath(start, rels))
    ))
  }

  test("should filter on relationship type B") {
    // given
    val (start, end, rels) = given {
      val g = sineGraph()
      val r1 = g.sb1.getRelationships(INCOMING).iterator().next()
      val r2 = g.sb2.getRelationships(INCOMING).iterator().next()
      val r3 = g.sb2.getRelationships(OUTGOING).iterator().next()
      val r4 = g.eb1.getRelationships(INCOMING).iterator().next()
      val r5 = g.eb2.getRelationships(INCOMING).iterator().next()
      val r6 = g.eb2.getRelationships(OUTGOING).iterator().next()
      (g.start, g.end, Seq(r1, r2, r3, r4, r5, r6))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y", "path")
      .shortestPath("(x)-[r:B*]->(y)", Some("path"))
      .cartesianProduct()
      .|.nodeByLabelScan("y", "END")
      .nodeByLabelScan("x", "START")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "r", "y", "path").withRows(Array(
      Array(start, util.Arrays.asList(rels: _*), end, TestPath(start, rels))
    ))
  }

  // EXPANSION FILTERING, NODE AND RELATIONSHIP PREDICATE

  test("should filter on node predicate") {
    // given
    val (start, end, forbidden, rels) = given {
      val g = sineGraph()
      val r2 = g.ec1.getRelationships(INCOMING).iterator().next()
      val r3 = g.ec2.getRelationships(INCOMING).iterator().next()
      val r4 = g.ec3.getRelationships(INCOMING).iterator().next()
      val r5 = g.ec3.getRelationships(OUTGOING).iterator().next()
      (g.start, g.end, g.ea1, Seq(g.startMiddle, r2, r3, r4, r5))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y", "path")
      .shortestPath("(x)-[r:A*]->(y)", Some("path"), predicates = Seq(s"All(n in nodes(path) WHERE id(n) <> ${forbidden.getId})"))
      .cartesianProduct()
      .|.nodeByLabelScan("y", "END")
      .nodeByLabelScan("x", "START")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "r", "y", "path").withRows(Array(
      Array(start, util.Arrays.asList(rels: _*), end, TestPath(start, rels))
    ))
  }

  test("should filter on node predicate on first node") {
    // given
    val start = given {
      val g = sineGraph()
      g.start
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .shortestPath("(x)-[r:A*]->(y)", Some("path"), predicates = Seq(s"All(n in nodes(path) WHERE id(n) <> ${start.getId})"))
      .cartesianProduct()
      .|.nodeByLabelScan("y", "END")
      .nodeByLabelScan("x", "START")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y").withNoRows()
  }

  test("should filter on node predicate on first node from reference") {
    // given
    val start = given {
      val g = sineGraph()
      g.start
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .shortestPath("(X)-[r:A*]->(y)", Some("path"), predicates = Seq(s"All(n in nodes(path) WHERE id(n) <> ${start.getId})"))
      .projection("x AS X")
      .cartesianProduct()
      .|.nodeByLabelScan("y", "END")
      .nodeByLabelScan("x", "START")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y").withNoRows()
  }

  test("should filter on relationship predicate") {
    // given
    val (start, end, forbidden, rels) = given {
      val g = sineGraph()
      val r1 = g.sa1.getRelationships(INCOMING).iterator().next()
      val r2 = g.sa1.getRelationships(OUTGOING).iterator().next()
      val r3 = g.ea1.getRelationships(INCOMING).iterator().next()
      val r4 = g.ea1.getRelationships(OUTGOING).iterator().next()
      (g.start, g.end, g.startMiddle, Seq(r1, r2, r3, r4))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y", "path")
      .shortestPath("(x)-[r:A*]->(y)", Some("path"), predicates = Seq(s"All(rel in relationships(path) WHERE id(rel) <> ${forbidden.getId})")) // OBS: r != rel
      .cartesianProduct()
      .|.nodeByLabelScan("y", "END")
      .nodeByLabelScan("x", "START")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "r", "y", "path").withRows(Array(
      Array(start, util.Arrays.asList(rels: _*), end, TestPath(start, rels))
    ))
  }

}
