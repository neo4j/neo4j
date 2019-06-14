/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.cypher.internal.runtime.spec._
import org.neo4j.cypher.internal.{CypherRuntime, RuntimeContext}

abstract class RelationshipCountFromCountStoreTestBase[CONTEXT <: RuntimeContext](
                                                                                   edition: Edition[CONTEXT],
                                                                                   runtime: CypherRuntime[CONTEXT],
                                                                                   sizeHint: Int
                                                                                 ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {
  test("should get count when both wildcard labels") {
    // given
    val (aNodes1, bNodes1) = bipartiteGraph(sizeHint, "LabelA", "LabelB", "RelType")
    val (aNodes2, bNodes2) = bipartiteGraph(sizeHint, "LabelC", "LabelD", "RelType")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .relationshipCountFromCountStore("x", None, List("RelType"), None)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expectedCount = aNodes1.size * bNodes1.size + aNodes2.size * bNodes2.size
    runtimeResult should beColumns("x").withRows(singleColumn(Seq(expectedCount)))
  }

  test("should get count for single relationship type and start label") {
    // given
    val (aNodes, bNodes) = bipartiteGraph(sizeHint, "LabelA", "LabelB", "RelType")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .relationshipCountFromCountStore("x", Some("LabelA"), List("RelType"), None)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(Seq(aNodes.size * bNodes.size)))
  }

  test("should get count for single relationship type and end label") {
    // given
    val (aNodes, bNodes) = bipartiteGraph(sizeHint, "LabelA", "LabelB", "RelType")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .relationshipCountFromCountStore("x", None, List("RelType"), Some("LabelB"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(Seq(aNodes.size * bNodes.size)))
  }

  test("should get count for wildcard relationship type and one label") {
    // given
    val (aNodes, bNodes) = bipartiteGraph(sizeHint, "LabelA", "LabelB", "RelType")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .relationshipCountFromCountStore("x", Some("LabelA"), List(), None)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(Seq(aNodes.size * bNodes.size)))
  }

  test("should work on rhs of apply") {
    // given
    val (aNodes, bNodes) = bipartiteGraph(sizeHint, "LabelA", "LabelB", "RelType")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("x > 0")
      .apply()
      .|.relationshipCountFromCountStore("x", Some("LabelA"), List(), None)
      .nodeByLabelScan("n", "LabelA")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expectedCount: Long = aNodes.size * bNodes.size
    val expectedRows = aNodes.map(_ => expectedCount)
    runtimeResult should beColumns("x").withRows(singleColumn(expectedRows))
  }

  test("should get count for multiple relationship types and one provided label") {
    // given
    val (aNodes1, bNodes1) = bipartiteGraph(sizeHint, "LabelA", "LabelB", "RelType1")
    val (aNodes2, bNodes2) = bipartiteGraph(sizeHint, "LabelA", "LabelC", "RelType2")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .relationshipCountFromCountStore("x", Some("LabelA"), List("RelType1", "RelType2"), None)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expectedCount = aNodes1.size * bNodes1.size + aNodes2.size * bNodes2.size
    runtimeResult should beColumns("x").withRows(singleColumn(Seq(expectedCount)))
  }

  test("should return zero for count of non-existent label") {
    // given
    bipartiteGraph(sizeHint, "LabelA", "LabelB", "RelType")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .relationshipCountFromCountStore("x", Some("NonExistent"), List("RelType"), None)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(Seq(0)))
  }

  test("should get count when all wildcards") {
    // given
    val (aNodes, bNodes) = bipartiteGraph(sizeHint, "LabelA", "LabelB", "RelType")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .relationshipCountFromCountStore("x", None, List(), None)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(Seq(aNodes.size * bNodes.size)))
  }

  test("should return zero for count of non-existent relationship type") {
    // given
    bipartiteGraph(sizeHint, "LabelA", "LabelB", "RelType")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .relationshipCountFromCountStore("x", None, List("NonExistent"), None)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(Seq(0)))
  }
}
