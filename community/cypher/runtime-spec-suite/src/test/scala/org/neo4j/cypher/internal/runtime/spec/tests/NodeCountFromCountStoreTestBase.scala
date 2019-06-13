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

abstract class NodeCountFromCountStoreTestBase[CONTEXT <: RuntimeContext](
                                                                           edition: Edition[CONTEXT],
                                                                           runtime: CypherRuntime[CONTEXT],
                                                                           sizeHint: Int
                                                                         ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {
  test("should get count for single label node") {
    // given
    val (aNodes, _) = bipartiteGraph(sizeHint, "LabelA", "LabelB", "RelType")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeCountFromCountStore("x", List(Some("LabelA")))
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(Seq(aNodes.size)))
  }

  test("should get count for label wildcard") {
    // given
    val (aNodes, bNodes) = bipartiteGraph(sizeHint, "LabelA", "LabelB", "RelType")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeCountFromCountStore("x", List(None))
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(Seq(aNodes.size + bNodes.size)))
  }

  test("should get count for non-existent label") {
    // given
    val (_, _) = bipartiteGraph(sizeHint, "LabelA", "LabelB", "RelType")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeCountFromCountStore("x", List(Some("NoLabel")))
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(Seq(0)))
  }

  test("should get count when one label is non-existent") {
    // given
    val (_, _) = bipartiteGraph(sizeHint, "LabelA", "LabelB", "RelType")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeCountFromCountStore("x", List(Some("LabelA"), Some("NoLabel")))
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(Seq(0)))
  }

  test("should get count for cartesian product of labels") {
    // given
    val (aNodes, bNodes) = bipartiteGraph(sizeHint, "LabelA", "LabelB", "RelType")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeCountFromCountStore("x", List(Some("LabelA"), Some("LabelB")))
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(Seq(aNodes.size * bNodes.size)))
  }

  test("should work when followed by other operators") {
    // given
    val (aNodes, bNodes) = bipartiteGraph(sizeHint, "LabelA", "LabelB", "RelType")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("x > 0")
      .nodeCountFromCountStore("x", List(Some("LabelA"), Some("LabelB")))
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(Seq(aNodes.size * bNodes.size)))
  }
}
