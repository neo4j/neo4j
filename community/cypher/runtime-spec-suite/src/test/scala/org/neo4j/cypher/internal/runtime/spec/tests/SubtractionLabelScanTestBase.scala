/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.spec.tests

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderDescending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.graphdb.Label

abstract class SubtractionLabelScanTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should scan all nodes of a label") {
    // given
    val justANodes = givenGraph {
      nodeGraph(sizeHint, "A", "B", "C")
      nodeGraph(sizeHint, "A", "B")
      nodeGraph(sizeHint, "A")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .subtractionNodeByLabelsScan("x", "A", "B", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(justANodes))
  }

  test("should scan nodes in ascending order") {
    // parallel does not maintain order
    assume(!isParallel)

    // given
    val justANodes = givenGraph {
      nodeGraph(sizeHint, "A", "B", "C")
      nodeGraph(sizeHint, "A", "B")
      nodeGraph(sizeHint, "A")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .subtractionNodeByLabelsScan("x", "A", "B", IndexOrderAscending)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumnInOrder(justANodes.sortBy(_.getId)))
  }

  test("should scan nodes in descending order") {
    // parallel does not maintain order
    assume(!isParallel)

    // given
    val justANodes = givenGraph {
      nodeGraph(sizeHint, "A", "B", "C")
      nodeGraph(sizeHint, "A", "B")
      nodeGraph(sizeHint, "A")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .subtractionNodeByLabelsScan("x", "A", "B", IndexOrderDescending)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumnInOrder(justANodes.sortBy(_.getId * -1)))
  }

  test("should scan empty graph") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .subtractionNodeByLabelsScan("x", "A", "B", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withNoRows()
  }

  test("should handle multiple scans") {
    // given
    val nodes = givenGraph {
      nodeGraph(10, "A", "B")
      val as = nodeGraph(10, "A")
      nodeGraph(10, "B")
      as
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "z", "x")
      .apply()
      .|.subtractionNodeByLabelsScan("x", "A", "B", IndexOrderNone)
      .apply()
      .|.subtractionNodeByLabelsScan("y", "A", "B", IndexOrderNone)
      .subtractionNodeByLabelsScan("z", "A", "B", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for { x <- nodes; y <- nodes; z <- nodes } yield Array(y, z, x)
    runtimeResult should beColumns("y", "z", "x").withRows(expected)
  }

  test("should handle non-existing labels") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .subtractionNodeByLabelsScan("x", "A", "B", IndexOrderNone)
      .build()

    // empty db
    val executablePlan = buildPlan(logicalQuery, runtime)
    execute(executablePlan) should beColumns("x").withNoRows()

    // CREATE As
    val as = givenGraph(nodeGraph(sizeHint, "A"))
    execute(executablePlan) should beColumns("x").withRows(rowCount(sizeHint))

    // ADD B
    givenGraph(as.foreach(_.addLabel(Label.label("B"))))

    execute(executablePlan) should beColumns("x").withNoRows()
  }

  test("scan on the RHS of apply") {
    // given
    val (aAndNoBNodes, cAndNoDNodes) = givenGraph {
      val aNodes = nodeGraph(10, "A")
      val abNodes = nodeGraph(10, "A", "B")
      val acNodes = nodeGraph(10, "A", "C")
      val adNodes = nodeGraph(10, "A", "D")
      val abcNodes = nodeGraph(10, "A", "B", "C")
      val acdNodes = nodeGraph(10, "A", "C", "D")
      val abcdNodes = nodeGraph(10, "A", "B", "C", "D")
      val bNodes = nodeGraph(10, "B")
      val bcNodes = nodeGraph(10, "B", "C")
      val bdNodes = nodeGraph(10, "B", "D")
      val bcdNodes = nodeGraph(10, "B", "C", "D")
      val cNodes = nodeGraph(10, "C")
      val cdNodes = nodeGraph(10, "C", "D")
      val dNodes = nodeGraph(10, "D")
      (aNodes ++ acNodes ++ adNodes ++ acdNodes, acNodes ++ abcNodes ++ bcNodes ++ cNodes)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .apply()
      .|.subtractionNodeByLabelsScan("y", "C", "D", IndexOrderNone, "x")
      .subtractionNodeByLabelsScan("x", "A", "B", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for { a <- aAndNoBNodes; b <- cAndNoDNodes } yield Array(a, b)
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should handle multiple labels") {
    // given
    val justABNodes = givenGraph {
      nodeGraph(sizeHint, "A", "B", "C", "X", "Y")
      nodeGraph(sizeHint, "A")
      nodeGraph(sizeHint, "A", "B") ++ nodeGraph(sizeHint, "A", "B", "X") ++ nodeGraph(sizeHint, "A", "B", "Y")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .subtractionNodeByLabelsScan("x", Seq("A", "B"), Seq("X", "Y"), IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(justABNodes))
  }
}
