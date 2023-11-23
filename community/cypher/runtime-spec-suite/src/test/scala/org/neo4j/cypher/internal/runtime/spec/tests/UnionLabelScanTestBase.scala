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

abstract class UnionLabelScanTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should scan all nodes of a label") {
    // given
    val nodes = givenGraph {
      nodeGraph(sizeHint, "Butter") ++
        nodeGraph(sizeHint, "Almond") ++
        nodeGraph(sizeHint, "Honey")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .unionNodeByLabelsScan("x", Seq("Honey", "Almond", "Butter"), IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(nodes))
  }

  test("should scan all nodes of a label and not produce duplicates if nodes have multiple labels") {
    // given
    val nodes = givenGraph {
      nodeGraph(sizeHint, "Butter", "Almond") ++
        nodeGraph(sizeHint, "Almond") ++
        nodeGraph(sizeHint, "Butter")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .unionNodeByLabelsScan("x", Seq("Almond", "Butter"), IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(nodes))
  }

  test("should scan all nodes of a label in ascending order") {
    // parallel does not maintain order
    assume(!isParallel)

    // given
    val nodes = givenGraph {
      nodeGraph(sizeHint, "Butter") ++
        nodeGraph(sizeHint, "Almond") ++
        nodeGraph(sizeHint, "Honey")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .unionNodeByLabelsScan("x", Seq("Honey", "Almond", "Butter"), IndexOrderAscending)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumnInOrder(nodes.sortBy(_.getId)))
  }

  test("should scan all nodes of a label in descending order") {
    // parallel does not maintain order
    assume(!isParallel)

    // given
    val nodes = givenGraph {
      nodeGraph(sizeHint, "Butter") ++
        nodeGraph(sizeHint, "Almond") ++
        nodeGraph(sizeHint, "Honey")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .unionNodeByLabelsScan("x", Seq("Honey", "Almond", "Butter"), IndexOrderDescending)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumnInOrder(nodes.sortBy(_.getId * -1)))
  }

  test("should scan empty graph") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .unionNodeByLabelsScan("x", Seq("Honey", "Almond", "Butter"), IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withNoRows()
  }

  test("should handle multiple scans") {
    // given
    val nodes = givenGraph { nodeGraph(10, "Honey") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "z", "x")
      .apply()
      .|.unionNodeByLabelsScan("x", Seq("Honey", "Almond", "Butter"), IndexOrderNone)
      .apply()
      .|.unionNodeByLabelsScan("y", Seq("Honey", "Almond", "Butter"), IndexOrderNone)
      .unionNodeByLabelsScan("z", Seq("Honey", "Almond", "Butter"), IndexOrderNone)
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
      .unionNodeByLabelsScan("x", Seq("Honey", "Almond", "Butter"), IndexOrderNone)
      .build()

    // empty db
    val executablePlan = buildPlan(logicalQuery, runtime)
    execute(executablePlan) should beColumns("x").withNoRows()

    // CREATE Almond
    givenGraph(nodeGraph(sizeHint, "Almond"))
    execute(executablePlan) should beColumns("x").withRows(rowCount(sizeHint))

    // CREATE Honey
    givenGraph(nodeGraph(sizeHint, "Honey"))
    execute(executablePlan) should beColumns("x").withRows(rowCount(2 * sizeHint))

    // CREATE Butter
    givenGraph(nodeGraph(sizeHint, "Butter"))
    execute(executablePlan) should beColumns("x").withRows(rowCount(3 * sizeHint))
  }

  test("scan on the RHS of apply") {
    // given
    val (aNodes, bNodes, cNodes, dNodes) = givenGraph {
      val aNodes = nodeGraph(10, "A")
      val bNodes = nodeGraph(10, "B")
      val cNodes = nodeGraph(10, "C")
      val dNodes = nodeGraph(10, "D")
      (aNodes, bNodes, cNodes, dNodes)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .apply()
      .|.unionNodeByLabelsScan("y", Seq("C", "D"), IndexOrderNone, "x")
      .unionNodeByLabelsScan("x", Seq("A", "B"), IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for { a <- aNodes ++ bNodes; b <- cNodes ++ dNodes } yield Array(a, b)
    runtimeResult should beColumns("x", "y").withRows(expected)
  }
}
