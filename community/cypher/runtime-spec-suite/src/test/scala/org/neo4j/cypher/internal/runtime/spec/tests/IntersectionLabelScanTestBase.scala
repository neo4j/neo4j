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

abstract class IntersectionLabelScanTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should scan all nodes of a label") {
    // given
    val nodes = givenGraph {
      nodeGraph(sizeHint, "Butter")
      nodeGraph(sizeHint, "Almond")
      nodeGraph(sizeHint, "Honey")
      nodeGraph(sizeHint, "Butter", "Almond", "Honey")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .intersectionNodeByLabelsScan("x", Seq("Honey", "Almond", "Butter"), IndexOrderNone)
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
      nodeGraph(sizeHint, "Butter")
      nodeGraph(sizeHint, "Almond")
      nodeGraph(sizeHint, "Honey")
      nodeGraph(sizeHint, "Butter", "Almond", "Honey")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .intersectionNodeByLabelsScan("x", Seq("Honey", "Almond", "Butter"), IndexOrderAscending)
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
      nodeGraph(sizeHint, "Butter")
      nodeGraph(sizeHint, "Almond")
      nodeGraph(sizeHint, "Honey")
      nodeGraph(sizeHint, "Butter", "Almond", "Honey")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .intersectionNodeByLabelsScan("x", Seq("Honey", "Almond", "Butter"), IndexOrderDescending)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumnInOrder(nodes.sortBy(_.getId * -1)))
  }

  test("should scan empty graph") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .intersectionNodeByLabelsScan("x", Seq("Honey", "Almond", "Butter"), IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withNoRows()
  }

  test("should handle multiple scans") {
    // given
    val nodes = givenGraph { nodeGraph(10, "Honey", "Almond", "Butter") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "z", "x")
      .apply()
      .|.filter("true")
      .|.intersectionNodeByLabelsScan("x", Seq("Honey", "Almond", "Butter"), IndexOrderNone)
      .apply()
      .|.filter("true")
      .|.intersectionNodeByLabelsScan("y", Seq("Honey", "Almond", "Butter"), IndexOrderNone)
      .filter("true")
      .intersectionNodeByLabelsScan("z", Seq("Honey", "Almond", "Butter"), IndexOrderNone)
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
      .filter("true")
      .intersectionNodeByLabelsScan("x", Seq("Honey", "Almond", "Butter"), IndexOrderNone)
      .build()

    // empty db
    val executablePlan = buildPlan(logicalQuery, runtime)
    execute(executablePlan) should beColumns("x").withNoRows()

    // CREATE Almond
    givenGraph(nodeGraph(sizeHint, "Almond"))
    execute(executablePlan) should beColumns("x").withNoRows()

    // CREATE Almond, Honey
    givenGraph(nodeGraph(sizeHint, "Almond", "Honey"))
    execute(executablePlan) should beColumns("x").withNoRows()

    // / CREATE Almond, Honey, Butter
    givenGraph(nodeGraph(sizeHint, "Almond", "Honey", "Butter"))
    execute(executablePlan) should beColumns("x").withRows(rowCount(sizeHint))
  }

  test("scan on the RHS of apply") {
    // given
    val (abNodes, cdNodes) = givenGraph {
      val abNodes = nodeGraph(sizeHint / 2, "A", "B")
      val cdNodes = nodeGraph(sizeHint / 2, "C", "D")
      (abNodes, cdNodes)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .apply()
      .|.filter("true")
      .|.intersectionNodeByLabelsScan("y", Seq("C", "D"), IndexOrderNone, "x")
      .filter("true")
      .intersectionNodeByLabelsScan("x", Seq("A", "B"), IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for { ab <- abNodes; cd <- cdNodes } yield Array(ab, cd)
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("scan on the RHS of union") {
    // given
    val (abNodes, cdNodes) = givenGraph {
      val abNodes = nodeGraph(sizeHint, "A", "B")
      val cdNodes = nodeGraph(sizeHint, "C", "D")
      (abNodes, cdNodes)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .union()
      .|.filter("true")
      .|.intersectionNodeByLabelsScan("x", Seq("C", "D"), IndexOrderNone, "x")
      .filter("true")
      .intersectionNodeByLabelsScan("x", Seq("A", "B"), IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(abNodes ++ cdNodes))
  }

  test("scan on the RHS of cartesian product") {
    // given
    val (abNodes, cdNodes) = givenGraph {
      val abNodes = nodeGraph(sizeHint / 2, "A", "B")
      val cdNodes = nodeGraph(sizeHint / 2, "C", "D")
      (abNodes, cdNodes)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .cartesianProduct()
      .|.filter("true")
      .|.intersectionNodeByLabelsScan("y", Seq("C", "D"), IndexOrderNone, "x")
      .filter("true")
      .intersectionNodeByLabelsScan("x", Seq("A", "B"), IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for { ab <- abNodes; cd <- cdNodes } yield Array(ab, cd)
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("join two scans") {
    // given
    val expected = givenGraph {
      val abNodes = nodeGraph(sizeHint, "A", "B")
      nodeGraph(sizeHint, "B", "C")
      abNodes.zipWithIndex.foreach {
        case (node, i) if i % 2 == 0 => node.addLabel(Label.label("C"))
        case _                       => // do nothing
      }
      abNodes.filter(_.hasLabel(Label.label("C")))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeHashJoin("x")
      .|.filter("true")
      .|.intersectionNodeByLabelsScan("x", Seq("B", "C"), IndexOrderNone, "x")
      .filter("true")
      .intersectionNodeByLabelsScan("x", Seq("A", "B"), IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(expected))
  }

  test("scan + aggregation") {
    givenGraph {
      nodeGraph(sizeHint, "A")
      nodeGraph(sizeHint, "A", "B")
      nodeGraph(sizeHint, "A", "B", "C")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("count(*) AS c"))
      .filter("x:A")
      .intersectionNodeByLabelsScan("x", Seq("A", "B", "C"), IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("c").withSingleRow(sizeHint)
  }

  test("scan on the RHS of apply with limit") {
    // given
    val (abNodes, cdNodes) = givenGraph {
      val abNodes = nodeGraph(sizeHint, "A", "B")
      val cdNodes = nodeGraph(sizeHint, "C", "D")
      (abNodes, cdNodes)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .apply()
      .|.limit(10)
      .|.filter("true")
      .|.intersectionNodeByLabelsScan("y", Seq("C", "D"), IndexOrderNone, "x")
      .limit(10)
      .filter("true")
      .intersectionNodeByLabelsScan("x", Seq("A", "B"), IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "y").withRows(rowCount(10 * 10))
  }
}
