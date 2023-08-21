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
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.graphdb.Node

import scala.jdk.CollectionConverters.IterableHasAsScala

abstract class Top1WithTiesTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("empty input gives empty output") {
    // when
    val input = inputValues()

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .top1WithTies("x ASC", "y ASC")
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test("should handle null values, one column") {
    val unfilteredNodes = given { nodeGraph(sizeHint) }
    val nodes = select(unfilteredNodes, nullProbability = 0.52)
    val input = inputValues(nodes.flatMap(n => Seq.fill(5)(Array[Any](n))): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .top1WithTies("x ASC")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = nodes.minBy(n => if (n == null) Long.MaxValue else n.getId)
    runtimeResult should beColumns("x").withRows(singleColumn(Seq.fill(5)(expected)))
  }

  test("should top many columns") {
    val input = inputValues((0 until sizeHint).map(i => Array[Any](i % 2, i % 4, i % 6)): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b", "c")
      .top1WithTies("a DESC", "b ASC", "c DESC")
      .input(variables = Seq("a", "b", "c"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val sorted =
      input.flatten.sortBy(arr => (-arr(0).asInstanceOf[Int], arr(1).asInstanceOf[Int], -arr(2).asInstanceOf[Int]))
    val expected = sorted.takeWhile(_.toSeq == sorted.head.toSeq)
    runtimeResult should beColumns("a", "b", "c").withRows(inOrder(expected))
  }

  test("should top under apply") {
    val nodesPerLabel = 100
    val (aNodes, bNodes) = given { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .apply()
      .|.top1WithTies("b ASC")
      .|.expandAll("(a)-->(b)")
      .|.argument("a")
      .allNodeScan("a")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      x <- aNodes
      y = bNodes.minBy(_.getId)
    } yield Array[Any](x, y)

    runtimeResult should beColumns("a", "b").withRows(expected)
  }

  test("should return all tied rows") {
    val input = inputValues((0 until sizeHint).flatMap(i => Seq.fill(5)(Array[Any](i))): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a")
      .top1WithTies("a ASC")
      .input(variables = Seq("a"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = input.flatten.sortBy(arr => arr(0).asInstanceOf[Int]).take(5)
    runtimeResult should beColumns("a").withRows(inOrder(expected))
  }

  test("should top twice in a row") {
    // given
    val nodes = given { nodeGraph(1000) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .top1WithTies("x ASC")
      .top1WithTies("x DESC")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.maxBy(_.getId)

    runtimeResult should beColumns("x").withSingleRow(expected)
  }

  test("should top on top of apply with expand and top on rhs of apply") {
    // given
    val nodesPerLabel = 10
    val (aNodes, bNodes) = given { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .top1WithTies("y ASC")
      .apply()
      .|.top1WithTies("y DESC")
      .|.expand("(x)-[:R]->(y)")
      .|.argument("x")
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val expected = bNodes.maxBy(_.getId)

    runtimeResult should beColumns("y").withRows(Seq.fill(nodesPerLabel)(Array[Any](expected)))
  }

  test("should top on top of apply") {
    // given
    val nodeCount = 10
    val (nodes, _) = given { circleGraph(nodeCount) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .top1WithTies("y DESC", "x ASC")
      .apply()
      .|.expandAll("(x)--(y)")
      .|.argument()
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then

    val allRows = for {
      x <- nodes
      rel <- x.getRelationships().asScala
      y = rel.getOtherNode(x)
    } yield Array[Any](x, y)

    val expected = allRows.minBy(row => (-row(1).asInstanceOf[Node].getId, row(0).asInstanceOf[Node].getId))

    runtimeResult should beColumns("x", "y").withSingleRow(expected: _*)
  }

  test("should apply apply top") {
    // given
    val nodesPerLabel = 100
    val (aNodes, _) = given { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z")
      .apply()
      .|.apply()
      .|.|.top1WithTies("z ASC")
      .|.|.expandAll("(y)--(z)")
      .|.|.argument()
      .|.top1WithTies("y DESC")
      .|.expandAll("(x)--(y)")
      .|.argument()
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    def outerTop(from: Node): (Node, Node) = {
      (for {
        rel <- from.getRelationships().asScala.toSeq
        to = rel.getOtherNode(from)
      } yield (from, to)).minBy(tuple => -tuple._2.getId)
    }

    def innerTop(fromNode: Node): Node = {
      (for {
        rel <- fromNode.getRelationships().asScala.toSeq
        to = rel.getOtherNode(fromNode)
      } yield to).minBy(_.getId)
    }

    val expected = for {
      n <- aNodes
      (x, y) = outerTop(n)
      z = innerTop(y.asInstanceOf[Node])
    } yield Array[Any](x, y, z)

    // then
    runtimeResult should beColumns("x", "y", "z").withRows(expected)
  }
}
