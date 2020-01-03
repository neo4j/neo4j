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

import org.neo4j.cypher.internal.logical.plans.{Ascending, Descending}
import org.neo4j.cypher.internal.runtime.spec._
import org.neo4j.cypher.internal.{CypherRuntime, RuntimeContext}
import org.neo4j.graphdb.Node

import scala.collection.JavaConverters._

abstract class TopTestBase[CONTEXT <: RuntimeContext](
                                                        edition: Edition[CONTEXT],
                                                        runtime: CypherRuntime[CONTEXT],
                                                        sizeHint: Int
                                                      ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("empty input gives empty output") {
    // when
    val input = inputValues()

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .top(Seq(Ascending("x"), Ascending("y")), 10)
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test("should handle null values, one column") {
    val unfilteredNodes = given { nodeGraph(sizeHint) }
    val nodes = select(unfilteredNodes, nullProbability = 0.52)
    val limit = nodes.size - 1
    val input = inputValues(nodes.map(n => Array[Any](n)): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .top(Seq(Ascending("x")), limit)
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = nodes.sortBy(n => if (n == null) Long.MaxValue else n.getId).take(limit)
    runtimeResult should beColumns("x").withRows(inOrder(expected.map(x => Array[Any](x))))
  }

  test("should top many columns") {
    val input = inputValues((0 until sizeHint).map(i => Array[Any](i % 2, i % 4, i)): _*)
    val limit = sizeHint - 10

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b", "c")
      .top(Seq(Descending("a"), Ascending("b"), Descending("c")), limit)
      .input(variables = Seq("a", "b", "c"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = input.flatten.sortBy(arr => (-arr(0).asInstanceOf[Int], arr(1).asInstanceOf[Int], -arr(2).asInstanceOf[Int])).take(limit)
    runtimeResult should beColumns("a", "b", "c").withRows(inOrder(expected))
  }

  test("should top under apply") {
    val nodesPerLabel = 100
    val (aNodes, bNodes) = given { bipartiteGraph(nodesPerLabel, "A", "B", "R") }
    val limit = nodesPerLabel - 10

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .apply()
      .|.top(Seq(Ascending("b")), limit)
      .|.expandAll("(a)-->(b)")
      .|.argument("a")
      .allNodeScan("a")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      x <- aNodes
      y <- bNodes.sortBy(_.getId).take(limit)
    } yield Array[Any](x, y)

    runtimeResult should beColumns("a", "b").withRows(expected)
  }

  test("should return all rows when top is larger than input") {
    val input = inputValues((0 until sizeHint).map(i => Array[Any](i)): _*)
    val limit = sizeHint + 1

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a")
      .top(Seq(Ascending("a")), limit)
      .input(variables = Seq("a"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = input.flatten.sortBy(arr => arr(0).asInstanceOf[Int])
    runtimeResult should beColumns("a").withRows(inOrder(expected))
  }

  test("should handle limit 0") {
    val input = inputValues((0 until sizeHint).map(i => Array[Any](i)): _*)
    val limit = 0

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a")
      .top(Seq(Ascending("a")), limit)
      .input(variables = Seq("a"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = input.flatten.sortBy(arr => arr(0).asInstanceOf[Int])
    runtimeResult should beColumns("a").withNoRows()
  }

  test("should handle limit of Int.MaxValue") {
    val input = inputValues((0 until sizeHint).map(i => Array[Any](i)): _*)
    val limit = Int.MaxValue

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a")
      .top(Seq(Ascending("a")), limit)
      .input(variables = Seq("a"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = input.flatten.sortBy(arr => arr(0).asInstanceOf[Int])
    runtimeResult should beColumns("a").withRows(inOrder(expected))
  }

  test("should handle limit larger than Int.MaxValue") {
    val input = inputValues((0 until sizeHint).map(i => Array[Any](i)): _*)
    val limit: Long = Int.MaxValue + 1L

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a")
      .top(Seq(Ascending("a")), limit)
      .input(variables = Seq("a"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = input.flatten.sortBy(arr => arr(0).asInstanceOf[Int])
    runtimeResult should beColumns("a").withRows(inOrder(expected))
  }

  test("should top twice in a row") {
    // given
    val nodes = given { nodeGraph(1000) }
    val limit1 = 100
    val limit2 = 50

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .top(sortItems = Seq(Ascending("x")), limit2)
      .top(sortItems = Seq(Descending("x")), limit1)
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.sortBy(-_.getId).take(limit1).sortBy(_.getId).take(limit2)

    runtimeResult should beColumns("x").withRows(singleColumnInOrder(expected))
  }

  test("should top on top of apply with expand and top on rhs of apply") {
    // given
    val nodesPerLabel = 10
    val (aNodes, bNodes) = given { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    val limit1 = nodesPerLabel / 2
    val limit2 = limit1 / 2

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .top(sortItems = Seq(Ascending("y")), limit2)
      .apply()
      .|.top(sortItems = Seq(Descending("y")), limit1)
      .|.expand("(x)-[:R]->(y)")
      .|.argument("x")
      .nodeByLabelScan("x","A")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val expected = aNodes.flatMap(_ => bNodes.sortBy(-_.getId)
                         .take(limit1))
                         .sortBy(_.getId)
                         .take(limit2)
                         .map(Array[Any](_))

    runtimeResult should beColumns("y").withRows(inOrder(expected))
  }

  test("should top on top of apply") {
    // given
    val nodeCount = 10
    val (nodes, _) = given { circleGraph(nodeCount) }
    val limit = nodeCount / 2

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .top(Seq(Descending("y"), Ascending("x")), limit )
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

    val expected = allRows.sortBy(row => (-row(1).asInstanceOf[Node].getId, row(0).asInstanceOf[Node].getId)).take(limit)

    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should apply apply top") {
    // given
    val nodesPerLabel = 100
    val (aNodes, _) = given { bipartiteGraph(nodesPerLabel, "A", "B", "R") }
    val limit1 = nodesPerLabel / 2
    val limit2 = limit1 / 2

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z")
      .apply()
      .|.apply()
      .|.|.top(Seq(Ascending("z")), limit2)
      .|.|.expandAll("(y)--(z)")
      .|.|.argument()
      .|.top(Seq(Descending("y")), limit1)
      .|.expandAll("(x)--(y)")
      .|.argument()
      .nodeByLabelScan("x", "A")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    def outerTop(from: Node): Seq[(Node, Node)] = {
      (for {
        rel <- from.getRelationships().asScala.toSeq
        to = rel.getOtherNode(from)
      } yield (from, to)).sortBy(tuple => -tuple._2.getId).take(limit1)
    }

    def innerTop(fromNode: Node): Seq[Node] = {
      (for {
        rel <- fromNode.getRelationships().asScala.toSeq
        to = rel.getOtherNode(fromNode)
      } yield to).sortBy(_.getId).take(limit2)
    }

    val expected = for {
      x <- aNodes
      (x, y) <- outerTop(x)
      z <- innerTop(y.asInstanceOf[Node])
    } yield Array[Any](x, y, z)

    // then
    runtimeResult should beColumns("x", "y", "z").withRows(expected)
  }
}
