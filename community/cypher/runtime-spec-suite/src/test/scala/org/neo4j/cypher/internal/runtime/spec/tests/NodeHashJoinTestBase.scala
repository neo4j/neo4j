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
import org.neo4j.graphdb.{Direction, Node}

import scala.collection.JavaConverters._

abstract class NodeHashJoinTestBase[CONTEXT <: RuntimeContext](edition: Edition[CONTEXT],
                                                               runtime: CypherRuntime[CONTEXT],
                                                               sizeHint: Int
                                                              ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should join after expand on empty lhs") {
    // given
    given { circleGraph(sizeHint) }
    val lhsRows = inputValues()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .nodeHashJoin("x")
      .|.expand("(y)--(x)")
      .|.allNodeScan("y")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test("should join on empty rhs") {
    // given
    val nodes = given { nodeGraph(sizeHint) }
    val lhsRows = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .nodeHashJoin("x")
      .|.expand("(y)--(x)")
      .|.allNodeScan("y")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    // because graph contains no relationships, the expand will return no rows
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test("should join on empty lhs and rhs") {
    // given
    given { nodeGraph(sizeHint) }
    val lhsRows = inputValues()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .nodeHashJoin("x")
      .|.expand("(y)--(x)")
      .|.allNodeScan("y")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    // because graph contains no relationships, the expand will return no rows
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test("should join after expand on rhs") {
    // given
    val (unfilteredNodes, _) = given { circleGraph(sizeHint) }
    val nodes = select(unfilteredNodes, selectivity = 0.5, duplicateProbability = 0.5, nullProbability = 0.1)
    val lhsRows = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .nodeHashJoin("x")
      .|.expand("(y)--(x)")
      .|.allNodeScan("y")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    val expectedResultRows = for {node <- nodes if node != null
                                  rel <- node.getRelationships().asScala
                                  otherNode = rel.getOtherNode(node)
                                  } yield Array(node, otherNode)

    runtimeResult should beColumns("x", "y").withRows(expectedResultRows)
  }

  test("should join after expand on lhs") {
    // given
    val (unfilteredNodes, _) = given { circleGraph(sizeHint) }
    val nodes = select(unfilteredNodes, selectivity = 0.5, duplicateProbability = 0.5, nullProbability = 0.1)
    val lhsRows = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .nodeHashJoin("x")
      .|.allNodeScan("x")
      .expand("(y)--(x)")
      .input(nodes = Seq("y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    val expectedResultRows = for {node <- nodes if node != null
                                  rel <- node.getRelationships().asScala
                                  otherNode = rel.getOtherNode(node)
                                  } yield Array(otherNode, node)

    runtimeResult should beColumns("x", "y").withRows(expectedResultRows)
  }

  test("should join after expand on both sides") {
    // given
    val (unfilteredNodes, _) = given { circleGraph(sizeHint) }
    val nodes = select(unfilteredNodes, selectivity = 0.5, duplicateProbability = 0.5, nullProbability = 0.1)
    val lhsRows = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .nodeHashJoin("x", "y")
      .|.expand("(x)--(y)")
      .|.allNodeScan("x")
      .expand("(y)--(x)")
      .input(nodes = Seq("y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    val expectedResultRows = for {node <- nodes if node != null
                                  rel <- node.getRelationships().asScala
                                  otherNode = rel.getOtherNode(node)
                                  } yield Array(otherNode, node)

    runtimeResult should beColumns("x", "y").withRows(expectedResultRows)
  }

  test("should join nested") {
    val withAllLabels = given {
      nodePropertyGraph(sizeHint, { case i => Map("prop" -> i) }, "A", "C")
      nodePropertyGraph(sizeHint, { case i => Map("prop" -> i) }, "A", "B")
      nodePropertyGraph(sizeHint, { case i => Map("prop" -> i) }, "A", "B", "C", "D")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a")
      .nodeHashJoin("a")
      .|.nodeHashJoin("a")
      .|.|.filter("a:D", "a.prop % 20 = 0")
      .|.|.allNodeScan("a")
      .|.filter("a:C", "a.prop <= 80")
      .|.allNodeScan("a")
      .nodeHashJoin("a")
      .|.filter("a:B", "a.prop % 10 = 0")
      .|.allNodeScan("a")
      .filter("a:A", "a.prop < 100")
      .allNodeScan("a")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expectedResultRows = for {node <- withAllLabels
                                  i = node.getProperty("prop").asInstanceOf[Int]
                                  if i % 20 == 0 && i <= 80
    } yield Array(node)

    runtimeResult should beColumns("a").withRows(expectedResultRows)
  }

  test("should join below an apply") {
    // given
    val (unfilteredNodes, _) = given { circleGraph(sizeHint) }
    val nodes = select(unfilteredNodes, selectivity = 0.5, duplicateProbability = 0.5, nullProbability = 0.1)
    val lhsRows = inputValues(nodes.map(n => Array[Any](n)): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z")
      .apply()
      .|.nodeHashJoin("x")
      .|.|.expand("(x)<--(z)")
      .|.|.argument("x")
      .|.expand("(x)-->(y)")
      .|.argument("x")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    val expectedResultRows = for {x <- nodes if x != null
                                  rel1 <- x.getRelationships(Direction.OUTGOING).asScala
                                  rel2 <- x.getRelationships(Direction.INCOMING).asScala
                                  y = rel1.getOtherNode(x)
                                  z = rel2.getOtherNode(x)
                                  } yield Array(x, y, z)

    runtimeResult should beColumns("x", "y", "z").withRows(expectedResultRows)
  }

  test("should join below an apply and sort") {
    // given
    val (unfilteredNodes, _) = given { circleGraph(sizeHint) }
    val nodes = select(unfilteredNodes, selectivity = 0.5, duplicateProbability = 0.5, nullProbability = 0.1)
    val lhsRows = inputValues(nodes.map(n => Array[Any](n)): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z")
      .sort(Seq(Descending("x"), Descending("y"), Descending("z")))
      .apply()
      .|.nodeHashJoin("x")
      .|.|.expand("(x)<--(z)")
      .|.|.argument("x")
      .|.expand("(x)-->(y)")
      .|.argument("x")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    val unsortedExpectedResult = for {x <- nodes if x != null
                                      rel1 <- x.getRelationships(Direction.OUTGOING).asScala
                                      rel2 <- x.getRelationships(Direction.INCOMING).asScala
                                      y = rel1.getOtherNode(x)
                                      z = rel2.getOtherNode(x)
                                      } yield Array(x, y, z)
    val expectedResult = unsortedExpectedResult.sortBy(arr => (-arr(0).getId, -arr(1).getId, -arr(2).getId))

    runtimeResult should beColumns("x", "y", "z").withRows(expectedResult)
  }

  test("should join with double sort and limit after join") {
    // given
    val (unfilteredNodes, _) = given { circleGraph(sizeHint) }
    val nodes = select(unfilteredNodes, selectivity = 0.5, duplicateProbability = 0.5, nullProbability = 0.1)
    val lhsRows = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()
    val limitCount = nodes.size / 2

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .limit(count = limitCount)
      .sort(sortItems = Seq(Descending("x"), Ascending("y")))
      .sort(sortItems = Seq(Ascending("x"), Descending("y")))
      .nodeHashJoin("x")
      .|.expand("(y)--(x)")
      .|.allNodeScan("y")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    val allRows = for {node <- nodes if node != null
                       rel <- node.getRelationships().asScala
                       otherNode = rel.getOtherNode(node)
                       } yield Array(node, otherNode)
    val expectedResultRows = allRows.sortBy(arr => (-arr(0).getId, arr(1).getId)).take(limitCount)

    runtimeResult should beColumns("x", "y").withRows(inOrder(expectedResultRows))
  }

  test("should join with sort and limit after join") {
    // given
    val (unfilteredNodes, _) = given { circleGraph(sizeHint) }
    val nodes = select(unfilteredNodes, selectivity = 0.5, duplicateProbability = 0.5, nullProbability = 0.1)
    val lhsRows = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()
    val limitCount = nodes.size / 2

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .limit(count = limitCount)
      .sort(sortItems = Seq(Descending("x"), Ascending("y")))
      .nodeHashJoin("x")
      .|.expand("(y)--(x)")
      .|.allNodeScan("y")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    val allRows = for {node <- nodes if node != null
                       rel <- node.getRelationships().asScala
                       otherNode = rel.getOtherNode(node)
                       } yield Array(node, otherNode)
    val expectedResultRows = allRows.sortBy(arr => (-arr(0).getId, arr(1).getId)).take(limitCount)

    runtimeResult should beColumns("x", "y").withRows(inOrder(expectedResultRows))
  }

  test("should join with sort and limit on lhs") {
    // given
    val (unfilteredNodes, _) = given { circleGraph(sizeHint) }
    val nodes = select(unfilteredNodes, selectivity = 0.5, duplicateProbability = 0.5, nullProbability = 0.1)
    val lhsRows = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()
    val limitCount = nodes.size / 2

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .nodeHashJoin("x")
      .|.expand("(y)--(x)")
      .|.allNodeScan("y")
      .limit(count = limitCount)
      .sort(sortItems = Seq(Ascending("x")))
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    val expectedResultRows = for {node <- nodes.filter(_ != null).sortBy(_.getId).take(limitCount)
                                  rel <- node.getRelationships().asScala
                                  otherNode = rel.getOtherNode(node)
                                  } yield Array(node, otherNode)

    runtimeResult should beColumns("x", "y").withRows(expectedResultRows)
  }

  test("should join with limit after join") {
    // given
    val (unfilteredNodes, _) = given { circleGraph(sizeHint) }
    val nodes = select(unfilteredNodes, selectivity = 0.5, duplicateProbability = 0.5, nullProbability = 0.1)
    val lhsRows = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()
    val limitCount = nodes.size / 2

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .limit(count = limitCount)
      .nodeHashJoin("x")
      .|.expand("(y)--(x)")
      .|.allNodeScan("y")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    runtimeResult should beColumns("x", "y").withRows(rowCount(limitCount))
  }

  test("should join with limit on lhs") {
    // given
    val (unfilteredNodes, _) = given { circleGraph(sizeHint) }
    // We cannot have a nullProbability in this test. Otherwise we would not know if null-rows survive through the limit or not,
    // and that influences the number of result rows.
    val nodes = select(unfilteredNodes, selectivity = 0.5, duplicateProbability = 0.5)
    val lhsRows = inputColumns(100000, 3, i => nodes(i % nodes.size)).stream() // setting it high so the last assertion is not flaky
    val limitCount = 10 // setting it low so the last assertion is not flaky

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .nodeHashJoin("x")
      .|.expand("(y)--(x)")
      .|.allNodeScan("y")
      .limit(count = limitCount)
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    runtimeResult should beColumns("x", "y").withRows(rowCount(limitCount * 2))

    lhsRows.hasMore should be(true)
  }

  test("should join with sort and limit on rhs") {
    // given
    val (unfilteredNodes, _) = given { circleGraph(sizeHint) }
    val nodes = select(unfilteredNodes, selectivity = 0.5, duplicateProbability = 0.5, nullProbability = 0.1)
    val lhsRows = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()
    val limitCount = nodes.size / 2

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .nodeHashJoin("x")
      .|.limit(count = limitCount)
      .|.sort(sortItems = Seq(Descending("x"), Descending("y")))
      .|.expand("(y)-->(x)")
      .|.allNodeScan("y")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    val rhsRows = {
      for {y <- unfilteredNodes
           rel <- y.getRelationships(Direction.OUTGOING).asScala
           rhsX = rel.getOtherNode(y)
           } yield (rhsX, y)
      }.sortBy {
      case (rhsX, y) => (-rhsX.getId, -y.getId)
    }.take(limitCount)
    val expectedResultRows = for {(rhsX, y) <- rhsRows
                                  lhsX <- nodes.filter(_ == rhsX)
                                  } yield Array(lhsX, y)

    runtimeResult should beColumns("x", "y").withRows(expectedResultRows)
  }

  test("should join with limit on rhs") {
    // given
    val (nodes, _) = given { circleGraph(sizeHint) }
    val lhsRows = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()
    val limitCount = nodes.size / 2

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .nodeHashJoin("x")
      .|.limit(count = limitCount)
      .|.expand("(y)-->(x)")
      .|.allNodeScan("y")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    runtimeResult should beColumns("x", "y").withRows(rowCount(limitCount))
  }

  test("should join on more than 5 variables") {
    // given
    given { circleGraph(sizeHint, "A", "B") }
    val limitCount = 1

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x1")
      .limit(count = limitCount)
      .nodeHashJoin("x1", "x2", "x3", "x4", "x5", "x6")
      .|.expand("(x5)-->(x6)")
      .|.expand("(x4)-->(x5)")
      .|.expand("(x3)-->(x4)")
      .|.expand("(x2)-->(x3)")
      .|.expand("(x1)-->(x2)")
      .|.expand("(y)-->(x1)")
      .|.nodeByLabelScan("y", "B")
      .expand("(x6)-->(z)")
      .expand("(x5)-->(x6)")
      .expand("(x4)-->(x5)")
      .expand("(x3)-->(x4)")
      .expand("(x2)-->(x3)")
      .expand("(x1)-->(x2)")
      .nodeByLabelScan("x1", "A")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x1").withRows(rowCount(limitCount))
  }
}
