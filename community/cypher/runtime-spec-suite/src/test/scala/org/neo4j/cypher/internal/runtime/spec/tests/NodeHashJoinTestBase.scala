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
import org.neo4j.graphdb.Direction
import org.neo4j.values.storable.Values.stringValue

import scala.jdk.CollectionConverters.IterableHasAsScala

abstract class NodeHashJoinTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should join after expand on empty lhs") {
    // given
    givenGraph { circleGraph(sizeHint) }
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

  test("should join on empty LHS when on RHS of apply") {
    // given
    givenGraph {
      nodeGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a")
      .apply()
      .|.nodeHashJoin("a")
      .|.|.argument("a")
      .|.filter("false")
      .|.argument("a")
      .filter("true")
      .allNodeScan("a")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("a").withNoRows()
  }

  test("should join on empty rhs") {
    // given
    val nodes = givenGraph { nodeGraph(sizeHint) }
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

  test("should join on empty RHS when on RHS of apply") {
    // given
    givenGraph {
      nodeGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a")
      .apply()
      .|.nodeHashJoin("a")
      .|.|.filter("false")
      .|.|.argument("a")
      .|.argument("a")
      .filter("true")
      .allNodeScan("a")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("a").withNoRows()
  }

  test("should join on empty lhs and rhs") {
    // given
    givenGraph { nodeGraph(sizeHint) }
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

  test("should join with nulls in lhs and rhs") {
    // given
    val nodes = givenGraph { nodeGraph(1) }
    val lhsRows = inputValues()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeHashJoin("x")
      .|.injectValue("x", "null")
      .|.allNodeScan("x")
      .injectValue("x", "null")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    // because graph contains no relationships, the expand will return no rows
    runtimeResult should beColumns("x").withSingleRow(nodes.head)
  }

  test("should join with nodes in ref slots") {
    // given
    val nodes = givenGraph { nodeGraph(1) }
    val lhsRows = inputValues()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeHashJoin("x")
      // Adding in a string and then filtering it out should cause
      // the nodes to be passed in a ref slot
      .|.filter("x <> 'foo'")
      .|.injectValue("x", "'foo'")
      .|.allNodeScan("x")
      .filter("x <> 'foo'")
      .injectValue("x", "'foo'")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    // because graph contains no relationships, the expand will return no rows
    runtimeResult should beColumns("x").withSingleRow(nodes.head)
  }

  test("should join after expand on rhs") {
    // given
    val (unfilteredNodes, _) = givenGraph { circleGraph(sizeHint) }
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
    val expectedResultRows = for {
      node <- nodes if node != null
      rel <- node.getRelationships().asScala
      otherNode = rel.getOtherNode(node)
    } yield Array(node, otherNode)

    runtimeResult should beColumns("x", "y").withRows(expectedResultRows)
  }

  test("should join after expand on lhs") {
    // given
    val (unfilteredNodes, _) = givenGraph { circleGraph(sizeHint) }
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
    val expectedResultRows = for {
      node <- nodes if node != null
      rel <- node.getRelationships().asScala
      otherNode = rel.getOtherNode(node)
    } yield Array(otherNode, node)

    runtimeResult should beColumns("x", "y").withRows(expectedResultRows)
  }

  test("should join after expand on both sides") {
    // given
    val (unfilteredNodes, _) = givenGraph { circleGraph(sizeHint) }
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
    val expectedResultRows = for {
      node <- nodes if node != null
      rel <- node.getRelationships().asScala
      otherNode = rel.getOtherNode(node)
    } yield Array(otherNode, node)

    runtimeResult should beColumns("x", "y").withRows(expectedResultRows)
  }

  test("should join nested") {
    val withAllLabels = givenGraph {
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
    val expectedResultRows = for {
      node <- withAllLabels
      i = node.getProperty("prop").asInstanceOf[Int]
      if i % 20 == 0 && i <= 80
    } yield Array(node)

    runtimeResult should beColumns("a").withRows(expectedResultRows)
  }

  test("should join below an apply") {
    // given
    val (unfilteredNodes, _) = givenGraph { circleGraph(sizeHint) }
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
    val expectedResultRows = for {
      x <- nodes if x != null
      rel1 <- x.getRelationships(Direction.OUTGOING).asScala
      rel2 <- x.getRelationships(Direction.INCOMING).asScala
      y = rel1.getOtherNode(x)
      z = rel2.getOtherNode(x)
    } yield Array(x, y, z)

    runtimeResult should beColumns("x", "y", "z").withRows(expectedResultRows)
  }

  test("should join below an apply and sort") {
    // given
    val (unfilteredNodes, _) = givenGraph { circleGraph(sizeHint) }
    val nodes = select(unfilteredNodes, selectivity = 0.5, duplicateProbability = 0.5, nullProbability = 0.1)
    val lhsRows = inputValues(nodes.map(n => Array[Any](n)): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z")
      .sort("x DESC", "y DESC", "z DESC")
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
    val unsortedExpectedResult = for {
      x <- nodes if x != null
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
    val (unfilteredNodes, _) = givenGraph { circleGraph(sizeHint) }
    val nodes = select(unfilteredNodes, selectivity = 0.5, duplicateProbability = 0.5, nullProbability = 0.1)
    val lhsRows = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()
    val limitCount = nodes.size / 2

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .limit(count = limitCount)
      .sort("x DESC", "y ASC")
      .sort("x ASC", "y DESC")
      .nodeHashJoin("x")
      .|.expand("(y)--(x)")
      .|.allNodeScan("y")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    val allRows = for {
      node <- nodes if node != null
      rel <- node.getRelationships().asScala
      otherNode = rel.getOtherNode(node)
    } yield Array(node, otherNode)
    val expectedResultRows = allRows.sortBy(arr => (-arr(0).getId, arr(1).getId)).take(limitCount)

    runtimeResult should beColumns("x", "y").withRows(inOrder(expectedResultRows))
  }

  test("should join with sort and limit after join") {
    // given
    val (unfilteredNodes, _) = givenGraph { circleGraph(sizeHint) }
    val nodes = select(unfilteredNodes, selectivity = 0.5, duplicateProbability = 0.5, nullProbability = 0.1)
    val lhsRows = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()
    val limitCount = nodes.size / 2

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .limit(count = limitCount)
      .sort("x DESC", "y ASC")
      .nodeHashJoin("x")
      .|.expand("(y)--(x)")
      .|.allNodeScan("y")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    val allRows = for {
      node <- nodes if node != null
      rel <- node.getRelationships().asScala
      otherNode = rel.getOtherNode(node)
    } yield Array(node, otherNode)
    val expectedResultRows = allRows.sortBy(arr => (-arr(0).getId, arr(1).getId)).take(limitCount)

    runtimeResult should beColumns("x", "y").withRows(inOrder(expectedResultRows))
  }

  test("should join with sort and limit on lhs") {
    // given
    val (unfilteredNodes, _) = givenGraph { circleGraph(sizeHint) }
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
      .sort("x ASC")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    val expectedResultRows = for {
      node <- nodes.filter(_ != null).sortBy(_.getId).take(limitCount)
      rel <- node.getRelationships().asScala
      otherNode = rel.getOtherNode(node)
    } yield Array(node, otherNode)

    runtimeResult should beColumns("x", "y").withRows(expectedResultRows)
  }

  test("should join with limit after join") {
    // given
    val (unfilteredNodes, _) = givenGraph { circleGraph(sizeHint) }
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
    val (unfilteredNodes, _) = givenGraph { circleGraph(sizeHint) }
    // We cannot have a nullProbability in this test. Otherwise we would not know if null-rows survive through the limit or not,
    // and that influences the number of result rows.
    val nodes = select(unfilteredNodes, selectivity = 0.5, duplicateProbability = 0.5)
    val lhsRows =
      inputColumns(100000, 3, i => nodes(i % nodes.size)).stream() // setting it high so the last assertion is not flaky
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
    val (unfilteredNodes, _) = givenGraph { circleGraph(sizeHint) }
    val nodes = select(unfilteredNodes, selectivity = 0.5, duplicateProbability = 0.5, nullProbability = 0.1)
    val lhsRows = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()
    val limitCount = nodes.size / 2

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .nodeHashJoin("x")
      .|.limit(count = limitCount)
      .|.sort("x DESC", "y DESC")
      .|.expand("(y)-->(x)")
      .|.allNodeScan("y")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    val rhsRows = {
      for {
        y <- unfilteredNodes
        rel <- y.getRelationships(Direction.OUTGOING).asScala
        rhsX = rel.getOtherNode(y)
      } yield (rhsX, y)
    }.sortBy {
      case (rhsX, y) => (-rhsX.getId, -y.getId)
    }.take(limitCount)
    val expectedResultRows = for {
      (rhsX, y) <- rhsRows
      lhsX <- nodes.filter(_ == rhsX)
    } yield Array(lhsX, y)

    runtimeResult should beColumns("x", "y").withRows(expectedResultRows)
  }

  test("should join with limit on rhs") {
    // given
    val (nodes, _) = givenGraph { circleGraph(sizeHint) }
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
    givenGraph { circleGraph(sizeHint, "A", "B") }
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
      .|.nodeByLabelScan("y", "B", IndexOrderNone)
      .expand("(x6)-->(z)")
      .expand("(x5)-->(x6)")
      .expand("(x4)-->(x5)")
      .expand("(x3)-->(x4)")
      .expand("(x2)-->(x3)")
      .expand("(x1)-->(x2)")
      .nodeByLabelScan("x1", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x1").withRows(rowCount(limitCount))
  }

  test("should pass cached properties through after join") {
    val nodes = givenGraph {
      nodePropertyGraph(sizeHint, { case i => Map("prop" -> i) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("cache[a.prop] AS prop")
      .nodeHashJoin("a")
      .|.filter("cache[a.prop] % 10 = 0")
      .|.cacheProperties("cache[a.prop]")
      .|.allNodeScan("a")
      .filter("cache[a.prop] < 100")
      .allNodeScan("a")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expectedResultRows = for {
      n <- nodes
      i = n.getProperty("prop").asInstanceOf[Int]
      if i % 10 == 0 && i < 100
    } yield Array(i)

    runtimeResult should beColumns("prop").withRows(expectedResultRows)
  }

  test("should join with alias on non-join-key on RHS") {
    // given
    val (unfilteredNodes, _) = givenGraph { circleGraph(sizeHint) }
    val nodes = select(unfilteredNodes, selectivity = 0.5, duplicateProbability = 0.5, nullProbability = 0.1)
    val lhsRows = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "y2")
      .nodeHashJoin("x")
      .|.projection("y AS y2")
      .|.expand("(y)--(x)")
      .|.allNodeScan("y")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    val expectedResultRows = for {
      node <- nodes if node != null
      rel <- node.getRelationships().asScala
      otherNode = rel.getOtherNode(node)
    } yield Array(node, otherNode, otherNode)
    runtimeResult should beColumns("x", "y", "y2").withRows(expectedResultRows)
  }

  test("should join on nodes with different types on rhs and lhs") {
    // given
    val (nodes, _) = givenGraph {
      circleGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeHashJoin("x")
      .|.unwind("[xLong] as x") // refslot
      .|.allNodeScan("xLong")
      .allNodeScan("x") // longslot
      .build()

    val runtimeResult = execute(logicalQuery, runtime, NO_INPUT)

    // then
    val expectedResultRows = nodes.map(Array(_))
    runtimeResult should beColumns("x").withRows(expectedResultRows)
  }

  test("nested joins on nodes with different types and different nullability") {
    // given
    val (nodes, _) = givenGraph {
      circleGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .nodeHashJoin("y")
      .|.unwind("[x] as y")
      .|.nodeHashJoin("x")
      .|.|.allNodeScan("x")
      .|.unwind("[xLong] as x")
      .|.allNodeScan("xLong")
      .nodeHashJoin("y")
      .|.unwind("[yLong] as y")
      .|.allNodeScan("yLong")
      .allNodeScan("y")
      .build()

    val runtimeResult = execute(logicalQuery, runtime, NO_INPUT)

    // then
    val expectedResultRows = nodes.map(Array(_))
    runtimeResult should beColumns("y").withRows(expectedResultRows)
  }

  test("should join with alias on join-key on RHS") {
    // given
    val (unfilteredNodes, _) = givenGraph { circleGraph(sizeHint) }
    val nodes = select(unfilteredNodes, selectivity = 0.5, duplicateProbability = 0.5, nullProbability = 0.1)
    val lhsRows = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "x2", "y")
      .nodeHashJoin("x")
      .|.projection("x AS x2")
      .|.expand("(y)--(x)")
      .|.allNodeScan("y")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    val expectedResultRows = for {
      node <- nodes if node != null
      rel <- node.getRelationships().asScala
      otherNode = rel.getOtherNode(node)
    } yield Array(node, node, otherNode)
    runtimeResult should beColumns("x", "x2", "y").withRows(expectedResultRows)
  }

  test("should join with alias on non-join-key on LHS") {
    // given
    val (unfilteredNodes, _) = givenGraph { circleGraph(sizeHint) }
    val nodes = select(unfilteredNodes, selectivity = 0.5, duplicateProbability = 0.5, nullProbability = 0.1)
    val lhsRows = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "y2")
      .nodeHashJoin("x")
      .|.allNodeScan("x")
      .projection("y AS y2")
      .expand("(x)--(y)")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    val expectedResultRows = for {
      node <- nodes if node != null
      rel <- node.getRelationships().asScala
      otherNode = rel.getOtherNode(node)
    } yield Array(node, otherNode, otherNode)
    runtimeResult should beColumns("x", "y", "y2").withRows(expectedResultRows)
  }

  test("should join with alias on join-key on LHS") {
    // given
    val (unfilteredNodes, _) = givenGraph { circleGraph(sizeHint) }
    val nodes = select(unfilteredNodes, selectivity = 0.5, duplicateProbability = 0.5, nullProbability = 0.1)
    val lhsRows = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "x2", "y")
      .nodeHashJoin("x")
      .|.expand("(y)--(x)")
      .|.allNodeScan("y")
      .projection("x AS x2")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    val expectedResultRows = for {
      node <- nodes if node != null
      rel <- node.getRelationships().asScala
      otherNode = rel.getOtherNode(node)
    } yield Array(node, node, otherNode)
    runtimeResult should beColumns("x", "x2", "y").withRows(expectedResultRows)
  }

  test("should join after expand and apply on both sides") {
    // given
    val (unfilteredNodes, _) = givenGraph { circleGraph(sizeHint) }
    val nodes = select(unfilteredNodes, selectivity = 0.5, duplicateProbability = 0.5, nullProbability = 0.1)
    val lhsRows = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .nodeHashJoin("x", "y")
      .|.expand("(x)--(y)")
      .|.apply()
      .|.|.argument("x")
      .|.allNodeScan("x")
      .expand("(y)--(x)")
      .apply()
      .|.argument("y")
      .input(nodes = Seq("y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    val expectedResultRows = for {
      node <- nodes if node != null
      rel <- node.getRelationships().asScala
      otherNode = rel.getOtherNode(node)
    } yield Array(otherNode, node)

    runtimeResult should beColumns("x", "y").withRows(expectedResultRows)
  }

  test("should join after expand and apply on both sides with aggregation on top") {
    // given
    val (unfilteredNodes, _) = givenGraph { circleGraph(sizeHint) }
    val nodes = select(unfilteredNodes, selectivity = 0.5, duplicateProbability = 0.5, nullProbability = 0.1)
    val lhsRows = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .projection("r[0] as x", "r[1] as y")
      .unwind("c as r")
      .aggregation(Seq.empty, Seq("collect([x, y]) as c"))
      .nodeHashJoin("x", "y")
      .|.expand("(x)--(y)")
      .|.apply()
      .|.|.argument("x")
      .|.allNodeScan("x")
      .expand("(y)--(x)")
      .apply()
      .|.argument("y")
      .input(nodes = Seq("y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    val expectedResultRows = for {
      node <- nodes if node != null
      rel <- node.getRelationships().asScala
      otherNode = rel.getOtherNode(node)
    } yield Array(otherNode, node)

    runtimeResult should beColumns("x", "y").withRows(expectedResultRows)
  }

  test("should discard columns") {
    assume(runtime.name != "interpreted")

    val size = 15
    givenGraph {
      nodePropertyGraph(size, { case i => Map("p" -> i) })
    }

    val probe = recordingProbe("lhsKeep", "lhsDiscard", "rhsKeep", "rhsDiscard")
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("lhsKeep", "rhsKeep", "rhsDiscard")
      .prober(probe)
      // We discard here but should not remove since we don't put it in an eager buffer
      .projection("0 as hi")
      .nodeHashJoin("n")
      // Note, discarding from rhs is not implemented
      .|.projection("rhsKeep AS rhsKeep")
      .|.projection("toString(n.p + 2) AS rhsKeep", "toString(n.p + 3) AS rhsDiscard")
      .|.allNodeScan("n")
      .projection("lhsKeep AS lhsKeep")
      .projection("toString(n.p) AS lhsKeep", "toString(n.p + 1) AS lhsDiscard")
      .allNodeScan("n")
      .build()

    val result = execute(logicalQuery, runtime)

    result should beColumns("lhsKeep", "rhsKeep", "rhsDiscard")
      .withRows(inAnyOrder(Range(0, size).map(i => Array(s"$i", s"${i + 2}", s"${i + 3}"))))

    probe.seenRows.map(_.toSeq).toSeq should contain theSameElementsAs
      Range(0, size)
        .map(i => Seq(stringValue(s"$i"), null, stringValue(s"${i + 2}"), stringValue(s"${i + 3}")))
  }

  test("should handle argument cancellation") {
    // given
    val lhsLimit = 3
    val rhsLimit = 3

    givenGraph {
      nodeGraph(1)
    }

    val prepareInput = for {
      downstreamRangeTo <- Range.inclusive(0, 2)
      lhsRangeTo <- Range.inclusive(0, lhsLimit * 2)
      rhsRangeTo <- Range.inclusive(0, rhsLimit * 2)
    } yield {
      Array(downstreamRangeTo, lhsRangeTo, rhsRangeTo)
    }
    val input = prepareInput ++ prepareInput ++ prepareInput ++ prepareInput

    val downstreamLimit = input.size / 2
    val upstreamLimit1 = (lhsLimit * rhsLimit) / 2
    val upstreamLimit2 = (0.75 * input.size).toInt

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z", "a", "b", "c")
      .limit(upstreamLimit2)
      .apply()
      .|.limit(upstreamLimit1)
      .|.nodeHashJoin("n")
      .|.|.limit(rhsLimit)
      .|.|.unwind("range(0, z-1) as c")
      .|.|.allNodeScan("n")
      .|.limit(lhsLimit)
      .|.unwind("range(0, y-1) as b")
      .|.allNodeScan("n")
      .limit(downstreamLimit)
      .unwind("range(0, x-1) as a")
      .input(variables = Seq("x", "y", "z"))
      .build()

    val result = execute(logicalQuery, runtime, inputValues(input.map(_.toArray[Any]): _*))
    result.awaitAll()
  }

  test("should handle argument cancellation 2") {
    // given
    val Seq(n) = givenGraph {
      nodeGraph(1)
    }

    /**
     * Generates an input that results in a repeating sequence of 9 scenarios:
     *
     * LHS             RHS
     * 0   []              []
     * 1   []              [1]
     * 2   []              [1...100]
     * 3   [1]             []
     * 4   [1]             [1]
     * 5   [1]             [1...100]
     * 6   [1...100]      []
     * 7   [1...100]      [1]
     * 8   [1...100]      [1...100]
     * 9   []              []
     * etc.
     * etc.
     *
     * This is intended to stress the join buffers, including work cancellation which kicks in when either side is empty.
     */

    val none = -1
    val one = 0
    val many = 100
    val rowCounts = Array(none, one, many)

    val input = for {
      i <- 0 until sizeHint
      iLhs = (i / 3) % 3
      iRhs = i % 3
      lhsRows = rowCounts(iLhs)
      rhsRows = rowCounts(iRhs)
    } yield {
      Array(lhsRows, rhsRows)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .apply()
      .|.nodeHashJoin("n")
      .|.|.unwind("range(0, rhs) as r")
      .|.|.allNodeScan("n")
      .|.unwind("range(0, lhs) as l")
      .|.allNodeScan("n")
      .input(variables = Seq("lhs", "rhs"))
      .build()

    val expected = for {
      Array(lhs, rhs) <- input
      _ <- none until lhs
      _ <- none until rhs
    } yield {
      n
    }

    val runtimeResult = execute(logicalQuery, runtime, inputValues(input.map(_.toArray[Any]): _*))
    runtimeResult should beColumns("n").withRows(singleColumn(expected))
  }

  test("should join when join-key is alias on rhs") {
    val nodes = givenGraph {
      nodeGraph(sizeHint)
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .nodeHashJoin("x")
      .|.projection("y as x")
      .|.allNodeScan("y")
      .allNodeScan("x")
      .build()

    val result = execute(query, runtime)

    val expected = nodes.map(n => Array(n, n))

    result should beColumns("x", "y").withRows(expected)
  }

  test("should join when join-key is alias on lhs") {
    val nodes = givenGraph {
      nodeGraph(sizeHint)
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .nodeHashJoin("y")
      .|.allNodeScan("y")
      .projection("x as y")
      .allNodeScan("x")
      .build()

    val result = execute(query, runtime)

    val expected = nodes.map(n => Array(n, n))

    result should beColumns("x", "y").withRows(expected)
  }

  test("should join when join-key is alias on both lhs and rhs") {
    val nodes = givenGraph {
      nodeGraph(sizeHint)
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z")
      .nodeHashJoin("z")
      .|.projection("y as z")
      .|.allNodeScan("y")
      .projection("x as z")
      .allNodeScan("x")
      .build()

    val result = execute(query, runtime)

    val expected = nodes.map(n => Array(n, n, n))

    result should beColumns("x", "y", "z").withRows(expected)
  }

  test("should join nested when join-key is alias on rhs") {
    val nodes = givenGraph {
      nodeGraph(sizeHint)
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .nodeHashJoin("x")
      .|.nodeHashJoin("x")
      .|.|.projection("y as x")
      .|.|.allNodeScan("y")
      .|.allNodeScan("x")
      .allNodeScan("x")
      .build()

    val result = execute(query, runtime)

    val expected = nodes.map(n => Array(n, n))

    result should beColumns("x", "y").withRows(expected)
  }
}
