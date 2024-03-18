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
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.RelationshipType
import org.neo4j.values.storable.Values.stringValue

import java.util.Collections.emptyList

import scala.jdk.CollectionConverters.IterableHasAsJava
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.util.Random

abstract class LeftOuterHashJoinTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  val sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should handle Apply all around") {
    // given
    val (nodes, _) = givenGraph { circleGraph(sizeHint) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "y2")
      .apply()
      .|.leftOuterHashJoin("x")
      .|.|.apply()
      .|.|.|.projection("y AS y2")
      .|.|.|.argument("x", "y")
      .|.|.expand("(x)--(y)")
      .|.|.argument("x")
      .|.argument("x")
      .apply()
      .|.argument("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expectedResultRows = for {
      y <- nodes
      rel <- y.getRelationships().asScala
      x = rel.getOtherNode(y)
    } yield Array(x, y, y)
    runtimeResult should beColumns("x", "y", "y2").withRows(expectedResultRows)
  }

  test("should join under Apply with alias on non-join-key on RHS") {
    // given
    val (nodes, _) = givenGraph { circleGraph(sizeHint) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "y2")
      .apply()
      .|.leftOuterHashJoin("x")
      .|.|.projection("y AS y2")
      .|.|.expand("(x)--(y)")
      .|.|.argument("x")
      .|.argument("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expectedResultRows = for {
      y <- nodes
      rel <- y.getRelationships().asScala
      x = rel.getOtherNode(y)
    } yield Array(x, y, y)
    runtimeResult should beColumns("x", "y", "y2").withRows(expectedResultRows)
  }

  test("should join with alias on non-join-key on RHS") {
    // given
    val (nodes, _) = givenGraph { circleGraph(sizeHint) }
    val lhsRows = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "y2")
      .leftOuterHashJoin("x")
      .|.projection("y AS y2")
      .|.expand("(y)--(x)")
      .|.allNodeScan("y")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    val expectedResultRows = for {
      y <- nodes
      rel <- y.getRelationships().asScala
      x = rel.getOtherNode(y)
    } yield Array(x, y, y)
    runtimeResult should beColumns("x", "y", "y2").withRows(expectedResultRows)
  }

  test("should join with alias on join-key on RHS") {
    // given
    val (nodes, _) = givenGraph { circleGraph(sizeHint) }
    val lhsRows = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "x2", "y")
      .leftOuterHashJoin("x")
      .|.projection("x AS x2")
      .|.expand("(y)--(x)")
      .|.allNodeScan("y")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    val expectedResultRows = for {
      y <- nodes
      rel <- y.getRelationships().asScala
      x = rel.getOtherNode(y)
    } yield Array(x, x, y)
    runtimeResult should beColumns("x", "x2", "y").withRows(expectedResultRows)
  }

  test("should join with alias on non-join-key on LHS") {
    // given
    val (nodes, _) = givenGraph { circleGraph(sizeHint) }
    // val baseSize = sizeHint / 8
    val baseSize = 1
    val seq = nodes.map(n => Array[Any](n))
    val lhsRows = batchedInputValues(baseSize, seq.toSeq: _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "y2")
      .leftOuterHashJoin("x")
      .|.allNodeScan("x")
      .projection("y AS y2")
      .expand("(x)--(y)")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    val expectedResultRows = for {
      y <- nodes
      rel <- y.getRelationships().asScala
      x = rel.getOtherNode(y)
    } yield Array(x, y, y)
    runtimeResult should beColumns("x", "y", "y2").withRows(expectedResultRows)
  }

  test("should join with alias on join-key on LHS") {
    // given
    val (nodes, _) = givenGraph { circleGraph(sizeHint) }
    val lhsRows = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "x2", "y")
      .leftOuterHashJoin("x")
      .|.expand("(y)--(x)")
      .|.allNodeScan("y")
      .projection("x AS x2")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    val expectedResultRows = for {
      y <- nodes
      rel <- y.getRelationships().asScala
      x = rel.getOtherNode(y)
    } yield Array(x, x, y)
    runtimeResult should beColumns("x", "x2", "y").withRows(expectedResultRows)
  }

  test("should join on nodes with different types on rhs and lhs") {
    // given
    val (nodes, _) = givenGraph {
      circleGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .leftOuterHashJoin("x")
      .|.unwind("[xLong] as x") // refslot
      .|.allNodeScan("xLong")
      .allNodeScan("x") // longslot
      .build()

    val runtimeResult = execute(logicalQuery, runtime, NO_INPUT)

    // then
    val expectedResultRows = nodes.map(Array(_))
    runtimeResult should beColumns("x").withRows(expectedResultRows)
  }

  test("should handle apply on LHS and RHS") {
    // given
    val (nodes, _) = givenGraph { circleGraph(sizeHint) }
    val lhsRows = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "y2")
      .leftOuterHashJoin("x")
      .|.projection("y AS y2")
      .|.expand("(y)--(x)")
      .|.apply()
      .|.|.argument("y")
      .|.allNodeScan("y")
      .apply()
      .|.argument("x")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    val expectedResultRows = for {
      y <- nodes
      rel <- y.getRelationships().asScala
      x = rel.getOtherNode(y)
    } yield Array(x, y, y)
    runtimeResult should beColumns("x", "y", "y2").withRows(expectedResultRows)
  }

  test("should work when LHS is empty") {
    givenGraph {
      val randomSmallIntProps: PartialFunction[Int, Map[String, Any]] = {
        case _ => Map("leftProp" -> Random.nextInt(4), "rightProp" -> Random.nextInt(4))
      }

      nodePropertyGraph(sizeHint, randomSmallIntProps, "Right")
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n", "l", "r")
      .leftOuterHashJoin("n")
      .|.projection("n.rightProp AS r")
      .|.nodeByLabelScan("n", "Right")
      .projection("n.leftProp AS l")
      .nodeByLabelScan("n", "Left")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("n", "l", "r").withNoRows()
  }

  test("should work when LHS is empty when on RHS of apply") {
    // given
    val nodes = givenGraph {
      nodeGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a")
      .apply()
      .|.leftOuterHashJoin("a")
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

  test("should work when RHS is empty") {
    val nodes = givenGraph {
      val randomSmallIntProps: PartialFunction[Int, Map[String, Any]] = {
        case _ => Map("leftProp" -> Random.nextInt(4), "rightProp" -> Random.nextInt(4))
      }

      nodePropertyGraph(sizeHint, randomSmallIntProps, "Left")
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n", "l", "r")
      .leftOuterHashJoin("n")
      .|.projection("n.rightProp AS r")
      .|.nodeByLabelScan("n", "Right")
      .projection("n.leftProp AS l")
      .nodeByLabelScan("n", "Left")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then

    val expectedRows = for {
      n <- nodes.filter(_.hasLabel(Label.label("Left")))
      l = n.getProperty("leftProp").asInstanceOf[Int]
    } yield Array[Any](n, l, null)

    runtimeResult should beColumns("n", "l", "r").withRows(expectedRows)
  }

  test("should work when RHS is empty when on RHS of apply") {
    // given
    val nodes = givenGraph { nodeGraph(sizeHint) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a")
      .apply()
      .|.leftOuterHashJoin("a")
      .|.|.filter("false")
      .|.|.argument("a")
      .|.argument("a")
      .filter("true")
      .allNodeScan("a")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.map(Array[Any](_))

    runtimeResult should beColumns("a").withRows(expected)
  }

  test("should work when LHS and RHS produce 0 to n rows with the same join key") {
    val randomSmallIntProps: PartialFunction[Int, Map[String, Any]] = {
      case _ => Map("leftProp" -> Random.nextInt(4), "rightProp" -> Random.nextInt(4))
    }

    val nodes = givenGraph {
      nodePropertyGraph(sizeHint, randomSmallIntProps, "Left") ++
        nodePropertyGraph(sizeHint, randomSmallIntProps, "Right") ++
        nodePropertyGraph(sizeHint, randomSmallIntProps, "Right", "Left")
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n", "l", "r")
      .leftOuterHashJoin("n")
      // Duplicate 0..n.rightProp times
      .|.unwind("tail(range(0, n.rightProp)) AS r")
      .|.nodeByLabelScan("n", "Right")
      // Duplicate 0..n.leftProp times
      .unwind("tail(range(0, n.leftProp)) AS l")
      .nodeByLabelScan("n", "Left")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then

    val lhsRows = for {
      n <- nodes.filter(_.hasLabel(Label.label("Left")))
      l <- Range.inclusive(0, n.getProperty("leftProp").asInstanceOf[Int]).tail
    } yield (n, l)

    val rhsRows = for {
      n <- nodes.filter(_.hasLabel(Label.label("Right")))
      r <- Range.inclusive(0, n.getProperty("rightProp").asInstanceOf[Int]).tail
    } yield (n, r)

    val expectedRows = for {
      (n, l) <- lhsRows
      (_, r) <- matchingRowsOuter[Node, Any](rhsRows, n)
    } yield Array(n, l, r)

    runtimeResult should beColumns("n", "l", "r").withRows(expectedRows)
  }

  List("", " with aggregation on top").foreach { t =>
    test(s"should work when LHS and RHS produce nulls in the join column $t") {

      val randomSmallIntProps: PartialFunction[Int, Map[String, Any]] = {
        case _ => Map("leftProp" -> Random.nextInt(4), "rightProp" -> Random.nextInt(4))
      }

      val nodes = givenGraph {
        nodePropertyGraph(sizeHint, randomSmallIntProps, "Left") ++
          nodePropertyGraph(sizeHint, randomSmallIntProps, "Right") ++
          nodePropertyGraph(sizeHint, randomSmallIntProps, "Right", "Left")
      }

      val logicalQueryTop =
        if (t.isEmpty) {
          new LogicalQueryBuilder(this)
            .produceResults("n", "l", "r")
        } else {
          new LogicalQueryBuilder(this)
            .produceResults("n", "l", "r")
            .projection("row[0] as n", "row[1] as l", "row[2] as r")
            .unwind("c as row")
            .aggregation(Seq.empty, Seq("collect([n, l, r]) as c"))
        }

      val logicalQuery = logicalQueryTop
        .leftOuterHashJoin("n")
        .|.unwind("tail(range(0, coalesce(n.rightProp, 1))) AS r")
        .|.injectValue("n", "null")
        .|.nodeByLabelScan("n", "Right")
        .unwind("tail(range(0, coalesce(n.leftProp, 1))) AS l")
        .injectValue("n", "null")
        .nodeByLabelScan("n", "Left")
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      // then
      val lhsRows = for {
        n <- nodes.filter(_.hasLabel(Label.label("Left"))) ++ Seq(null)
        lp = if (n == null) 1 else n.getProperty("leftProp").asInstanceOf[Int]
        l <- Range.inclusive(0, lp).tail
      } yield (n, l)

      val rhsRows = for {
        n <- nodes.filter(_.hasLabel(Label.label("Right"))) ++ Seq(null)
        rp = if (n == null) 1 else n.getProperty("rightProp").asInstanceOf[Int]
        r <- Range.inclusive(0, rp).tail
      } yield (n, r)

      val expectedRows = for {
        (n, l) <- lhsRows
        (_, r) <- matchingRowsOuter[Node, Any](rhsRows, n)
      } yield Array[Any](n, l, r)

      runtimeResult should beColumns("n", "l", "r").withRows(expectedRows)
    }
  }

  test("should work when LHS and RHS have nodes in ref slots") {
    val randomSmallIntProps: PartialFunction[Int, Map[String, Any]] = {
      case _ => Map("leftProp" -> Random.nextInt(4), "rightProp" -> Random.nextInt(4))
    }

    val nodes = givenGraph {
      nodePropertyGraph(sizeHint, randomSmallIntProps, "Left") ++
        nodePropertyGraph(sizeHint, randomSmallIntProps, "Right") ++
        nodePropertyGraph(sizeHint, randomSmallIntProps, "Right", "Left")
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n", "l", "r")
      .leftOuterHashJoin("n")
      .|.projection("n.rightProp AS r")
      // Adding in a string and then filtering it out should cause
      // the nodes to be passed in a ref slot
      .|.filter("n <> 'foo'")
      .|.injectValue("n", "'foo'")
      .|.nodeByLabelScan("n", "Right")
      .projection("n.leftProp AS l")
      .filter("n <> 'foo'")
      .injectValue("n", "'foo'")
      .nodeByLabelScan("n", "Left")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val lhsRows = for {
      n <- nodes.filter(_.hasLabel(Label.label("Left")))
      l = n.getProperty("leftProp").asInstanceOf[Int]
    } yield (n, l)

    val rhsRows = for {
      n <- nodes.filter(_.hasLabel(Label.label("Right")))
      r = n.getProperty("rightProp").asInstanceOf[Int]
    } yield (n, r)

    val expectedRows = for {
      (n, l) <- lhsRows
      (_, r) <- matchingRowsOuter[Node, Any](rhsRows, n)
    } yield Array(n, l, r)
    runtimeResult should beColumns("n", "l", "r").withRows(expectedRows)
  }

  test("should join on two key columns") {
    val randomSmallIntProps: PartialFunction[Int, Map[String, Any]] = {
      case _ => Map("leftProp" -> Random.nextInt(4), "rightProp" -> Random.nextInt(4))
    }

    val sizeHint = 2

    val nodes = givenGraph {
      nodePropertyGraph(sizeHint, randomSmallIntProps, "Left") ++
        nodePropertyGraph(sizeHint, randomSmallIntProps, "Right") ++
        nodePropertyGraph(sizeHint, randomSmallIntProps, "Right", "Left") ++
        nodePropertyGraph(sizeHint, randomSmallIntProps, "LeftExtra") ++
        nodePropertyGraph(sizeHint, randomSmallIntProps, "RightExtra") ++
        nodePropertyGraph(sizeHint, randomSmallIntProps, "RightExtra", "LeftExtra")
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n", "m", "l", "r", "le", "re")
      .leftOuterHashJoin("n", "m")
      // Duplicate 0..n.rightProp times
      .|.unwind("tail(range(0, n.rightProp)) AS r")
      .|.cartesianProduct()
      .|.|.projection("m.rightProp AS re")
      .|.|.nodeByLabelScan("m", "RightExtra")
      .|.nodeByLabelScan("n", "Right")
      // Duplicate 0..n.leftProp times
      .unwind("tail(range(0, n.leftProp)) AS l")
      .cartesianProduct()
      .|.projection("m.leftProp AS le")
      .|.nodeByLabelScan("m", "LeftExtra")
      .nodeByLabelScan("n", "Left")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then

    val lhsRows = for {
      n <- nodes.filter(_.hasLabel(Label.label("Left")))
      m <- nodes.filter(_.hasLabel(Label.label("LeftExtra")))
      le = m.getProperty("leftProp").asInstanceOf[Int]
      l <- Range.inclusive(0, n.getProperty("leftProp").asInstanceOf[Int]).tail
    } yield ((n, m), (l, le))

    val rhsRows = for {
      n <- nodes.filter(_.hasLabel(Label.label("Right")))
      m <- nodes.filter(_.hasLabel(Label.label("RightExtra")))
      re = m.getProperty("rightProp").asInstanceOf[Int]
      r <- Range.inclusive(0, n.getProperty("rightProp").asInstanceOf[Int]).tail
    } yield ((n, m), (r, re))

    val expectedRows = for {
      ((n, m), lVals) <- lhsRows
      (_, rVals) <- matchingRowsOuter(rhsRows, (n, m))
      r = if (rVals == null) null else rVals._1
      re = if (rVals == null) null else rVals._2
      l = if (lVals == null) null else lVals._1
      le = if (lVals == null) null else lVals._2
    } yield Array(n, m, l, r, le, re)

    runtimeResult should beColumns("n", "m", "l", "r", "le", "re").withRows(expectedRows)
  }

  test("should handle cached properties correctly") {
    val randomSmallIntProps: PartialFunction[Int, Map[String, Any]] = {
      case _ => Map("leftProp" -> Random.nextInt(4), "rightProp" -> Random.nextInt(4))
    }

    val nodes = givenGraph {
      nodePropertyGraph(sizeHint, randomSmallIntProps, "Left") ++
        nodePropertyGraph(sizeHint, randomSmallIntProps, "Right") ++
        nodePropertyGraph(sizeHint, randomSmallIntProps, "Right", "Left")
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n", "l", "r")
      // leftProp should be cached for :Right:Left
      // rightProp should be cached for :Right and :Right:Left
      .projection("cache[n.leftProp] AS l", "cache[n.rightProp] AS r")
      // keep :Right and :Right:Left
      .leftOuterHashJoin("n")
      // cache rightProp for :Right and :Right:Left
      .|.filter("cache[n.rightProp] > -1")
      .|.nodeByLabelScan("n", "Right")
      // cache leftProp for :Left and :Right:Left
      .filter("cache[n.leftProp] > -1")
      .nodeByLabelScan("n", "Left")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)

    // then
    val lhsRows = for {
      n <- nodes.filter(_.hasLabel(Label.label("Left")))
    } yield (n, ())

    val rhsRows = for {
      n <- nodes.filter(_.hasLabel(Label.label("Right")))
    } yield (n, ())

    val expectedRows = for {
      (n, _) <- lhsRows
      (_, _) <- matchingRowsOuter[Node, Any](rhsRows, n)
      l = n.getProperty("leftProp").asInstanceOf[Int]
      r = n.getProperty("rightProp").asInstanceOf[Int]
    } yield Array[Any](n, l, r)

    runtimeResult should beColumns("n", "l", "r").withRows(expectedRows)

    val rhsFilterDbHits = runtimeResult.runtimeResult.queryProfile().operatorProfile(3).dbHits()

    // final projection should only need to look up n.leftProp for :Right nodes
    runtimeResult.runtimeResult.queryProfile().operatorProfile(1).dbHits() shouldBe rhsFilterDbHits / 2
  }

  test("should handle aggregation on top of left-outer hash join") {
    // given
    val exposures = givenGraph {
      // create some unattached nodes
      tx.createNode(Label.label("OB"))
      tx.createNode(Label.label("OB"))
      tx.createNode(Label.label("OB"))
      // ...and one attached
      val node = tx.createNode(Label.label("OB"))
      val exposures = (1 to 4).map(_ => tx.createNode(Label.label("Exposure")))

      exposures.foreach(e => node.createRelationshipTo(e, RelationshipType.withName("R")))
      exposures
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("exposures")
      .aggregation(Seq("ob AS ob"), Seq("collect(e) AS exposures"))
      .leftOuterHashJoin("ob")
      .|.expand("(e)<-[:R]-(ob)")
      .|.nodeByLabelScan("e", "Exposure")
      .nodeByLabelScan("ob", "OB")
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(exposures.asJava, emptyList(), emptyList(), emptyList())
      .map(r => Array(r))
    runtimeResult should beColumns("exposures").withRows(expected, listInAnyOrder = true)
  }

  test("nested joins") {
    // given
    val nodes = givenGraph {
      nodeGraph(12)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .leftOuterHashJoin("y")
      .|.leftOuterHashJoin("y")
      .|.|.allNodeScan("y")
      .|.allNodeScan("y")
      .allNodeScan("y")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expectedResultRows = nodes.map(Array(_))
    runtimeResult should beColumns("y").withRows(expectedResultRows)
  }

  test("nested joins on nodes with different types and different nullability") {
    // given
    val (nodes, _) = givenGraph {
      circleGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .leftOuterHashJoin("y")
      .|.unwind("[x] as y")
      .|.leftOuterHashJoin("x")
      .|.|.allNodeScan("x")
      .|.unwind("[xLong] as x")
      .|.allNodeScan("xLong")
      .leftOuterHashJoin("y")
      .|.unwind("[yLong] as y")
      .|.allNodeScan("yLong")
      .allNodeScan("y")
      .build()

    val runtimeResult = execute(logicalQuery, runtime, NO_INPUT)

    // then
    val expectedResultRows = nodes.map(Array(_))
    runtimeResult should beColumns("y").withRows(expectedResultRows)
  }

  test("nested joins, with join-key as alias") {
    val nodes = givenGraph {
      nodeGraph(sizeHint)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .leftOuterHashJoin("x")
      .|.leftOuterHashJoin("x")
      .|.|.projection("y as x")
      .|.|.allNodeScan("y")
      .|.allNodeScan("x")
      .allNodeScan("x")
      .build()

    val result = execute(logicalQuery, runtime)

    val expected = nodes.map(n => Array(n, n))

    result should beColumns("x", "y").withRows(expected)
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
      .leftOuterHashJoin("n")
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
      .|.leftOuterHashJoin("n")
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
      .|.leftOuterHashJoin("n")
      .|.|.unwind("range(0, rhs) as r")
      .|.|.allNodeScan("n")
      .|.unwind("range(0, lhs) as l")
      .|.allNodeScan("n")
      .input(variables = Seq("lhs", "rhs"))
      .build()

    val expected = for {
      Array(lhs, rhs) <- input
      _ <- none until lhs
      lohjRhs = if (rhs == none) one else rhs
      _ <- none until lohjRhs
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
      .leftOuterHashJoin("x")
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
      .leftOuterHashJoin("y")
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
      .leftOuterHashJoin("z")
      .|.projection("y as z")
      .|.allNodeScan("y")
      .projection("x as z")
      .allNodeScan("x")
      .build()

    val result = execute(query, runtime)

    val expected = nodes.map(n => Array(n, n, n))

    result should beColumns("x", "y", "z").withRows(expected)
  }

  // Emulates outer join.
  // Given keyed rows (k, v) and a key k',
  // return the rows that have a matching key (k', v)
  // or a single row (k', null) if no key matches
  private def matchingRowsOuter[K, V >: Null](rows: Seq[(K, V)], key: K): Seq[(K, V)] = {
    // Get matching rows. Null keys match nothing. No matches gives a null value
    rows.collect { case row @ (candidate, _) if matches(key, candidate) => row }
      .padTo(1, (key, null))
  }

  private def matches[K](key: K, candidate: K): Boolean = key match {
    case null                                           => false
    case p: Product if p.productIterator.contains(null) => false
    case k                                              => k == candidate
  }

}
