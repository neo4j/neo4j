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

import java.util.Collections

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.RelationshipType

import org.neo4j.graphdb.RelationshipType

import java.util.Collections
import scala.collection.JavaConverters.asJavaIterableConverter
import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.util.Random

abstract class LeftOuterHashJoinTestBase[CONTEXT <: RuntimeContext](edition: Edition[CONTEXT],
                                                                    runtime: CypherRuntime[CONTEXT],
                                                                    val sizeHint: Int
                                                              ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should handle Apply all around") {
    // given
    val (nodes, _) = given { circleGraph(sizeHint) }

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
    val expectedResultRows = for {y <- nodes
                                  rel <- y.getRelationships().asScala
                                  x = rel.getOtherNode(y)
                                  } yield Array(x, y, y)
    runtimeResult should beColumns("x", "y", "y2").withRows(expectedResultRows)
  }

  test("should join under Apply with alias on non-join-key on RHS") {
    // given
    val (nodes, _) = given { circleGraph(sizeHint) }

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
    val expectedResultRows = for {y <- nodes
                                  rel <- y.getRelationships().asScala
                                  x = rel.getOtherNode(y)
                                  } yield Array(x, y, y)
    runtimeResult should beColumns("x", "y", "y2").withRows(expectedResultRows)
  }

  test("should join with alias on non-join-key on RHS") {
    // given
    val (nodes, _) = given { circleGraph(sizeHint) }
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
    val expectedResultRows = for {y <- nodes
                                  rel <- y.getRelationships().asScala
                                  x = rel.getOtherNode(y)
                                  } yield Array(x, y, y)
    runtimeResult should beColumns("x", "y", "y2").withRows(expectedResultRows)
  }

  test("should join with alias on join-key on RHS") {
    // given
    val (nodes, _) = given { circleGraph(sizeHint) }
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
    val expectedResultRows = for {y <- nodes
                                  rel <- y.getRelationships().asScala
                                  x = rel.getOtherNode(y)
                                  } yield Array(x, x, y)
    runtimeResult should beColumns("x", "x2", "y").withRows(expectedResultRows)
  }

  test("should join with alias on non-join-key on LHS") {
    // given
    val (nodes, _) = given { circleGraph(sizeHint) }
    // val baseSize = sizeHint / 8
    val baseSize = 1
    val seq = nodes.map(n => Array[Any](n))
    val lhsRows = batchedInputValues(baseSize, seq: _*).stream()

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
    val expectedResultRows = for {y <- nodes
                                  rel <- y.getRelationships().asScala
                                  x = rel.getOtherNode(y)
                                  } yield Array(x, y, y)
    runtimeResult should beColumns("x", "y", "y2").withRows(expectedResultRows)
  }

  test("should join with alias on join-key on LHS") {
    // given
    val (nodes, _) = given { circleGraph(sizeHint) }
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
    val expectedResultRows = for {y <- nodes
                                  rel <- y.getRelationships().asScala
                                  x = rel.getOtherNode(y)
                                  } yield Array(x, x, y)
    runtimeResult should beColumns("x", "x2", "y").withRows(expectedResultRows)
  }

  test("should handle apply on LHS and RHS") {
    // given
    val (nodes, _) = given { circleGraph(sizeHint) }
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
    val expectedResultRows = for {y <- nodes
                                  rel <- y.getRelationships().asScala
                                  x = rel.getOtherNode(y)
                                  } yield Array(x, y, y)
    runtimeResult should beColumns("x", "y", "y2").withRows(expectedResultRows)
  }
  
  test("should work when LHS is empty") {
    val nodes = given {
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

  test("should work when RHS is empty") {
    val nodes = given {
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

    val lhsRows = for {
      n <- nodes.filter(_.hasLabel(Label.label("Left")))
      l = n.getProperty("leftProp").asInstanceOf[Int]
    } yield (n, l)

    val rhsRows = for {
      n <- nodes.filter(_.hasLabel(Label.label("Right")))
      r  = n.getProperty("rightProp").asInstanceOf[Int]
    } yield (n, r)

    val expectedRows = for {
      (n, l) <- lhsRows
      (_, r) <- matchingRowsOuter(rhsRows, n)
    } yield Array(n, l, r)

    runtimeResult should beColumns("n", "l", "r").withRows(expectedRows)
  }

  test("should work when LHS and RHS produce 0 to n rows with the same join key") {
    val randomSmallIntProps: PartialFunction[Int, Map[String, Any]] = {
      case _ => Map("leftProp" -> Random.nextInt(4), "rightProp" -> Random.nextInt(4))
    }

    val nodes = given {
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
      (_, r) <- matchingRowsOuter(rhsRows, n)
    } yield Array(n, l, r)

    runtimeResult should beColumns("n", "l", "r").withRows(expectedRows)
  }

  test("should work when LHS and RHS produce nulls in the join column") {

    val randomSmallIntProps: PartialFunction[Int, Map[String, Any]] = {
      case _ => Map("leftProp" -> Random.nextInt(4), "rightProp" -> Random.nextInt(4))
    }

    val nodes = given {
      nodePropertyGraph(sizeHint, randomSmallIntProps, "Left") ++
        nodePropertyGraph(sizeHint, randomSmallIntProps, "Right") ++
        nodePropertyGraph(sizeHint, randomSmallIntProps, "Right", "Left")
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n", "l", "r")
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
      (_, r) <- matchingRowsOuter(rhsRows, n)
    } yield Array(n, l, r)

    runtimeResult should beColumns("n", "l", "r").withRows(expectedRows)
  }

  test("should work when LHS and RHS have nodes in ref slots") {
    val randomSmallIntProps: PartialFunction[Int, Map[String, Any]] = {
      case _ => Map("leftProp" -> Random.nextInt(4), "rightProp" -> Random.nextInt(4))
    }

    val nodes = given {
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
      (_, r) <- matchingRowsOuter(rhsRows, n)
    } yield Array(n, l, r)
    runtimeResult should beColumns("n", "l", "r").withRows(expectedRows)
  }

  test("should join on two key columns") {
    val randomSmallIntProps: PartialFunction[Int, Map[String, Any]] = {
      case _ => Map("leftProp" -> Random.nextInt(4), "rightProp" -> Random.nextInt(4))
    }

    val sizeHint = 2

    val nodes = given {
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

    val nodes = given {
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
      (_, _) <- matchingRowsOuter(rhsRows, n)
      l = n.getProperty("leftProp").asInstanceOf[Int]
      r = n.getProperty("rightProp").asInstanceOf[Int]
    } yield Array(n, l, r)

    runtimeResult should beColumns("n", "l", "r").withRows(expectedRows)

    val rhsFilterDbHits = runtimeResult.runtimeResult.queryProfile().operatorProfile(3).dbHits()

    // final projection should only need to look up n.leftProp for :Right nodes
    runtimeResult.runtimeResult.queryProfile().operatorProfile(1).dbHits() shouldBe rhsFilterDbHits/2
  }

  //see https://github.com/neo4j/neo4j/issues/12657
  test("should handle aggregation on top of left-outer hash join") {
    //given
    val exposures = given {
      //create some unattached nodes
      tx.createNode(Label.label("OB"))
      tx.createNode(Label.label("OB"))
      tx.createNode(Label.label("OB"))
      //...and one attached
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

    //when
    val runtimeResult = execute(logicalQuery, runtime)

    //then
    runtimeResult should beColumns("exposures").withRows(Seq(Array(exposures.asJava), Array(Collections.emptyList()), Array(Collections.emptyList()), Array(Collections.emptyList())), listInAnyOrder = true)
  }

  // Emulates outer join.
  // Given keyed rows (k, v) and a key k',
  // return the rows that have a matching key (k', v)
  // or a single row (k', null) if no key matches
  private def matchingRowsOuter[K, V >: Null](rows: Seq[(K, V)], key: K): Seq[(K, V)] = {
    // Get matching rows. Null keys match nothing. No matches gives a null value
    rows.collect { case row@(candidate, _) if matches(key, candidate) => row }
      .padTo(1, (key, null))
  }

  private def matches[K](key: K, candidate: K): Boolean = key match {
    case null                                           => false
    case p: Product if p.productIterator.contains(null) => false
    case k                                              => k == candidate
  }


}
