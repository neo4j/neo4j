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
import org.neo4j.cypher.internal.runtime.spec.RowCount
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.exceptions.ParameterWrongTypeException
import org.neo4j.graphdb.Direction.BOTH
import org.neo4j.graphdb.Direction.INCOMING
import org.neo4j.graphdb.Direction.OUTGOING
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Label.label
import org.neo4j.graphdb.RelationshipType

import scala.jdk.CollectionConverters.IterableHasAsScala

abstract class OptionalExpandIntoTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  protected val sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should optional expand and provide variables for relationship and end node - outgoing") {
    // given
    val n = Math.sqrt(sizeHint).toInt

    val (xs, ys, moreXs, moreYs) = given {
      val (xs, ys) = bipartiteGraph(n, "X", "Y", "R")
      val moreXs = nodeGraph(n, "X")
      val moreYs = nodeGraph(n, "Y")
      (xs, ys, moreXs, moreYs)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .optionalExpandInto("(x)-[r]->(y)")
      .cartesianProduct()
      .|.nodeByLabelScan("y", "Y", IndexOrderNone)
      .nodeByLabelScan("x", "X", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      x <- xs ++ moreXs
      y <- ys ++ moreYs
      _rels = x.getRelationships(OUTGOING).asScala.filter(_.getEndNode == y)
      rels = if (_rels.nonEmpty) _rels else Seq(null)
      r <- rels
    } yield Array(x, r, y)

    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("should expand and provide variables for relationship and end node - outgoing, one type") {
    // given
    val n = Math.sqrt(sizeHint).toInt

    val (xs, ys, moreXs, moreYs) = given {
      val (xs, ys) = bipartiteGraph(n, "X", "Y", "R")
      val moreXs = nodeGraph(n, "X")
      val moreYs = nodeGraph(n, "Y")
      // not matched rels as well
      connect(moreXs ++ moreYs, moreXs.indices.map(i => (i, i + moreXs.length, "Q")))
      (xs, ys, moreXs, moreYs)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .optionalExpandInto("(x)-[r:R]->(y)")
      .cartesianProduct()
      .|.nodeByLabelScan("y", "Y", IndexOrderNone)
      .nodeByLabelScan("x", "X", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      x <- xs ++ moreXs
      y <- ys ++ moreYs
      _rels =
        x.getRelationships(OUTGOING).asScala.filter(r => r.getEndNode == y && r.isType(RelationshipType.withName("R")))
      rels = if (_rels.nonEmpty) _rels else Seq(null)
      r <- rels
    } yield Array(x, r, y)

    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("should expand and provide variables for relationship and end node - outgoing, two types") {
    // given
    val n = Math.sqrt(sizeHint).toInt

    val (xs, ys, moreXs, moreYs) = given {
      val (xs, ys) = bipartiteGraph(n, "X", "Y", "R")
      val moreXs = nodeGraph(n, "X")
      val moreYs = nodeGraph(n, "Y")
      connect(moreXs ++ moreYs, moreXs.indices.map(i => (i, i + moreXs.length, "S")))
      // not matched rels as well
      connect(moreXs ++ moreYs, moreXs.indices.map(i => (i, i + moreXs.length, "Q")))
      (xs, ys, moreXs, moreYs)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .optionalExpandInto("(x)-[r:R|S]->(y)")
      .cartesianProduct()
      .|.nodeByLabelScan("y", "Y", IndexOrderNone)
      .nodeByLabelScan("x", "X", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      x <- xs ++ moreXs
      y <- ys ++ moreYs
      _rels = x.getRelationships(OUTGOING).asScala.filter(r =>
        r.getEndNode == y && (r.isType(RelationshipType.withName("R")) || r.isType(RelationshipType.withName("S")))
      )
      rels = if (_rels.nonEmpty) _rels else Seq(null)
      r <- rels
    } yield Array(x, r, y)

    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("should optional expand and provide variables for relationship and end node - incoming") {
    // given
    val n = Math.sqrt(sizeHint).toInt

    val (xs, ys, moreXs, moreYs) = given {
      val (xs, ys) = bipartiteGraph(n, "X", "Y", "R")
      val moreXs = nodeGraph(n, "X")
      val moreYs = nodeGraph(n, "Y")
      (xs, ys, moreXs, moreYs)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .optionalExpandInto("(x)<-[r]-(y)")
      .cartesianProduct()
      .|.nodeByLabelScan("y", "Y", IndexOrderNone)
      .nodeByLabelScan("x", "X", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      x <- xs ++ moreXs
      y <- ys ++ moreYs
      _rels = x.getRelationships(INCOMING).asScala.filter(_.getEndNode == y)
      rels = if (_rels.nonEmpty) _rels else Seq(null)
      r <- rels
    } yield Array(x, r, y)

    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("should expand and provide variables for relationship and end node - incoming, one type") {
    // given
    val n = Math.sqrt(sizeHint).toInt

    val (xs, ys, moreXs, moreYs) = given {
      val (xs, ys) = bipartiteGraph(n, "X", "Y", "R")
      val moreXs = nodeGraph(n, "X")
      val moreYs = nodeGraph(n, "Y")
      // not matched rels as well
      connect(moreXs ++ moreYs, moreXs.indices.map(i => (i, i + moreXs.length, "Q")))
      (xs, ys, moreXs, moreYs)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .optionalExpandInto("(x)<-[r:R]-(y)")
      .cartesianProduct()
      .|.nodeByLabelScan("y", "Y", IndexOrderNone)
      .nodeByLabelScan("x", "X", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      x <- xs ++ moreXs
      y <- ys ++ moreYs
      _rels =
        x.getRelationships(INCOMING).asScala.filter(r => r.getEndNode == y && r.isType(RelationshipType.withName("R")))
      rels = if (_rels.nonEmpty) _rels else Seq(null)
      r <- rels
    } yield Array(x, r, y)

    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("should expand and provide variables for relationship and end node - incoming, two types") {
    // given
    val n = Math.sqrt(sizeHint).toInt

    val (xs, ys, moreXs, moreYs) = given {
      val (xs, ys) = bipartiteGraph(n, "X", "Y", "R")
      val moreXs = nodeGraph(n, "X")
      val moreYs = nodeGraph(n, "Y")
      connect(moreXs ++ moreYs, moreXs.indices.map(i => (i, i + moreXs.length, "S")))
      // not matched rels as well
      connect(moreXs ++ moreYs, moreXs.indices.map(i => (i, i + moreXs.length, "Q")))
      (xs, ys, moreXs, moreYs)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .optionalExpandInto("(x)<-[r:R|S]-(y)")
      .cartesianProduct()
      .|.nodeByLabelScan("y", "Y", IndexOrderNone)
      .nodeByLabelScan("x", "X", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      x <- xs ++ moreXs
      y <- ys ++ moreYs
      _rels = x.getRelationships(INCOMING).asScala.filter(r =>
        r.getEndNode == y && (r.isType(RelationshipType.withName("R")) || r.isType(RelationshipType.withName("S")))
      )
      rels = if (_rels.nonEmpty) _rels else Seq(null)
      r <- rels
    } yield Array(x, r, y)

    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("should optional expand and provide variables for relationship and end node - undirected") {
    // given
    val n = Math.sqrt(sizeHint).toInt

    val (xs, ys, moreXs, moreYs) = given {
      val (xs, ys) = bipartiteGraph(n, "X", "Y", "R")
      val moreXs = nodeGraph(n, "X")
      val moreYs = nodeGraph(n, "Y")
      (xs, ys, moreXs, moreYs)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .optionalExpandInto("(x)-[r]-(y)")
      .cartesianProduct()
      .|.nodeByLabelScan("y", "Y", IndexOrderNone)
      .nodeByLabelScan("x", "X", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      x <- xs ++ moreXs
      y <- ys ++ moreYs
      _rels = x.getRelationships(BOTH).asScala.filter(_.getEndNode == y)
      rels = if (_rels.nonEmpty) _rels else Seq(null)
      r <- rels
    } yield Array(x, r, y)

    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("should expand and provide variables for relationship and end node - undirected, one type") {
    // given
    val n = Math.sqrt(sizeHint).toInt

    val (xs, ys, moreXs, moreYs) = given {
      val (xs, ys) = bipartiteGraph(n, "X", "Y", "R")
      val moreXs = nodeGraph(n, "X")
      val moreYs = nodeGraph(n, "Y")
      // not matched rels as well
      connect(moreXs ++ moreYs, moreXs.indices.map(i => (i, i + moreXs.length, "Q")))
      (xs, ys, moreXs, moreYs)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .optionalExpandInto("(x)-[r:R]-(y)")
      .cartesianProduct()
      .|.nodeByLabelScan("y", "Y", IndexOrderNone)
      .nodeByLabelScan("x", "X", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      x <- xs ++ moreXs
      y <- ys ++ moreYs
      _rels =
        x.getRelationships(BOTH).asScala.filter(r => r.getEndNode == y && r.isType(RelationshipType.withName("R")))
      rels = if (_rels.nonEmpty) _rels else Seq(null)
      r <- rels
    } yield Array(x, r, y)

    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("should expand and provide variables for relationship and end node - undirected, two types") {
    // given
    val n = Math.sqrt(sizeHint).toInt

    val (xs, ys, moreXs, moreYs) = given {
      val (xs, ys) = bipartiteGraph(n, "X", "Y", "R")
      val moreXs = nodeGraph(n, "X")
      val moreYs = nodeGraph(n, "Y")
      connect(moreXs ++ moreYs, moreXs.indices.map(i => (i, i + moreXs.length, "S")))
      // not matched rels as well
      connect(moreXs ++ moreYs, moreXs.indices.map(i => (i, i + moreXs.length, "Q")))
      (xs, ys, moreXs, moreYs)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .optionalExpandInto("(x)-[r:R|S]-(y)")
      .cartesianProduct()
      .|.nodeByLabelScan("y", "Y", IndexOrderNone)
      .nodeByLabelScan("x", "X", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      x <- xs ++ moreXs
      y <- ys ++ moreYs
      _rels = x.getRelationships(BOTH).asScala.filter(r =>
        r.getEndNode == y && (r.isType(RelationshipType.withName("R")) || r.isType(RelationshipType.withName("S")))
      )
      rels = if (_rels.nonEmpty) _rels else Seq(null)
      r <- rels
    } yield Array(x, r, y)

    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("should expand and handle self loops") {
    // given
    val n = sizeHint
    val relTuples = (for (i <- 0 until n by 2) yield {
      Seq(
        (i, i, "ME")
      )
    }).reduce(_ ++ _)

    val nodes = given {
      val nodes = nodeGraph(n, "X")
      connect(nodes, relTuples)
      // Some not connected nodes as well
      val moreNodes = nodeGraph(n, "X")
      nodes ++ moreNodes
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r")
      .optionalExpandInto("(x)-[r:ME]->(x)")
      .nodeByLabelScan("x", "X", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      x <- nodes
      _rels = x.getRelationships(OUTGOING).asScala.filter(r => r.isType(RelationshipType.withName("ME")))
      rels = if (_rels.nonEmpty) _rels else Seq(null)
      r <- rels
    } yield Array(x, r)

    runtimeResult should beColumns("x", "r").withRows(expected)
  }

  test("should expand given an empty input") {
    // given
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r")
      .optionalExpandInto("(x)-[r]->(x)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "r").withNoRows()
  }

  test("should handle types missing on compile") {
    // flaky
    val (n1, n2) = given {
      val n1 = tx.createNode(Label.label("X"))
      val n2 = tx.createNode(Label.label("Y"))
      n1.createRelationshipTo(n2, RelationshipType.withName("BASE"))
      (n1, n2)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .optionalExpandInto("(x)-[r:R|S]->(y)")
      .cartesianProduct()
      .|.nodeByLabelScan("y", "Y", IndexOrderNone)
      .nodeByLabelScan("x", "X", IndexOrderNone)
      .build()

    execute(logicalQuery, runtime) should beColumns("x", "r", "y").withRows(Seq(
      Array(n1, null, n2)
    ))

    // CREATE S
    val (m1, m2, m3, m4, m3m4) = given {
      val m3 = tx.createNode(Label.label("X"))
      val m4 = tx.createNode(Label.label("Y"))
      val r34 = m3.createRelationshipTo(m4, RelationshipType.withName("S"))
      (n1, n2, m3, m4, r34)
    }
    execute(logicalQuery, runtime) should beColumns("x", "r", "y").withRows(Seq(
      Array(m1, null, m2),
      Array(m1, null, m4),
      Array(m3, null, m2),
      Array(m3, m3m4, m4)
    ))

    // CREATE R
    val (o1, o2, o3, o4, o5, o6, o3o4, o5o6) = given {
      val o5 = tx.createNode(Label.label("X"))
      val o6 = tx.createNode(Label.label("Y"))
      val o5o6 = o5.createRelationshipTo(o6, RelationshipType.withName("R"))
      (m1, m2, m3, m4, o5, o6, m3m4, o5o6)
    }
    execute(logicalQuery, runtime) should beColumns("x", "r", "y").withRows(Seq(
      Array(o1, null, o2),
      Array(o1, null, o4),
      Array(o1, null, o6),
      Array(o3, null, o2),
      Array(o3, o3o4, o4),
      Array(o3, null, o6),
      Array(o5, null, o2),
      Array(o5, null, o4),
      Array(o5, o5o6, o6)
    ))
  }

  test("cached plan should adapt to new relationship types") {
    val (n1, n2) = given {
      val n1 = tx.createNode(Label.label("X"))
      val n2 = tx.createNode(Label.label("Y"))
      n1.createRelationshipTo(n2, RelationshipType.withName("BASE"))
      (n1, n2)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .optionalExpandInto("(x)-[r:R|S]->(y)")
      .cartesianProduct()
      .|.nodeByLabelScan("y", "Y", IndexOrderNone)
      .nodeByLabelScan("x", "X", IndexOrderNone)
      .build()

    val executablePlan = buildPlan(logicalQuery, runtime)

    execute(executablePlan) should beColumns("x", "r", "y").withRows(Seq(
      Array(n1, null, n2)
    ))

    // CREATE S
    val (m1, m2, m3, m4, m3m4) = given {
      val m3 = tx.createNode(Label.label("X"))
      val m4 = tx.createNode(Label.label("Y"))
      val r34 = m3.createRelationshipTo(m4, RelationshipType.withName("S"))
      (n1, n2, m3, m4, r34)
    }
    execute(executablePlan) should beColumns("x", "r", "y").withRows(Seq(
      Array(m1, null, m2),
      Array(m1, null, m4),
      Array(m3, null, m2),
      Array(m3, m3m4, m4)
    ))

    // CREATE R
    val (o1, o2, o3, o4, o5, o6, o3o4, o5o6) = given {
      val o5 = tx.createNode(Label.label("X"))
      val o6 = tx.createNode(Label.label("Y"))
      val o5o6 = o5.createRelationshipTo(o6, RelationshipType.withName("R"))
      (m1, m2, m3, m4, o5, o6, m3m4, o5o6)
    }
    execute(executablePlan) should beColumns("x", "r", "y").withRows(Seq(
      Array(o1, null, o2),
      Array(o1, null, o4),
      Array(o1, null, o6),
      Array(o3, null, o2),
      Array(o3, o3o4, o4),
      Array(o3, null, o6),
      Array(o5, null, o2),
      Array(o5, null, o4),
      Array(o5, o5o6, o6)
    ))
  }

  test("given a null start point, returns more nulls") {
    // given
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .optionalExpandInto("(x)-[r]->(y)")
      .optional()
      .cartesianProduct()
      .|.nodeByLabelScan("y", "Y", IndexOrderNone)
      .nodeByLabelScan("x", "X", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "y", "r").withSingleRow(null, null, null)
  }

  test("should handle node reference as input") {
    // given
    val n = Math.sqrt(sizeHint).toInt

    val (xs, ys) = given {
      bipartiteGraph(n, "X", "Y", "R")
    }

    val input = inputValues(xs.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .optionalExpandInto("(x)-[r]->(y)")
      .cartesianProduct()
      .|.nodeByLabelScan("y", "Y", IndexOrderNone)
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = for {
      x <- xs
      y <- ys
      _rels = x.getRelationships(OUTGOING).asScala.filter(_.getEndNode == y)
      rels = if (_rels.nonEmpty) _rels else Seq(null)
      r <- rels
    } yield Array(x, r, y)

    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("should gracefully handle non-node reference as input") {
    // given
    val n = Math.sqrt(sizeHint).toInt
    given { nodeGraph(n, "Y") }
    val input = inputValues((1 to n).map(Array[Any](_)): _*)
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .optionalExpandAll("(x)-[r]->(y)")
      .apply()
      .|.nodeByLabelScan("y", "Y", IndexOrderNone, "x")
      .input(variables = Seq("x"))
      .build()

    // then
    a[ParameterWrongTypeException] should be thrownBy consume(execute(logicalQuery, runtime, input))
  }

  test("should support expandInto on RHS of apply") {
    // given
    val n = Math.sqrt(sizeHint).toInt
    val (xs, ys, moreXs, moreYs) = given {
      val (xs, ys) = bipartiteGraph(n, "X", "Y", "R")
      val moreXs = nodeGraph(n, "X")
      val moreYs = nodeGraph(n, "Y")
      (xs, ys, moreXs, moreYs)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .apply()
      .|.optionalExpandInto("(x)-[r:R]->(y)")
      .|.unwind("range(1, 5) AS ignored")
      .|.argument("x", "y")
      .cartesianProduct()
      .|.nodeByLabelScan("y", "Y", IndexOrderNone)
      .nodeByLabelScan("x", "X", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val expectedSingle = for {
      x <- xs ++ moreXs
      y <- ys ++ moreYs
      _rels = x.getRelationships(OUTGOING).asScala.filter(_.getEndNode == y)
      rels = if (_rels.nonEmpty) _rels else Seq(null)
      r <- rels
    } yield Array(x, r, y)
    val expected = expectedSingle.flatMap(List.fill(5)(_))

    // then
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("should support undirected expandInto on RHS of apply") {
    // given
    val n = Math.sqrt(sizeHint).toInt
    val (xs, ys, moreXs, moreYs) = given {
      val (xs, ys) = bipartiteGraph(n, "X", "Y", "R")
      val moreXs = nodeGraph(n, "X")
      val moreYs = nodeGraph(n, "Y")
      (xs, ys, moreXs, moreYs)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .apply()
      .|.optionalExpandInto("(x)-[r:R]-(y)")
      .|.unwind("range(1, 5) AS ignored")
      .|.argument("x", "y")
      .cartesianProduct()
      .|.nodeByLabelScan("y", "Y", IndexOrderNone)
      .nodeByLabelScan("x", "X", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val expectedSingle = for {
      x <- xs ++ moreXs
      y <- ys ++ moreYs
      _rels = x.getRelationships(BOTH).asScala.filter(_.getEndNode == y)
      rels = if (_rels.nonEmpty) _rels else Seq(null)
      r <- rels
    } yield Array(x, r, y)
    val expected = expectedSingle.flatMap(List.fill(5)(_))

    // then
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("should filter with a predicate") {
    // given
    val n = Math.sqrt(sizeHint).toInt

    val (xs, ys) = given {
      val xs = nodeGraph(n, "X")
      val ys = nodeGraph(n, "Y")
      connectWithProperties(xs ++ ys, xs.indices.map(i => (i, i + xs.length, "R", Map("num" -> i))))
      (xs, ys)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .optionalExpandInto("(x)-[r]->(y)", Some("r.num > 20"))
      .cartesianProduct()
      .|.nodeByLabelScan("y", "Y", IndexOrderNone)
      .nodeByLabelScan("x", "X", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      x <- xs
      y <- ys
      _rels = x.getRelationships(OUTGOING).asScala.filter(r =>
        r.getEndNode == y && r.getProperty("num").asInstanceOf[Int] > 20
      )
      rels = if (_rels.nonEmpty) _rels else Seq(null)
      r <- rels
    } yield Array(x, r, y)

    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("should filter with a predicate on cached relationship property") {
    // given
    val rels = given {
      val (_, rs) = circleGraph(sizeHint)
      rs.indices.foreach(i => rs(i).setProperty("prop", i))
      rs
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r2", "y")
      .optionalExpandInto("(x)-[r2]-(y)", Some("r2.prop = cacheR[r1.prop] AND cacheR[r1.prop] % 2 = 0"))
      .expandAll("(x)<-[r1]-(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected =
      for {
        r <- rels
        x = r.getEndNode
        r2 = if (r.getProperty("prop").asInstanceOf[Int] % 2 == 0) r else null
        y = r.getStartNode
        row <- List(Array(x, r2, y))
      } yield row
    runtimeResult should beColumns("x", "r2", "y").withRows(expected)
  }

  test("should handle nested optional expand intos") {
    // given
    val n = 10

    val r1s = given {
      val as = nodeGraph(n, "A")
      val bs = nodeGraph(n, "B")
      val cs = nodeGraph(n, "C")

      val r1s = for {
        (a, i) <- as.zipWithIndex
        b <- bs
      } yield {
        val r1 = b.createRelationshipTo(a, RelationshipType.withName("R"))
        r1.setProperty("num", i)
        r1
      }

      for {
        c <- cs
        b <- bs
      } // r2
        c.createRelationshipTo(b, RelationshipType.withName("R"))

      r1s
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r1", "r2")
      .optionalExpandInto("(b)-[r2]->(c)")
      .optionalExpandInto("(b)-[r1]->(a)", Some("r1.num > 2"))
      .cartesianProduct()
      .|.nodeByLabelScan("c", "C", IndexOrderNone)
      .cartesianProduct()
      .|.nodeByLabelScan("b", "B", IndexOrderNone)
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      r1 <- r1s
      _ <- 0 until n
    } yield {
      val actualR1 = if (r1.getProperty("num").asInstanceOf[Int] > 2) r1 else null
      Array[Any](actualR1, null)
    }

    runtimeResult should beColumns("r1", "r2").withRows(expected)
  }

  test("should handle relationship property predicate") {
    // given
    val node = given {
      val x = tx.createNode(Label.label("START"))
      val y = tx.createNode(Label.label("END"))
      val r = x.createRelationshipTo(y, RelationshipType.withName("R"))
      r.setProperty("prop", 100)
      x
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r")
      .optionalExpandInto("(x)-[r]->(y)", Some("r.prop > 100"))
      .apply()
      .|.nodeByLabelScan("y", "END", IndexOrderNone)
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "r").withSingleRow(node, null)
  }

  test("should handle optional expand into + filter") {
    // given
    val size = 1000
    val (_, rels) = given { circleGraph(size) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .filter(s"id(y) >= ${size / 2}")
      .optionalExpandInto("(x)-[r]->(y)", Some(s"id(x) >= ${size / 2}"))
      .expandAll("(x)-->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected =
      for {
        rel <- rels
        x = rel.getStartNode
        y = rel.getEndNode
        if y.getId >= size / 2
        r = if (x.getId >= size / 2) rel else null
        row <- List(Array(x, r, y))
      } yield row

    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("should not return nulls when some rows match the predicate") {
    // given
    val nodesAndRels = given {
      val rels = Seq("REL", "ZERO_REL", "ONE_REL").map(RelationshipType.withName)
      val labels = Seq("Start", "End").map(label)

      for (idx <- 0 until sizeHint) yield {
        val Seq(start, end) = labels.map(l => tx.createNode(l))

        start.setProperty("idx", idx)

        val Seq(_, zeroRel, oneRel) = rels.map(start.createRelationshipTo(end, _))

        zeroRel.setProperty("n", 0)
        oneRel.setProperty("n", 1)

        (start, end, zeroRel, oneRel, idx)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .optionalExpandInto("(x)-[r]-(y)", Some("r.n = x.idx % 2"))
      .expandAll("(x)-[:REL]->(y)")
      .nodeByLabelScan("x", "Start", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      (start, end, zeroRel, oneRel, idx) <- nodesAndRels
    } yield {
      if (idx % 2 == 0) {
        Array[Any](start, end, zeroRel)
      } else {
        Array[Any](start, end, oneRel)
      }
    }

    runtimeResult should beColumns("x", "y", "r").withRows(expected)
  }

  test("should be able access property nulled relationship") {
    // given
    given {
      nodeGraph(1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("res")
      .projection("r.prop AS res")
      .optionalExpandInto("(x)-[r]->(y)")
      .cartesianProduct()
      .|.allNodeScan("y")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("res").withSingleRow(null)
  }

  test("should be able access property on nulled relationship, property token existing") {
    // given
    given {
      val n = nodeGraph(1).head
      n.createRelationshipTo(n, RelationshipType.withName("R")).setProperty("prop", "hello")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("res")
      .projection("r.prop AS res")
      .optionalExpandInto("(x)-[r:S]->(y)")
      .cartesianProduct()
      .|.allNodeScan("y")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("res").withSingleRow(null)
  }

  test("should handle multiple optional expands when first predicate fails") {
    // given
    val nodeLabel = label("M")
    val nodes = given {
      val nodes = nodeGraph(sizeHint, "N")
      val yNode = tx.createNode(nodeLabel)
      nodes.foreach(n => {
        n.createRelationshipTo(yNode, RelationshipType.withName("A"))
        n.createRelationshipTo(yNode, RelationshipType.withName("A"))
        n.createRelationshipTo(yNode, RelationshipType.withName("B"))
        n.createRelationshipTo(yNode, RelationshipType.withName("B"))
        n.createRelationshipTo(yNode, RelationshipType.withName("B"))
      })
      nodes
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .optionalExpandInto("(x)-[r2:B]->(y)", Some(s"y:M"))
      .optionalExpandInto("(x)-[r1:A]->(y)", Some("y:NOT_THERE")) // this predicate will always fail
      .cartesianProduct()
      .|.nodeByLabelScan("y", "M")
      .nodeByLabelScan("x", "N")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(nodes.flatMap(n => Seq.fill(3)(n))))
  }

  test("should handle multiple optional expands when second predicate fails") {
    // given
    val nodeLabel = label("M")
    val nodes = given {
      val nodes = nodeGraph(sizeHint, "N")
      val yNode = tx.createNode(nodeLabel)
      nodes.foreach(n => {
        n.createRelationshipTo(yNode, RelationshipType.withName("A"))
        n.createRelationshipTo(yNode, RelationshipType.withName("A"))
        n.createRelationshipTo(yNode, RelationshipType.withName("B"))
        n.createRelationshipTo(yNode, RelationshipType.withName("B"))
        n.createRelationshipTo(yNode, RelationshipType.withName("B"))
      })
      nodes
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .optionalExpandInto("(x)-[r2:B]->(y)", Some("y:NOT_THERE")) // this predicate will always fail
      .optionalExpandInto("(x)-[r1:A]->(y)", Some(s"y:${nodeLabel.name()}"))
      .cartesianProduct()
      .|.nodeByLabelScan("y", "M")
      .nodeByLabelScan("x", "N")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(nodes.flatMap(n => Seq.fill(2)(n))))
  }

  test("should handle multiple optional expands when first predicate fails with limit") {
    // given
    val nodeLabel = label("M")
    given {
      val nodes = nodeGraph(sizeHint, "N")
      val yNode = tx.createNode(nodeLabel)
      nodes.foreach(n => {
        n.createRelationshipTo(yNode, RelationshipType.withName("A"))
        n.createRelationshipTo(yNode, RelationshipType.withName("A"))
        n.createRelationshipTo(yNode, RelationshipType.withName("B"))
        n.createRelationshipTo(yNode, RelationshipType.withName("B"))
        n.createRelationshipTo(yNode, RelationshipType.withName("B"))
      })
      nodes
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .limit(10)
      .optionalExpandInto("(x)-[r2:B]->(y)", Some(s"y:${nodeLabel.name()}"))
      .optionalExpandInto("(x)-[r1:A]->(y)", Some("y:NOT_THERE")) // this predicate will always fail
      .cartesianProduct()
      .|.nodeByLabelScan("y", "M")
      .nodeByLabelScan("x", "N")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(RowCount(10))
  }

  test("should handle multiple optional expands when second predicate fails with limit") {
    // given
    val nodeLabel = label("M")
    given {
      val nodes = nodeGraph(sizeHint, "N")
      val yNode = tx.createNode(nodeLabel)
      nodes.foreach(n => {
        n.createRelationshipTo(yNode, RelationshipType.withName("A"))
        n.createRelationshipTo(yNode, RelationshipType.withName("A"))
        n.createRelationshipTo(yNode, RelationshipType.withName("B"))
        n.createRelationshipTo(yNode, RelationshipType.withName("B"))
        n.createRelationshipTo(yNode, RelationshipType.withName("B"))
      })
      nodes
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .limit(10)
      .optionalExpandInto("(x)-[r2:B]->(y)", Some("y:NOT_THERE")) // this predicate will always fail
      .optionalExpandInto("(x)-[r1:A]->(y)", Some(s"y:${nodeLabel.name()}"))
      .cartesianProduct()
      .|.nodeByLabelScan("y", "M")
      .nodeByLabelScan("x", "N")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(RowCount(10))
  }

  test("should handle multiple optional expands when first predicate fails on the RHS of Apply") {
    // given
    val nodeLabel = label("M")
    val nodes = given {
      val nodes = nodeGraph(sizeHint, "N")
      val yNode = tx.createNode(nodeLabel)
      nodes.foreach(n => {
        n.createRelationshipTo(yNode, RelationshipType.withName("A"))
        n.createRelationshipTo(yNode, RelationshipType.withName("A"))
        n.createRelationshipTo(yNode, RelationshipType.withName("B"))
        n.createRelationshipTo(yNode, RelationshipType.withName("B"))
        n.createRelationshipTo(yNode, RelationshipType.withName("B"))
      })
      nodes
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .apply()
      .|.optionalExpandInto("(x)-[r2:B]->(y)", Some(s"y:${nodeLabel.name()}"))
      .|.optionalExpandInto("(x)-[r1:A]->(y)", Some("y:NOT_THERE")) // this predicate will always fail
      .|.cartesianProduct()
      .|.|.nodeByLabelScan("y", "M")
      .|.nodeByLabelScan("x", "N")
      .input(variables = Seq("i"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(Seq.fill(10)(Array[Any](42)): _*))

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(nodes.flatMap(n => Seq.fill(30)(n))))
  }

  test("should handle multiple optional expands when second predicate fails on the RHS of an Apply") {
    // given
    val nodeLabel = label("M")
    val nodes = given {
      val nodes = nodeGraph(sizeHint, "N")
      val yNode = tx.createNode(nodeLabel)
      nodes.foreach(n => {
        n.createRelationshipTo(yNode, RelationshipType.withName("A"))
        n.createRelationshipTo(yNode, RelationshipType.withName("A"))
        n.createRelationshipTo(yNode, RelationshipType.withName("B"))
        n.createRelationshipTo(yNode, RelationshipType.withName("B"))
        n.createRelationshipTo(yNode, RelationshipType.withName("B"))
      })
      nodes
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .apply()
      .|.optionalExpandInto("(x)-[r2:B]->(y)", Some("y:NOT_THERE")) // this predicate will always fail
      .|.optionalExpandInto("(x)-[r1:A]->(y)", Some(s"y:${nodeLabel.name()}"))
      .|.cartesianProduct()
      .|.|.nodeByLabelScan("y", "M")
      .|.nodeByLabelScan("x", "N")
      .input(variables = Seq("i"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(Seq.fill(10)(Array[Any](42)): _*))

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(nodes.flatMap(n => Seq.fill(20)(n))))
  }
}
