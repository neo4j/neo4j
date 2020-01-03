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

import org.neo4j.cypher.internal.runtime.spec._
import org.neo4j.cypher.internal.{CypherRuntime, RuntimeContext}
import org.neo4j.exceptions.ParameterWrongTypeException
import org.neo4j.graphdb.Direction.OUTGOING
import org.neo4j.graphdb.Direction.BOTH
import org.neo4j.graphdb.Direction.INCOMING
import org.neo4j.graphdb.{Label, RelationshipType}

import scala.collection.JavaConverters._

abstract class OptionalExpandIntoTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  protected  val sizeHint: Int
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
      .|.nodeByLabelScan("y", "Y")
      .nodeByLabelScan("x", "X")
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
      .|.nodeByLabelScan("y", "Y")
      .nodeByLabelScan("x", "X")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      x <- xs ++ moreXs
      y <- ys ++ moreYs
      _rels = x.getRelationships(OUTGOING).asScala.filter(r => r.getEndNode == y && r.isType(RelationshipType.withName("R")))
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
      .|.nodeByLabelScan("y", "Y")
      .nodeByLabelScan("x", "X")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      x <- xs ++ moreXs
      y <- ys ++ moreYs
      _rels = x.getRelationships(OUTGOING).asScala.filter(r => r.getEndNode == y && (r.isType(RelationshipType.withName("R")) || r.isType(RelationshipType.withName("S"))))
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
      .|.nodeByLabelScan("y", "Y")
      .nodeByLabelScan("x", "X")
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
      .|.nodeByLabelScan("y", "Y")
      .nodeByLabelScan("x", "X")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      x <- xs ++ moreXs
      y <- ys ++ moreYs
      _rels = x.getRelationships(INCOMING).asScala.filter(r => r.getEndNode == y && r.isType(RelationshipType.withName("R")))
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
      .|.nodeByLabelScan("y", "Y")
      .nodeByLabelScan("x", "X")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      x <- xs ++ moreXs
      y <- ys ++ moreYs
      _rels = x.getRelationships(INCOMING).asScala.filter(r => r.getEndNode == y && (r.isType(RelationshipType.withName("R")) || r.isType(RelationshipType.withName("S"))))
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
      .|.nodeByLabelScan("y", "Y")
      .nodeByLabelScan("x", "X")
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
      .|.nodeByLabelScan("y", "Y")
      .nodeByLabelScan("x", "X")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      x <- xs ++ moreXs
      y <- ys ++ moreYs
      _rels = x.getRelationships(BOTH).asScala.filter(r => r.getEndNode == y && r.isType(RelationshipType.withName("R")))
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
      .|.nodeByLabelScan("y", "Y")
      .nodeByLabelScan("x", "X")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      x <- xs ++ moreXs
      y <- ys ++ moreYs
      _rels = x.getRelationships(BOTH).asScala.filter(r => r.getEndNode == y && (r.isType(RelationshipType.withName("R")) || r.isType(RelationshipType.withName("S"))))
      rels = if (_rels.nonEmpty) _rels else Seq(null)
      r <- rels
    } yield Array(x, r, y)

    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("should expand and handle self loops") {
    // given
    val n = sizeHint
    val relTuples = (for(i <- 0 until n by 2) yield {
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
      .nodeByLabelScan("x", "X")
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
      .produceResults("x","r")
      .optionalExpandInto("(x)-[r]->(x)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "r").withNoRows()
  }

  test("should handle types missing on compile") {
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
      .|.nodeByLabelScan("y", "Y")
      .nodeByLabelScan("x", "X")
      .build()

    execute(logicalQuery, runtime) should beColumns("x", "r", "y").withRows(Seq(
      Array(n1, null, n2)
    ))

    //CREATE S
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

    //CREATE R
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
      .|.nodeByLabelScan("y", "Y")
      .nodeByLabelScan("x", "X")
      .build()

    val executablePlan = buildPlan(logicalQuery, runtime)

    execute(executablePlan) should beColumns("x", "r", "y").withRows(Seq(
      Array(n1, null, n2)
    ))

    //CREATE S
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

    //CREATE R
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
      .|.nodeByLabelScan("y", "Y")
      .nodeByLabelScan("x", "X")
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
      .|.nodeByLabelScan("y", "Y")
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
    val input = inputValues((1 to n).map(Array[Any](_)):_*)
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .optionalExpandAll("(x)-[r]->(y)")
      .apply()
      .|.nodeByLabelScan("y", "Y", "x")
      .input(variables = Seq("x"))
      .build()

    // then
    a [ParameterWrongTypeException] should be thrownBy consume(execute(logicalQuery, runtime, input))
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
      .|.nodeByLabelScan("y", "Y")
      .nodeByLabelScan("x", "X")
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
      .|.nodeByLabelScan("y", "Y")
      .nodeByLabelScan("x", "X")
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
      .|.nodeByLabelScan("y", "Y")
      .nodeByLabelScan("x", "X")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      x <- xs
      y <- ys
      _rels = x.getRelationships(OUTGOING).asScala.filter(r => r.getEndNode == y && r.getProperty("num").asInstanceOf[Int] > 20)
      rels = if (_rels.nonEmpty) _rels else Seq(null)
      r <- rels
    } yield Array(x, r, y)

    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }
}
