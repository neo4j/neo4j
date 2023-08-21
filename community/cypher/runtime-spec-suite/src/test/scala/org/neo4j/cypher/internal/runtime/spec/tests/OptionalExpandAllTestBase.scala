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
import org.neo4j.graphdb.Label.label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.RelationshipType

import scala.util.Random

abstract class OptionalExpandAllTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  protected val sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should optional expand and provide variables for relationship and end node - outgoing") {
    // given
    val n = sizeHint
    val relTuples = (for (i <- 0 until n by 2) yield {
      Seq(
        (i, (2 * i) % n, "OTHER"),
        (i, (i + 1) % n, "NEXT")
      )
    }).reduce(_ ++ _)

    val (nodes, rels) = given {
      val nodes = nodeGraph(n, "Honey")
      val rels = connect(nodes, relTuples)
      (nodes, rels)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .optionalExpandAll("(x)-[r]->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relTuples.zip(rels).map {
      case ((f, t, _), rel) => Array(nodes(f), nodes(t), rel)
    } ++ (for (i <- 1 until n + 1 by 2) yield Array(nodes(i), null, null))

    runtimeResult should beColumns("x", "y", "r").withRows(expected)
  }

  test("should expand and provide variables for relationship and end node - outgoing, one type") {
    // given
    val n = sizeHint
    val relTuples = (for (i <- 0 until n by 2) yield {
      Seq(
        (i, (2 * i) % n, "OTHER"),
        (i, (i + 1) % n, "NEXT")
      )
    }).reduce(_ ++ _)

    val (nodes, rels) = given {
      val nodes = nodeGraph(n, "Honey")
      val rels = connect(nodes, relTuples)
      (nodes, rels)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .optionalExpandAll("(x)-[r:OTHER]->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relTuples.zip(rels).collect {
      case ((f, t, typ), rel) if typ == "OTHER" => Array(nodes(f), nodes(t), rel)
    } ++ (for (i <- 1 until n + 1 by 2) yield Array(nodes(i), null, null))

    runtimeResult should beColumns("x", "y", "r").withRows(expected)
  }

  test("should expand and provide variables for relationship and end node - outgoing, two types") {
    // given
    val n = sizeHint
    val relTuples = (for (i <- 0 until n by 2) yield {
      Seq(
        (i, (2 * i) % n, "OTHER"),
        (i, (i + 1) % n, "NEXT"),
        (i, (3 * i + 5) % n, "BLACKHOLE")
      )
    }).reduce(_ ++ _)

    val (nodes, rels) = given {
      val nodes = nodeGraph(n, "Honey")
      val rels = connect(nodes, relTuples)
      (nodes, rels)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .optionalExpandAll("(x)-[r:OTHER|NEXT]->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relTuples.zip(rels).collect {
      case ((f, t, typ), rel) if typ == "OTHER" || typ == "NEXT" => Array(nodes(f), nodes(t), rel)
    } ++ (for (i <- 1 until n + 1 by 2) yield Array(nodes(i), null, null))

    runtimeResult should beColumns("x", "y", "r").withRows(expected)
  }

  test("should optional expand and provide variables for relationship and end node - incoming") {
    // given
    val n = sizeHint
    val relTuples = (for (i <- 0 until n by 2) yield {
      Seq(
        (i, (2 * i) % n, "OTHER"),
        (i, (i + 1) % n, "NEXT")
      )
    }).reduce(_ ++ _)

    val (nodes, rels) = given {
      val nodes = nodeGraph(n, "Honey")
      val rels = connect(nodes, relTuples)
      (nodes, rels)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .optionalExpandAll("(x)<-[r]-(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val nodesWithIncomingRels = relTuples.map(_._2).distinct
    val expected = relTuples.zip(rels).map {
      case ((f, t, _), rel) => Array(nodes(t), nodes(f), rel)
    } ++ (for (i <- 0 until n if !nodesWithIncomingRels.contains(i)) yield Array(nodes(i), null, null))

    runtimeResult should beColumns("x", "y", "r").withRows(expected)
  }

  test("should expand and provide variables for relationship and end node - incoming, one type") {
    // given
    val n = sizeHint
    val relTuples = (for (i <- 0 until n by 2) yield {
      Seq(
        (i, (2 * i) % n, "OTHER"),
        (i, (i + 1) % n, "NEXT")
      )
    }).reduce(_ ++ _)

    val (nodes, rels) = given {
      val nodes = nodeGraph(n, "Honey")
      val rels = connect(nodes, relTuples)
      (nodes, rels)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .optionalExpandAll("(x)<-[r:OTHER]-(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val nodesWithIncomingOtherRels = relTuples.filter(_._3 == "OTHER").map(_._2).distinct
    val expected = relTuples.zip(rels).collect {
      case ((f, t, typ), rel) if typ == "OTHER" => Array(nodes(t), nodes(f), rel)
    } ++ (for (i <- 0 until n if !nodesWithIncomingOtherRels.contains(i)) yield Array(nodes(i), null, null))

    runtimeResult should beColumns("x", "y", "r").withRows(expected)
  }

  test("should expand and provide variables for relationship and end node - incoming, two types") {
    // given
    val n = sizeHint
    val relTuples = (for (i <- 0 until n by 2) yield {
      Seq(
        (i, (2 * i) % n, "OTHER"),
        (i, (i + 1) % n, "NEXT"),
        (i, (3 * i + 5) % n, "BLACKHOLE")
      )
    }).reduce(_ ++ _)

    val (nodes, rels) = given {
      val nodes = nodeGraph(n, "Honey")
      val rels = connect(nodes, relTuples)
      (nodes, rels)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .optionalExpandAll("(x)<-[r:OTHER|NEXT]-(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val nodesWithIncomingOtherOrNextRels = relTuples.filter(t => t._3 == "OTHER" || t._3 == "NEXT").map(_._2).distinct
    val expected = relTuples.zip(rels).collect {
      case ((f, t, typ), rel) if typ == "OTHER" || typ == "NEXT" => Array(nodes(t), nodes(f), rel)
    } ++ (for (i <- 0 until n if !nodesWithIncomingOtherOrNextRels.contains(i)) yield Array(nodes(i), null, null))

    runtimeResult should beColumns("x", "y", "r").withRows(expected)
  }

  test("should optional expand and provide variables for relationship and end node - undirected") {
    // given
    val n = sizeHint
    val relTuples = (for (i <- 1 until n by 4) yield {
      Seq(
        (i, (2 * i) % n, "OTHER"),
        (i, (i + 1) % n, "NEXT")
      )
    }).reduce(_ ++ _)

    val (nodes, rels) = given {
      val nodes = nodeGraph(n, "Honey")
      val rels = connect(nodes, relTuples)
      (nodes, rels)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .optionalExpandAll("(x)-[r]-(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val nodesWithRels = relTuples.flatMap(t => Seq(t._1, t._2)).distinct
    val expected = relTuples.zip(rels).flatMap {
      case ((f, t, _), rel) => Seq(Array(nodes(t), nodes(f), rel), Array(nodes(f), nodes(t), rel))
    } ++ (for (i <- 0 until n if !nodesWithRels.contains(i)) yield Array(nodes(i), null, null))

    runtimeResult should beColumns("x", "y", "r").withRows(expected)
  }

  test("should expand and provide variables for relationship and end node - undirected, one type") {
    // given
    val n = sizeHint
    val relTuples = (for (i <- 1 until n by 4) yield {
      Seq(
        (i, (2 * i) % n, "OTHER"),
        (i, (i + 1) % n, "NEXT")
      )
    }).reduce(_ ++ _)

    val (nodes, rels) = given {
      val nodes = nodeGraph(n, "Honey")
      val rels = connect(nodes, relTuples)
      (nodes, rels)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .optionalExpandAll("(x)-[r:OTHER]-(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val nodesWithOtherRels = relTuples.filter(_._3 == "OTHER").flatMap(t => Seq(t._1, t._2)).distinct
    val expected =
      (for {
        ((f, t, typ), rel) <- relTuples.zip(rels) if typ == "OTHER"
        (from, to) <- Seq((f, t), (t, f))
      } yield Array(nodes(to), nodes(from), rel)) ++
        (for (i <- 0 until n if !nodesWithOtherRels.contains(i)) yield Array(nodes(i), null, null))

    runtimeResult should beColumns("x", "y", "r").withRows(expected)
  }

  test("should expand and provide variables for relationship and end node - undirected, two types") {
    // given
    val n = sizeHint
    val relTuples = (for (i <- 1 until n by 4) yield {
      Seq(
        (i, (2 * i) % n, "OTHER"),
        (i, (i + 1) % n, "NEXT"),
        (i, (3 * i + 5) % n, "BLACKHOLE")
      )
    }).reduce(_ ++ _)

    val (nodes, rels) = given {
      val nodes = nodeGraph(n, "Honey")
      val rels = connect(nodes, relTuples)
      (nodes, rels)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .optionalExpandAll("(x)-[r:OTHER|NEXT]-(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val nodesWithOtherOrNextRels =
      relTuples.filter(t => t._3 == "OTHER" || t._3 == "NEXT").flatMap(t => Seq(t._1, t._2)).distinct
    val expected =
      (for {
        ((f, t, typ), rel) <- relTuples.zip(rels) if typ == "OTHER" || typ == "NEXT"
        (from, to) <- Seq((f, t), (t, f))
      } yield Array(nodes(to), nodes(from), rel)) ++
        (for (i <- 0 until n if !nodesWithOtherOrNextRels.contains(i)) yield Array(nodes(i), null, null))

    runtimeResult should beColumns("x", "y", "r").withRows(expected)
  }

  test("should optional expand when not possible to fully fuse") {
    // given
    val n = sizeHint
    val relTuples = (for (i <- 0 until n by 2) yield {
      Seq(
        (i, (2 * i) % n, "OTHER"),
        (i, (i + 1) % n, "NEXT")
      )
    }).reduce(_ ++ _)

    val (nodes, rels) = given {
      val nodes = nodeGraph(n, "Honey")
      val rels = connect(nodes, relTuples)
      (nodes, rels)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .nonFuseable()
      .optionalExpandAll("(x)-[r]->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relTuples.zip(rels).map {
      case ((f, t, _), rel) => Array(nodes(f), nodes(t), rel)
    } ++ (for (i <- 1 until n + 1 by 2) yield Array(nodes(i), null, null))

    runtimeResult should beColumns("x", "y", "r").withRows(expected)
  }

  test("should expand and handle self loops") {
    // given
    val n = sizeHint
    val relTuples = (for (i <- 0 until n by 2) yield {
      Seq(
        (i, i, "ME")
      )
    }).reduce(_ ++ _)

    val (nodes, rels) = given {
      val nodes = nodeGraph(n, "Honey")
      val rels = connect(nodes, relTuples)
      (nodes, rels)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .optionalExpandAll("(x)-[r:ME]->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relTuples.zip(rels).map {
      case ((f, t, _), rel) => Array(nodes(f), nodes(t), rel)
    } ++ (for (i <- 1 until n + 1 by 2) yield Array(nodes(i), null, null))

    runtimeResult should beColumns("x", "y", "r").withRows(expected)
  }

  test("should expand given an empty input") {
    // given
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .optionalExpandAll("(x)-[r]->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "y", "r").withNoRows()
  }

  test("should handle types missing on compile") {
    val (n1, n2) = given {
      val n1 = tx.createNode()
      val n2 = tx.createNode()
      n1.createRelationshipTo(n2, RelationshipType.withName("BASE"))
      (n1, n2)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .optionalExpandAll("(x)-[:R|S]->(y)")
      .allNodeScan("x")
      .build()

    execute(logicalQuery, runtime) should beColumns("x", "y").withRows(Seq(
      Array(n1, null),
      Array(n2, null)
    ))

    // CREATE S
    val (m1, m2, m3, m4) = given {
      val m3 = tx.createNode()
      val m4 = tx.createNode()
      m3.createRelationshipTo(m4, RelationshipType.withName("S"))
      (n1, n2, m3, m4)
    }
    execute(logicalQuery, runtime) should beColumns("x", "y").withRows(Seq(
      Array(m1, null),
      Array(m2, null),
      Array(m3, m4),
      Array(m4, null)
    ))

    // CREATE R
    val (o1, o2, o3, o4, o5, o6) = given {
      val o5 = tx.createNode()
      val o6 = tx.createNode()
      o5.createRelationshipTo(o6, RelationshipType.withName("R"))
      (m1, m2, m3, m4, o5, o6)
    }
    execute(logicalQuery, runtime) should beColumns("x", "y").withRows(Seq(
      Array(o1, null),
      Array(o2, null),
      Array(o3, o4),
      Array(o4, null),
      Array(o5, o6),
      Array(o6, null)
    ))
  }

  test("cached plan should adapt to new relationship types") {
    val (n1, n2) = given {
      val n1 = tx.createNode()
      val n2 = tx.createNode()
      n1.createRelationshipTo(n2, RelationshipType.withName("BASE"))
      (n1, n2)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .optionalExpandAll("(x)-[:R|S]->(y)")
      .allNodeScan("x")
      .build()

    val executablePlan = buildPlan(logicalQuery, runtime)

    execute(executablePlan) should beColumns("x", "y").withRows(Seq(
      Array(n1, null),
      Array(n2, null)
    ))

    // CREATE S
    val (m1, m2, m3, m4) = given {
      val m3 = tx.createNode()
      val m4 = tx.createNode()
      m3.createRelationshipTo(m4, RelationshipType.withName("S"))
      (n1, n2, m3, m4)
    }
    execute(executablePlan) should beColumns("x", "y").withRows(Seq(
      Array(m1, null),
      Array(m2, null),
      Array(m3, m4),
      Array(m4, null)
    ))

    // CREATE R
    val (o1, o2, o3, o4, o5, o6) = given {
      val o5 = tx.createNode()
      val o6 = tx.createNode()
      o5.createRelationshipTo(o6, RelationshipType.withName("R"))
      (m1, m2, m3, m4, o5, o6)
    }
    execute(executablePlan) should beColumns("x", "y").withRows(Seq(
      Array(o1, null),
      Array(o2, null),
      Array(o3, o4),
      Array(o4, null),
      Array(o5, o6),
      Array(o6, null)
    ))
  }

  test("given a null start point, returns more nulls") {
    // given
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .optionalExpandAll("(x)-[r]->(y)")
      .optional()
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "y", "r").withSingleRow(null, null, null)
  }

  test("should handle node reference as input") {
    // given
    val n = sizeHint
    val relTuples = (for (i <- 0 until n by 2) yield {
      Seq(
        (i, (2 * i) % n, "OTHER"),
        (i, (i + 1) % n, "NEXT")
      )
    }).reduce(_ ++ _)

    val (nodes, rels) = given {
      val nodes = nodeGraph(n, "Honey")
      val rels = connect(nodes, relTuples)
      (nodes, rels)
    }

    val input = inputValues(nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .optionalExpandAll("(x)-[r]->(y)")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = relTuples.zip(rels).map {
      case ((f, t, _), rel) => Array(nodes(f), nodes(t), rel)
    } ++ (for (i <- 1 until n + 1 by 2) yield Array(nodes(i), null, null))

    runtimeResult should beColumns("x", "y", "r").withRows(expected)
  }

  test("should gracefully handle non-node reference as input") {
    // given
    val input = inputValues(Array[Any]("nonNode"))
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .optionalExpandAll("(x)-[r]->(y)")
      .input(variables = Seq("x"))
      .build()

    // then
    a[ParameterWrongTypeException] should be thrownBy consume(execute(logicalQuery, runtime, input))
  }

  test("should support expandInto on RHS of apply") {
    // given
    val size = sizeHint / 16
    val (as, bs, moreAs) = given {
      val (as, bs) = bipartiteGraph(size, "A", "B", "R")
      // Some not connected nodes as well
      val moreAs = nodeGraph(size, "A")
      nodeGraph(size, "B")
      (as, bs, moreAs)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .apply()
      .|.optionalExpandAll("(a)-[:R]->(b)")
      .|.unwind("range(1, 5) AS ignored")
      .|.argument("a")
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val expectedSingle = (for { a <- as; b <- bs } yield Array(a, b)) ++
      (for { a <- moreAs } yield Array(a, null))
    val expected = expectedSingle.flatMap(List.fill(5)(_))

    // then
    runtimeResult should beColumns("a", "b").withRows(expected)
  }

  test("should support undirected expandInto on RHS of apply") {
    // given
    val size = sizeHint / 16
    val (as, bs, as2, bs2, moreAs) = given {
      val (as, bs) = bipartiteGraph(size, "A", "B", "R")
      val (bs2, as2) = bipartiteGraph(size, "B", "A", "R2")
      // Some not connected nodes as well
      val moreAs = nodeGraph(size, "A")
      nodeGraph(size, "B")
      (as, bs, as2, bs2, moreAs)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .apply()
      .|.optionalExpandAll("(a)--(b)")
      .|.unwind("range(1, 5) AS ignored")
      .|.argument("a")
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val expectedSingle = (for { a <- as; b <- bs } yield Array(a, b)) ++
      (for { a <- as2; b <- bs2 } yield Array(a, b)) ++
      (for { a <- moreAs } yield Array(a, null))
    val expected = expectedSingle.flatMap(List.fill(5)(_))

    // then
    runtimeResult should beColumns("a", "b").withRows(expected)
  }

  test("should support undirected expandInto on RHS of apply II") {
    // given
    val size = sizeHint / 16
    val (as, bs, as2, bs2, moreAs) = given {
      val (as, bs) = bipartiteGraph(size, "A", "B", "R")
      val (bs2, as2) = bipartiteGraph(size, "B", "A", "R2")
      // Some not connected nodes as well
      val moreAs = nodeGraph(size, "A")
      nodeGraph(size, "B")
      (as, bs, as2, bs2, moreAs)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .apply()
      .|.optional()
      .|.optionalExpandAll("(a)--(b)")
      .|.unwind("range(1, 5) AS ignored")
      .|.argument("a")
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val expectedSingle = (for { a <- as; b <- bs } yield Array(a, b)) ++
      (for { a <- as2; b <- bs2 } yield Array(a, b)) ++
      (for { a <- moreAs } yield Array(a, null))
    val expected = expectedSingle.flatMap(List.fill(5)(_))

    // then
    runtimeResult should beColumns("a", "b").withRows(expected)
  }

  test("should filter with a predicate") {
    // given
    val n = sizeHint
    val relTuples = (for (i <- 0 until n by 2) yield {
      Seq(
        (i, (2 * i) % n, "OTHER")
      )
    }).reduce(_ ++ _)

    val (nodes, rels) = given {
      val nodes = nodePropertyGraph(
        n,
        {
          case i: Int => Map("num" -> i)
        },
        "Honey"
      )
      val rels = connect(nodes, relTuples)
      (nodes, rels)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .optionalExpandAll("(x)-[r]->(y)", Some("y.num > 20"))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val xsConnectedToFilteredYs = relTuples.filter(_._2 <= 20).map(_._1).distinct
    val expected = relTuples.zip(rels).collect {
      case ((f, t, _), rel) if t > 20 => Array(nodes(f), nodes(t), rel)
    } ++
      (for (i <- 1 until n + 1 by 2) yield Array(nodes(i), null, null)) ++
      (for (i <- 0 until n if xsConnectedToFilteredYs.contains(i)) yield Array(nodes(i), null, null))

    runtimeResult should beColumns("x", "y", "r").withRows(expected)
  }

  test("should write null row if all is filtered out") {
    // given
    val n = sizeHint
    val relTuples = (for (i <- 0 until n by 2) yield {
      Seq(
        (i, (2 * i) % n, "OTHER")
      )
    }).reduce(_ ++ _)

    val (nodes, rels) = given {
      val nodes = nodePropertyGraph(
        n,
        {
          case i: Int => Map("num" -> i)
        },
        "Honey"
      )
      val rels = connect(nodes, relTuples)
      (nodes, rels)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .optionalExpandAll("(x)-[r]->(y)", Some(s"y.num > $n"))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.map(Array(_, null, null))
    runtimeResult should beColumns("x", "y", "r").withRows(expected)
  }

  test("should handle nested optional expands") {
    // given
    val n = sizeHint
    val relTuples = (for (i <- 0 until n by 2) yield {
      Seq(
        (i, (2 * i) % n, "OTHER")
      )
    }).reduce(_ ++ _)

    val (nodes, rels) = given {
      val nodes = nodePropertyGraph(
        n,
        {
          case i: Int => Map("num" -> i)
        },
        "Honey"
      )
      val rels = connect(nodes, relTuples)
      (nodes, rels)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z")
      .optionalExpandAll("(y)-[r2]->(z)", Some("z.num < 0"))
      .optionalExpandAll("(x)-[r1]->(y)", Some("y.num > 20"))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val xsConnectedToFilteredYs = relTuples.filter(_._2 <= 20).map(_._1).distinct
    val expected = relTuples.zip(rels).collect {
      case ((f, t, _), _) if t > 20 => Array(nodes(f), nodes(t), null)
    } ++
      (for (i <- 1 until n + 1 by 2) yield Array(nodes(i), null, null)) ++
      (for (i <- 0 until n if xsConnectedToFilteredYs.contains(i)) yield Array(nodes(i), null, null))

    runtimeResult should beColumns("x", "y", "z").withRows(expected)
  }

  test("should handle expand + predicate on cached property") {
    // given
    val size = 100

    val (aNodes, bNodes) = given {
      bipartiteGraph(
        size,
        "A",
        "B",
        "R",
        aProperties = {
          case i: Int => Map("prop" -> i)
        }
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .optionalExpandAll("(a)-[:R]->(b)", Some("cache[a.prop] < 10"))
      .cacheProperties("cache[a.prop]")
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected: Seq[Array[Node]] =
      for {
        a <- aNodes
        b <- if (a.getProperty("prop").asInstanceOf[Int] < 10) bNodes else Seq(null)
        row <- List(Array(a, b))
      } yield row

    runtimeResult should beColumns("a", "b").withRows(expected)
  }

  test("should invalidate cached property if row do not match the predicate") {
    // given
    val size = 100

    val (aNodes, bNodes) = given {
      bipartiteGraph(
        size,
        "A",
        "B",
        "R",
        bProperties = {
          case i: Int => Map("prop" -> i)
        }
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .optionalExpandAll("(a)-[:R]->(b)", Some("cache[b.prop] > 20 AND cache[b.prop] < 40 "))
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected: Seq[Array[Node]] =
      for {
        a <- aNodes
        b <- bNodes
        row <-
          if (b.getProperty("prop").asInstanceOf[Int] > 20 && b.getProperty("prop").asInstanceOf[Int] < 40)
            List(Array(a, b))
          else List.empty
      } yield row

    runtimeResult should beColumns("a", "b").withRows(expected)
  }

  test("should handle relationship property predicate") {
    // given
    val node = given {
      val person = tx.createNode(label("START"))
      val r = person.createRelationshipTo(tx.createNode(), RelationshipType.withName("R"))
      r.setProperty("prop", 100)
      person
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .optionalExpandAll("(x)-[r]->(y)", Some("r.prop > 100"))
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "y").withSingleRow(node, null)
  }

  test("should not return nulls when some rows match the predicate") {
    // given
    val nodes = given {
      val relType = RelationshipType.withName("R")
      val labels = Seq("Idx", "Zero", "One").map(label)

      for (idx <- 0 until sizeHint) yield {
        val Seq(idxNode, zeroNode, oneNode) = labels.map(l => tx.createNode(l))

        idxNode.setProperty("idx", idx)
        zeroNode.setProperty("n", 0)
        oneNode.setProperty("n", 1)

        idxNode.createRelationshipTo(zeroNode, relType)
        idxNode.createRelationshipTo(oneNode, relType)

        (idxNode, zeroNode, oneNode, idx)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .optionalExpandAll("(x)-[]->(y)", Some("y.n = x.idx % 2"))
      .nodeByLabelScan("x", "Idx", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      (idxNode, zeroNode, oneNode, idx) <- nodes
    } yield {
      if (idx % 2 == 0)
        Array[Any](idxNode, zeroNode)
      else
        Array[Any](idxNode, oneNode)
    }

    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should be able to access property on null node without errors") {
    // given
    given {
      nodeGraph(1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("res")
      .projection("n.t AS res")
      .optionalExpandAll("(n)-->(m)")
      .optionalExpandAll("(x)--(n)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("res").withSingleRow(null)
  }

  test("should be able to check property existence on null node without errors") {
    // given
    given {
      nodeGraph(1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("res")
      .projection("n.t IS NOT NULL AS res")
      .optionalExpandAll("(n)-->(m)")
      .optionalExpandAll("(x)--(n)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("res").withSingleRow(false)
  }

  test("should be able to check label existence on null node without errors") {
    // given
    given {
      nodeGraph(1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("res")
      .projection("n:L AS res")
      .optionalExpandAll("(n)-->(m)")
      .optionalExpandAll("(x)--(n)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("res").withSingleRow(null)
  }

  test("should be able access property on nulled relationship") {
    // given
    given {
      nodeGraph(1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("res")
      .projection("r.prop AS res")
      .optionalExpandAll("(x)-[r]->(y)")
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
      .optionalExpandAll("(x)-[r:S]->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("res").withSingleRow(null)
  }

  test("should handle multiple optional expands when first predicate fails") {
    // given
    val nodeLabel = label("Label")
    val nodes = given {
      val nodes = nodeGraph(sizeHint, "N")
      nodes.foreach(n => {
        n.createRelationshipTo(tx.createNode(nodeLabel), RelationshipType.withName("A"))
        n.createRelationshipTo(tx.createNode(nodeLabel), RelationshipType.withName("A"))
        n.createRelationshipTo(tx.createNode(nodeLabel), RelationshipType.withName("B"))
        n.createRelationshipTo(tx.createNode(nodeLabel), RelationshipType.withName("B"))
        n.createRelationshipTo(tx.createNode(nodeLabel), RelationshipType.withName("B"))
      })
      nodes
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .optionalExpandAll("(x)-[r2:B]->(z)", Some(s"z:${nodeLabel.name()}"))
      .optionalExpandAll("(x)-[r1:A]->(y)", Some("y:NOT_THERE")) // this predicate will always fail
      .nodeByLabelScan("x", "N")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(nodes.flatMap(n => Seq.fill(3)(n))))
  }

  test("should handle multiple optional expands when second predicate fails") {
    // given
    val nodeLabel = label("Label")
    val nodes = given {
      val nodes = nodeGraph(sizeHint, "N")
      nodes.foreach(n => {
        n.createRelationshipTo(tx.createNode(nodeLabel), RelationshipType.withName("A"))
        n.createRelationshipTo(tx.createNode(nodeLabel), RelationshipType.withName("A"))
        n.createRelationshipTo(tx.createNode(nodeLabel), RelationshipType.withName("B"))
        n.createRelationshipTo(tx.createNode(nodeLabel), RelationshipType.withName("B"))
        n.createRelationshipTo(tx.createNode(nodeLabel), RelationshipType.withName("B"))
      })
      nodes
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .optionalExpandAll("(x)-[r2:B]->(z)", Some("z:NOT_THERE")) // this predicate will always fail
      .optionalExpandAll("(x)-[r1:A]->(y)", Some(s"y:${nodeLabel.name()}"))
      .nodeByLabelScan("x", "N")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(nodes.flatMap(n => Seq.fill(2)(n))))
  }

  test("should handle many optional expand with random predicates") {
    // given
    given {
      val allLabels = Array("A", "B", "C", "D", "E")
      def randomLabel = label(allLabels(Random.nextInt(allLabels.length)))
      val nodes = nodeGraph(sizeHint, "N")
      nodes.foreach(n => {
        (1 to Random.nextInt(10)).foreach(_ =>
          n.createRelationshipTo(tx.createNode(randomLabel), RelationshipType.withName("R"))
        )
        (1 to Random.nextInt(10)).foreach(_ =>
          n.createRelationshipTo(tx.createNode(randomLabel), RelationshipType.withName("S"))
        )
        (1 to Random.nextInt(10)).foreach(_ =>
          n.createRelationshipTo(tx.createNode(randomLabel), RelationshipType.withName("T"))
        )
        (1 to Random.nextInt(10)).foreach(_ =>
          n.createRelationshipTo(tx.createNode(randomLabel), RelationshipType.withName("U"))
        )
        (1 to Random.nextInt(10)).foreach(_ =>
          n.createRelationshipTo(tx.createNode(randomLabel), RelationshipType.withName("V"))
        )
      })
      nodes
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .optionalExpandAll("(x)-[r1:V]->(e)", Some("e:E"))
      .optionalExpandAll("(x)-[r1:U]->(d)", Some("d:D"))
      .optionalExpandAll("(x)-[r1:T]->(c)", Some("c:C"))
      .optionalExpandAll("(x)-[r1:S]->(b)", Some("b:B"))
      .optionalExpandAll("(x)-[r1:R]->(a)", Some("a:A"))
      .nodeByLabelScan("x", "N")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then, just make sure the query finishes
    consume(runtimeResult) should not be empty
  }

  test("should handle multiple optional expands when first predicate fails with limit") {
    // given
    val nodeLabel = label("Label")
    given {
      val nodes = nodeGraph(sizeHint, "N")
      nodes.foreach(n => {
        n.createRelationshipTo(tx.createNode(nodeLabel), RelationshipType.withName("A"))
        n.createRelationshipTo(tx.createNode(nodeLabel), RelationshipType.withName("A"))
        n.createRelationshipTo(tx.createNode(nodeLabel), RelationshipType.withName("B"))
        n.createRelationshipTo(tx.createNode(nodeLabel), RelationshipType.withName("B"))
        n.createRelationshipTo(tx.createNode(nodeLabel), RelationshipType.withName("B"))
      })
      nodes
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .limit(10)
      .optionalExpandAll("(x)-[r2:B]->(z)", Some(s"z:${nodeLabel.name()}"))
      .optionalExpandAll("(x)-[r1:A]->(y)", Some("y:NOT_THERE")) // this predicate will always fail
      .nodeByLabelScan("x", "N")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(RowCount(10))
  }

  test("should handle multiple optional expands when second predicate fails with limit") {
    // given
    val nodeLabel = label("Label")
    given {
      val nodes = nodeGraph(sizeHint, "N")
      nodes.foreach(n => {
        n.createRelationshipTo(tx.createNode(nodeLabel), RelationshipType.withName("A"))
        n.createRelationshipTo(tx.createNode(nodeLabel), RelationshipType.withName("A"))
        n.createRelationshipTo(tx.createNode(nodeLabel), RelationshipType.withName("B"))
        n.createRelationshipTo(tx.createNode(nodeLabel), RelationshipType.withName("B"))
        n.createRelationshipTo(tx.createNode(nodeLabel), RelationshipType.withName("B"))
      })
      nodes
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .limit(10)
      .optionalExpandAll("(x)-[r2:B]->(z)", Some("z:NOT_THERE")) // this predicate will always fail
      .optionalExpandAll("(x)-[r1:A]->(y)", Some(s"y:${nodeLabel.name()}"))
      .nodeByLabelScan("x", "N")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(RowCount(10))
  }

  test("should handle multiple optional expands when first predicate fails on the RHS of Apply") {
    // given
    val nodeLabel = label("Label")
    val nodes = given {
      val nodes = nodeGraph(sizeHint, "N")
      nodes.foreach(n => {
        n.createRelationshipTo(tx.createNode(nodeLabel), RelationshipType.withName("A"))
        n.createRelationshipTo(tx.createNode(nodeLabel), RelationshipType.withName("A"))
        n.createRelationshipTo(tx.createNode(nodeLabel), RelationshipType.withName("B"))
        n.createRelationshipTo(tx.createNode(nodeLabel), RelationshipType.withName("B"))
        n.createRelationshipTo(tx.createNode(nodeLabel), RelationshipType.withName("B"))
      })
      nodes
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .apply()
      .|.optionalExpandAll("(x)-[r2:B]->(z)", Some(s"z:${nodeLabel.name()}"))
      .|.optionalExpandAll("(x)-[r1:A]->(y)", Some("y:NOT_THERE")) // this predicate will always fail
      .|.nodeByLabelScan("x", "N", IndexOrderNone, "i")
      .input(variables = Seq("i"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(Seq.fill(10)(Array[Any](42)): _*))

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(nodes.flatMap(n => Seq.fill(30)(n))))
  }

  test("should handle multiple optional expands when second predicate fails on the RHS of an Apply") {
    // given
    val nodeLabel = label("Label")
    val nodes = given {
      val nodes = nodeGraph(sizeHint, "N")
      nodes.foreach(n => {
        n.createRelationshipTo(tx.createNode(nodeLabel), RelationshipType.withName("A"))
        n.createRelationshipTo(tx.createNode(nodeLabel), RelationshipType.withName("A"))
        n.createRelationshipTo(tx.createNode(nodeLabel), RelationshipType.withName("B"))
        n.createRelationshipTo(tx.createNode(nodeLabel), RelationshipType.withName("B"))
        n.createRelationshipTo(tx.createNode(nodeLabel), RelationshipType.withName("B"))
      })
      nodes
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .apply()
      .|.optionalExpandAll("(x)-[r2:B]->(z)", Some("z:NOT_THERE")) // this predicate will always fail
      .|.optionalExpandAll("(x)-[r1:A]->(y)", Some(s"y:${nodeLabel.name()}"))
      .|.nodeByLabelScan("x", "N", IndexOrderNone, "i")
      .input(variables = Seq("i"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(Seq.fill(10)(Array[Any](42)): _*))

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(nodes.flatMap(n => Seq.fill(20)(n))))
  }
}
