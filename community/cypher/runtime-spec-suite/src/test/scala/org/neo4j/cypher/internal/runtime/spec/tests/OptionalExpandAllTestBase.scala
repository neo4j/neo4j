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

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.exceptions.ParameterWrongTypeException
import org.neo4j.graphdb.Label.label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.RelationshipType

abstract class OptionalExpandAllTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  protected  val sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should optional expand and provide variables for relationship and end node - outgoing") {
    // given
    val n = sizeHint
    val relTuples = (for(i <- 0 until n by 2) yield {
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
    val relTuples = (for(i <- 0 until n by 2) yield {
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
    val relTuples = (for(i <- 0 until n by 2) yield {
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
    val relTuples = (for(i <- 0 until n by 2) yield {
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
    val relTuples = (for(i <- 0 until n by 2) yield {
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
    val relTuples = (for(i <- 0 until n by 2) yield {
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
    val relTuples = (for(i <- 1 until n by 4) yield {
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
    val relTuples = (for(i <- 1 until n by 4) yield {
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
    val expected = (for {
      ((f, t, typ), rel) <- relTuples.zip(rels) if typ == "OTHER"
      (from, to) <- Seq((f, t), (t, f))
    } yield Array(nodes(to), nodes(from), rel)) ++
      (for (i <- 0 until n if !nodesWithOtherRels.contains(i)) yield Array(nodes(i), null, null))

    runtimeResult should beColumns("x", "y", "r").withRows(expected)
  }

  test("should expand and provide variables for relationship and end node - undirected, two types") {
    // given
    val n = sizeHint
    val relTuples = (for(i <- 1 until n by 4) yield {
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
    val nodesWithOtherOrNextRels = relTuples.filter(t => t._3 == "OTHER" || t._3 == "NEXT").flatMap(t => Seq(t._1, t._2)).distinct
    val expected = (for {
      ((f, t, typ), rel) <- relTuples.zip(rels) if typ == "OTHER" || typ == "NEXT"
      (from, to) <- Seq((f, t), (t, f))
    } yield Array(nodes(to), nodes(from), rel)) ++
      (for (i <- 0 until n if !nodesWithOtherOrNextRels.contains(i)) yield Array(nodes(i), null, null))

    runtimeResult should beColumns("x", "y", "r").withRows(expected)
  }

  test("should optional expand when not possible to fully fuse") {
    // given
    val n = sizeHint
    val relTuples = (for(i <- 0 until n by 2) yield {
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
    val relTuples = (for(i <- 0 until n by 2) yield {
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

    //CREATE S
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

    //CREATE R
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

    //CREATE S
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

    //CREATE R
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
    val relTuples = (for(i <- 0 until n by 2) yield {
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
    a [ParameterWrongTypeException] should be thrownBy consume(execute(logicalQuery, runtime, input))
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

    val expectedSingle = (for {a <- as; b <- bs} yield Array(a, b)) ++
      (for {a <- moreAs} yield Array(a, null))
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

    val expectedSingle = (for {a <- as; b <- bs} yield Array(a, b)) ++
      (for {a <- as2; b <- bs2} yield Array(a, b)) ++
      (for {a <- moreAs} yield Array(a, null))
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

    val expectedSingle = (for {a <- as; b <- bs} yield Array(a, b)) ++
      (for {a <- as2; b <- bs2} yield Array(a, b)) ++
      (for {a <- moreAs} yield Array(a, null))
    val expected = expectedSingle.flatMap(List.fill(5)(_))

    // then
    runtimeResult should beColumns("a", "b").withRows(expected)
  }

  test("should filter with a predicate") {
    // given
    val n = sizeHint
    val relTuples = (for(i <- 0 until n by 2) yield {
      Seq(
        (i, (2 * i) % n, "OTHER")
      )
    }).reduce(_ ++ _)

    val (nodes, rels) = given {
      val nodes = nodePropertyGraph(n, {
        case i: Int => Map("num" -> i)
      }, "Honey")
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
    val relTuples = (for(i <- 0 until n by 2) yield {
      Seq(
        (i, (2 * i) % n, "OTHER")
      )
    }).reduce(_ ++ _)

    val (nodes, rels) = given {
      val nodes = nodePropertyGraph(n, {
        case i: Int => Map("num" -> i)
      }, "Honey")
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
    val relTuples = (for(i <- 0 until n by 2) yield {
      Seq(
        (i, (2 * i) % n, "OTHER")
      )
    }).reduce(_ ++ _)

    val (nodes, rels) = given {
      val nodes = nodePropertyGraph(n, {
        case i: Int => Map("num" -> i)
      }, "Honey")
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
        })
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
}
