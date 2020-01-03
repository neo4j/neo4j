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

import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.runtime.spec._
import org.neo4j.cypher.internal.runtime.spec.tests.ExpandAllTestBase.smallTestGraph
import org.neo4j.cypher.internal.{CypherRuntime, RuntimeContext}
import org.neo4j.exceptions.ParameterWrongTypeException
import org.neo4j.graphdb.{Label, RelationshipType}

abstract class ExpandIntoTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  protected  val sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should expand into and provide variables for relationship - outgoing") {
    // given
    val n = sizeHint
    val relTuples = (for(i <- 0 until n) yield {
      Seq(
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
      .expandInto("(x)-[r]->(y)")
      .expandAll("(x)-->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relTuples.zip(rels).map {
      case ((f, t, _), rel) => Array(nodes(f), nodes(t), rel)
    }

    runtimeResult should beColumns("x", "y", "r").withRows(expected)
  }

  test("should expand and provide variables for relationship and end node - outgoing, one type") {
    // given
    val n = sizeHint
    val relTuples = (for(i <- 0 until n) yield {
      Seq(
        (i, (i + 2) % n, "SECONDNEXT"),
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
      .expandInto("(x)-[r:SECONDNEXT]->(y)")
      .expand("(x)--(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relTuples.zip(rels).collect {
      case ((f, t, typ), rel) if typ == "SECONDNEXT" => Array(nodes(f), nodes(t), rel)
    }

    runtimeResult should beColumns("x", "y", "r").withRows(expected)
  }

  test("should expand and provide variables for relationship and end node - outgoing, two types") {
    // given
    val n = sizeHint
    val relTuples = (for(i <- 0 until n) yield {
      Seq(
        (i, (i + 2) % n, "SECONDNEXT"),
        (i, (i + 1) % n, "NEXT"),
        (i, i, "SELF")
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
      .expandInto("(x)-[r:NEXT|SECONDNEXT]->(y)")
      .expand("(x)--(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relTuples.zip(rels).collect {
      case ((f, t, typ), rel) if typ == "NEXT" || typ == "SECONDNEXT" => Array(nodes(f), nodes(t), rel)
    }

    runtimeResult should beColumns("x", "y", "r").withRows(expected)
  }

  test("should expand and handle self loops") {
    // given
    val n = sizeHint
    val relTuples = (for(i <- 0 until n) yield {
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
      .expandInto("(x)-[r:ME]->(y)")
      .expand("(x)--(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relTuples.zip(rels).map {
      case ((f, t, _), rel) => Array(nodes(f), nodes(t), rel)
    }

    runtimeResult should beColumns("x", "y", "r").withRows(expected)
  }

  test("should expand given an empty input") {
    // given
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .expandInto("(x)-[r]->(y)")
      .cartesianProduct()
      .|.allNodeScan("y")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "y", "r").withNoRows()
  }

  test("should handle expand outgoing") {
    // given
    val (_, rels) = given { circleGraph(sizeHint) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expandInto("(x)-->(y)")
      .expandAll("(x)--(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected =
      for {
        r <- rels
        row <- List(Array(r.getStartNode, r.getEndNode))
      } yield row
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should handle expand incoming") {
    // given
    val (_, rels) = given { circleGraph(sizeHint) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expandInto("(x)<--(y)")
      .expandAll("(x)--(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected =
      for {
        r <- rels
        row <- List(Array(r.getEndNode, r.getStartNode))
      } yield row
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should handle expand undirected after expandAll") {
    val (_, rels) = given { circleGraph(sizeHint) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expandInto("(x)--(y)")
      .expandAll("(x)--(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected =
      for {
        r <- rels
        row <- List(Array(r.getStartNode, r.getEndNode), Array(r.getEndNode, r.getStartNode))
      } yield row
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should handle existing types") {
    // given
    val (r1, r2, r3) = given {
      val node = tx.createNode(Label.label("L"))
      val other = tx.createNode(Label.label("L"))
      (node.createRelationshipTo(other, RelationshipType.withName("R")),
        node.createRelationshipTo(other, RelationshipType.withName("S")),
        node.createRelationshipTo(other, RelationshipType.withName("T")))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expandInto("(x)-[:R|S|T]->(y)")
      .distinct("x AS x", "y AS y")
      .expandAll("(x)--(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = List(
      Array(r1.getStartNode, r1.getEndNode),
      Array(r2.getStartNode, r2.getEndNode),
      Array(r3.getStartNode, r3.getEndNode)
    )

    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should handle types missing on compile") {
    given {
      1 to sizeHint foreach { _ =>
        tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("BASE"))
      }
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expandInto("(x)-[:R|S|T]->(y)")
      .expandAll("(x)--(y)")
      .allNodeScan("x")
      .build()

    execute(logicalQuery, runtime) should beColumns("x", "y").withRows(List.empty)

    //CREATE S
    given { tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("S")) }
    execute(logicalQuery, runtime) should beColumns("x", "y").withRows(RowCount(1))

    //CREATE R
    given { tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("R")) }
    execute(logicalQuery, runtime) should beColumns("x", "y").withRows(RowCount(2))

    //CREATE T
    given { tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("T")) }
    execute(logicalQuery, runtime) should beColumns("x", "y").withRows(RowCount(3))
  }

  test("cached plan should adapt to new relationship types") {
    given {
      1 to sizeHint foreach { _ =>
        tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("BASE"))
      }
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expandInto("(x)-[:R|S|T]->(y)")
      .expandAll("(x)--(y)")
      .allNodeScan("x")
      .build()

    val executablePlan = buildPlan(logicalQuery, runtime)

    execute(executablePlan) should beColumns("x", "y").withRows(List.empty)

    //CREATE S
    given { tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("S")) }
    execute(executablePlan) should beColumns("x", "y").withRows(RowCount(1))

    //CREATE R
    given { tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("R")) }
    execute(executablePlan) should beColumns("x", "y").withRows(RowCount(2))

    //CREATE T
    given { tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("T")) }
    execute(executablePlan) should beColumns("x", "y").withRows(RowCount(3))
  }

  test("should handle arguments spanning two morsels") {
    // NOTE: This is a specific test for pipelined runtime with morsel size _4_
    // where an argument will span two morsels that are put into a MorselBuffer

    // given
    val (a1, a2, b1, b2, b3, c) = given { smallTestGraph(tx) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b", "c")
      .apply()
      .|.expandAll("(b)-[:R]->(c)")
      .|.expandInto("(a)-[:R]->(b)")
      .|.nodeByLabelScan("b", "B",  "a")
      .nodeByLabelScan("a", "A")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val expected = for {
      a <- Seq(a1, a2)
      b <- Seq(b1, b2, b3)
    } yield Array(a, b, c)

    // then
    runtimeResult should beColumns("a", "b", "c").withRows(expected)
  }

  test("should support expandInto on RHS of apply") {
    // given
    val size = sizeHint / 16
    val (as, bs) = given {
      nodeGraph(size, "A")
      nodeGraph(size, "B")
      bipartiteGraph(size, "A", "B", "R")
    }


    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .apply()
      .|.expandInto("(a)-[:R]->(b)")
      .|.nodeByLabelScan("b", "B",  "a")
      .nodeByLabelScan("a", "A")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val expected = for {
      a <- as
      b <- bs
    } yield Array(a, b)

    // then
    runtimeResult should beColumns("a", "b").withRows(expected)
  }

  test("should support undirected expandInto on RHS of apply") {
    val size = sizeHint / 16
    // given
    val (as, bs, as2, bs2) = given {
      val (as, bs) = bipartiteGraph(size, "A", "B", "R")
      val (bs2, as2) = bipartiteGraph(size, "B", "A", "R2")
      // Some not connected nodes as well
      nodeGraph(size, "A")
      nodeGraph(size, "B")
      (as, bs, as2, bs2)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .apply()
      .|.expandInto("(a)--(b)")
      .|.nodeByLabelScan("b", "B",  "a")
      .nodeByLabelScan("a", "A")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val expected = (for {a <- as;b <- bs} yield Array(a, b)) ++ (for {a <- as2;b <- bs2} yield Array(a, b))

    // then
    runtimeResult should beColumns("a", "b").withRows(expected)
  }
}

// Supported by interpreted, slotted, pipelined, parallel
trait ExpandIntoWithOtherOperatorsTestBase[CONTEXT <: RuntimeContext] {
  self: ExpandIntoTestBase[CONTEXT] =>

  test("given a null start point, returns an empty iterator") {
    // given
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .expandInto("(x)-[r]->(y)")
      .optional()
      .cartesianProduct()
      .|.allNodeScan("y")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "y", "r").withNoRows()
  }

  test("should handle arguments spanning two morsels with sort") {
    // NOTE: This is a specific test for pipelined runtime with morsel size _4_
    // where an argument will span two morsels that are put into a MorselBuffer

    val (a1, a2, b1, b2, b3, c) = given { smallTestGraph(tx) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b", "c")
      .apply()
      .|.sort(Seq(Ascending("a"), Ascending("b")))
      .|.expandAll("(b)-[:R]->(c)")
      .|.expandInto("(a)-[:R]->(b)")
      .|.nodeByLabelScan("b", "B",  "a")
      .nodeByLabelScan("a", "A")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val expected = for {
      a <- Seq(a1, a2)
      b <- Seq(b1, b2, b3)
    } yield Array(a, b, c)

    // then
    runtimeResult should beColumns("a", "b", "c").withRows(inOrder(expected))
  }

  test("should handle node reference as input") {
    // given
    val n = sizeHint
    val relTuples = (for(i <- 0 until n) yield {
      Seq(
        (i, (i + 1) % n, "NEXT")
      )
    }).reduce(_ ++ _)
    val (nodes, rels) = given {
      val nodes = nodeGraph(n, "Honey")
      val rels = connect(nodes, relTuples)
      (nodes, rels)
    }

    val input = inputValues(nodes.sliding(2).map(_.toArray[Any]).toSeq: _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .expandInto("(x)-[r]->(y)")
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = relTuples.zip(rels).collect {
      case ((f, t, _), rel) if f != sizeHint - 1 => Array(nodes(f), nodes(t), rel)
    }

    runtimeResult should beColumns("x", "y", "r").withRows(expected)
  }

  test("should gracefully handle non-node reference as input") {
    // given
    val n = sizeHint
    val input = inputValues((1 to n).map(i => Array[Any](i, i + 1)):_*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .expandInto("(x)-[r]->(y)")
      .input(variables = Seq("x", "y"))
      .build()

    // then
    a [ParameterWrongTypeException] should be thrownBy consume(execute(logicalQuery, runtime, input))
  }

  test("should handle expand + filter") {
    // given
    val size = 1000
    val (_, rels) = given { circleGraph(size) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .filter(s"id(y) >= ${size / 2}")
      .expandInto("(x)-->(y)")
      .expandAll("(x)-->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected =
      for {
        r <- rels
        if r.getEndNode.getId >= size /2
        row <- List(Array(r.getStartNode, r.getEndNode))
      } yield row
    runtimeResult should beColumns("x", "y").withRows(expected)
  }
}
