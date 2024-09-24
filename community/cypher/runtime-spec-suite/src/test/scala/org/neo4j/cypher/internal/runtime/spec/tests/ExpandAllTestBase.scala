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
import org.neo4j.cypher.internal.runtime.spec.tests.ExpandAllTestBase.smallTestGraph
import org.neo4j.exceptions.ParameterWrongTypeException
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.RelationshipType
import org.neo4j.kernel.impl.coreapi.InternalTransaction

object ExpandAllTestBase {

  def smallTestGraph(tx: InternalTransaction): (Node, Node, Node, Node, Node, Node) = {
    val a1 = tx.createNode(Label.label("A"))
    val a2 = tx.createNode(Label.label("A"))
    val b1 = tx.createNode(Label.label("B"))
    val b2 = tx.createNode(Label.label("B"))
    val b3 = tx.createNode(Label.label("B"))
    val c = tx.createNode(Label.label("C"))

    a1.createRelationshipTo(b1, RelationshipType.withName("R"))
    a1.createRelationshipTo(b2, RelationshipType.withName("R"))
    a1.createRelationshipTo(b3, RelationshipType.withName("R"))
    a2.createRelationshipTo(b1, RelationshipType.withName("R"))
    a2.createRelationshipTo(b2, RelationshipType.withName("R"))
    a2.createRelationshipTo(b3, RelationshipType.withName("R"))

    b1.createRelationshipTo(c, RelationshipType.withName("R"))
    b2.createRelationshipTo(c, RelationshipType.withName("R"))
    b3.createRelationshipTo(c, RelationshipType.withName("R"))
    (a1, a2, b1, b2, b3, c)
  }

}

abstract class ExpandAllTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  protected val sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should expand and provide variables for relationship and end node - outgoing") {
    // given
    val n = sizeHint
    val relTuples = (for (i <- 0 until n) yield {
      Seq(
        (i, (2 * i) % n, "OTHER"),
        (i, (i + 1) % n, "NEXT")
      )
    }).reduce(_ ++ _)
    val (nodes, rels) = givenGraph {
      val nodes = nodeGraph(n, "Honey")
      val rels = connect(nodes, relTuples)
      (nodes, rels)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .expand("(x)-[r]->(y)")
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
    val relTuples = (for (i <- 0 until n) yield {
      Seq(
        (i, (2 * i) % n, "OTHER"),
        (i, (i + 1) % n, "NEXT")
      )
    }).reduce(_ ++ _)
    val (nodes, rels) = givenGraph {
      val nodes = nodeGraph(n, "Honey")
      val rels = connect(nodes, relTuples)
      (nodes, rels)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .expand("(x)-[r:OTHER]->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relTuples.zip(rels).collect {
      case ((f, t, typ), rel) if typ == "OTHER" => Array(nodes(f), nodes(t), rel)
    }

    runtimeResult should beColumns("x", "y", "r").withRows(expected)
  }

  test("should expand and provide variables for relationship and end node - outgoing, two types") {
    // given
    val n = sizeHint
    val relTuples = (for (i <- 0 until n) yield {
      Seq(
        (i, (2 * i) % n, "OTHER"),
        (i, (i + 1) % n, "NEXT"),
        (i, (3 * i + 5) % n, "BLACKHOLE")
      )
    }).reduce(_ ++ _)
    val (nodes, rels) = givenGraph {
      val nodes = nodeGraph(n, "Honey")
      val rels = connect(nodes, relTuples)
      (nodes, rels)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .expand("(x)-[r:OTHER|NEXT]->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relTuples.zip(rels).collect {
      case ((f, t, typ), rel) if typ == "OTHER" || typ == "NEXT" => Array(nodes(f), nodes(t), rel)
    }

    runtimeResult should beColumns("x", "y", "r").withRows(expected)
  }

  test("should expand and handle self loops") {
    // given
    val n = sizeHint
    val relTuples = (for (i <- 0 until n) yield {
      Seq(
        (i, i, "ME")
      )
    }).reduce(_ ++ _)
    val (nodes, rels) = givenGraph {
      val nodes = nodeGraph(n, "Honey")
      val rels = connect(nodes, relTuples)
      (nodes, rels)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .expand("(x)-[r:ME]->(y)")
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
      .expand("(x)-[r]->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "y", "r").withNoRows()
  }

  test("should handle expand outgoing") {
    // given
    val (_, rels) = givenGraph { circleGraph(sizeHint) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expandAll("(x)-->(y)")
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
    val (_, rels) = givenGraph { circleGraph(sizeHint) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expandAll("(x)<--(y)")
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

  test("should handle existing types") {
    // given
    val (r1, r2, r3) = givenGraph {
      val node = tx.createNode(Label.label("L"))
      val other = tx.createNode(Label.label("L"))
      (
        node.createRelationshipTo(other, RelationshipType.withName("R")),
        node.createRelationshipTo(other, RelationshipType.withName("S")),
        node.createRelationshipTo(other, RelationshipType.withName("T"))
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expandAll("(x)-[:R|S|T]->(y)")
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
    givenGraph {
      1 to sizeHint foreach { _ =>
        tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("BASE"))
      }
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expandAll("(x)-[:R|S|T]->(y)")
      .allNodeScan("x")
      .build()

    execute(logicalQuery, runtime) should beColumns("x", "y").withRows(List.empty)

    // CREATE S
    givenGraph { tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("S")) }
    execute(logicalQuery, runtime) should beColumns("x", "y").withRows(RowCount(1))

    // CREATE R
    givenGraph { tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("R")) }
    execute(logicalQuery, runtime) should beColumns("x", "y").withRows(RowCount(2))

    // CREATE T
    givenGraph { tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("T")) }
    execute(logicalQuery, runtime) should beColumns("x", "y").withRows(RowCount(3))
  }

  test("cached plan should adapt to new relationship types") {
    givenGraph {
      1 to sizeHint foreach { _ =>
        tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("BASE"))
      }
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expandAll("(x)-[:R|S|T]->(y)")
      .allNodeScan("x")
      .build()

    val executablePlan = buildPlan(logicalQuery, runtime)

    execute(executablePlan) should beColumns("x", "y").withRows(List.empty)

    // CREATE S
    givenGraph { tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("S")) }
    execute(executablePlan) should beColumns("x", "y").withRows(RowCount(1))

    // CREATE R
    givenGraph { tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("R")) }
    execute(executablePlan) should beColumns("x", "y").withRows(RowCount(2))

    // CREATE T
    givenGraph { tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("T")) }
    execute(executablePlan) should beColumns("x", "y").withRows(RowCount(3))
  }

  test("should handle arguments spanning two morsels") {
    // NOTE: This is a specific test for pipelined runtime with morsel size _4_
    // where an argument will span two morsels that are put into a MorselBuffer

    // given
    val (a1, a2, b1, b2, b3, c) = givenGraph { smallTestGraph(tx) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b", "c")
      .apply()
      .|.expandAll("(b)-[:R]->(c)")
      .|.expandAll("(a)-[:R]->(b)")
      .|.argument("a")
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val expected = for {
      a <- Seq(a1, a2)
      b <- Seq(b1, b2, b3)
    } yield Array(a, b, c)

    // then
    runtimeResult should beColumns("a", "b", "c").withRows(expected)
  }

  test("should handle single non-existing type for sparse node") {
    // given

    givenGraph {
      val referenceNode = tx.createNode(Label.label("S"))
      (1 to 10).foreach(i => {
        referenceNode.createRelationshipTo(tx.createNode(), RelationshipType.withName("R1"))
        referenceNode.createRelationshipTo(tx.createNode(), RelationshipType.withName("R2"))
      })

    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .expand("(x)-[r:NOPE]->(y)")
      .nodeByLabelScan("x", "S")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "y", "r").withNoRows()
  }

  test("should handle single non-existing type for dense node") {
    // given

    givenGraph {
      val referenceNode = tx.createNode(Label.label("S"))
      (1 to 10).foreach(_ => {
        referenceNode.createRelationshipTo(tx.createNode(), RelationshipType.withName("R1"))
        referenceNode.createRelationshipTo(tx.createNode(), RelationshipType.withName("R2"))
      })

      // Make it a dense node
      (1 to 1000).foreach(_ => {
        referenceNode.createRelationshipTo(tx.createNode(), RelationshipType.withName("R3"))
      })

    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .expand("(x)-[r:NOPE]->(y)")
      .nodeByLabelScan("x", "S")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "y", "r").withNoRows()
  }

  test("should handle multiple types where some are non-existing for sparse node") {
    // given

    givenGraph {
      val referenceNode = tx.createNode(Label.label("S"))
      (1 to 10).foreach(_ => {
        referenceNode.createRelationshipTo(tx.createNode(), RelationshipType.withName("R1"))
        referenceNode.createRelationshipTo(tx.createNode(), RelationshipType.withName("R2"))
      })

      // Make it a dense node
      (1 to 1000).foreach(_ => {
        referenceNode.createRelationshipTo(tx.createNode(), RelationshipType.withName("R3"))
      })

    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .expand("(x)-[r:R1|NOPE]->(y)")
      .nodeByLabelScan("x", "S")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "y", "r").withRows(rowCount(10))
  }

  test("should handle multiple types where some are non-existing for dense node") {
    // given
    givenGraph {
      val referenceNode = tx.createNode(Label.label("S"))
      (1 to 10).foreach(_ => {
        referenceNode.createRelationshipTo(tx.createNode(), RelationshipType.withName("R1"))
        referenceNode.createRelationshipTo(tx.createNode(), RelationshipType.withName("R2"))
      })

      // Make it a dense node
      (1 to 1000).foreach(_ => {
        referenceNode.createRelationshipTo(tx.createNode(), RelationshipType.withName("R3"))
      })

    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .expand("(x)-[r:R1|NOPE]->(y)")
      .nodeByLabelScan("x", "S")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "y", "r").withRows(rowCount(10))
  }
}

// Supported by interpreted, slotted, pipelined, parallel
trait ExpandAllWithOtherOperatorsTestBase[CONTEXT <: RuntimeContext] {
  self: ExpandAllTestBase[CONTEXT] =>

  test("given a null start point, returns an empty iterator") {
    // given
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .expand("(x)-[r]->(y)")
      .optional()
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "y", "r").withNoRows()
  }

  test("should handle arguments spanning two morsels with sort") {
    // NOTE: This is a specific test for pipelined runtime with morsel size _4_
    // where an argument will span two morsels that are put into a MorselBuffer

    val (a1, a2, b1, b2, b3, c) = givenGraph { smallTestGraph(tx) }

    // NOTE: Parallel runtime does not guarantee order is preserved across an apply scope

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b", "c")
      .planIf(isParallel)(_.sort("a ASC")) // Insert a top-level sort in parallel runtime
      .apply()
      .|.sort("a ASC", "b ASC")
      .|.expandAll("(b)-[:R]->(c)")
      .|.expandAll("(a)-[:R]->(b)")
      .|.argument("a")
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val expected = for {
      a <- Seq(a1, a2)
      b <- Seq(b1, b2, b3)
    } yield Array(a, b, c)

    // then
    /*
     There is no defined order coming from the Label Scan, so the test can not assert on a total ordering,
     however there is a defined grouping by argument 'a', and a per-argument ordering on 'b'.
     */
    val rowOrderMatcher = if (isParallel) sortedAsc("a") else groupedBy("a").asc("b")
    runtimeResult should beColumns("a", "b", "c").withRows(rowOrderMatcher)
    runtimeResult should beColumns("a", "b", "c").withRows(expected)
  }

  test("should handle node reference as input") {
    // given
    val n = sizeHint
    val relTuples = (for (i <- 0 until n) yield {
      Seq(
        (i, (2 * i) % n, "OTHER"),
        (i, (i + 1) % n, "NEXT")
      )
    }).reduce(_ ++ _)
    val (nodes, rels) = givenGraph {
      val nodes = nodeGraph(n, "Honey")
      val rels = connect(nodes, relTuples)
      (nodes, rels)
    }

    val input = inputValues(nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .expand("(x)-[r]->(y)")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = relTuples.zip(rels).map {
      case ((f, t, _), rel) => Array(nodes(f), nodes(t), rel)
    }

    runtimeResult should beColumns("x", "y", "r").withRows(expected)
  }

  test("should gracefully handle non-node reference as input") {
    // given
    val n = sizeHint
    val input = inputValues((1 to n).map(Array[Any](_)): _*)
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .expand("(x)-[r]->(y)")
      .input(variables = Seq("x"))
      .build()

    // then
    a[ParameterWrongTypeException] should be thrownBy consume(execute(logicalQuery, runtime, input))
  }

  test("should handle expand + filter") {
    // given
    val size = 1000
    val (_, rels) = givenGraph { circleGraph(size) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .filter(s"id(y) >= ${size / 2}")
      .expandAll("(x)-->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected =
      for {
        r <- rels
        if r.getEndNode.getId >= size / 2
        row <- List(Array(r.getStartNode, r.getEndNode))
      } yield row
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should handle expand + filter on cached property") {
    // given
    val size = 100

    val (aNodes, bNodes) = givenGraph {
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
      .filter("cache[a.prop] < 10")
      .expandAll("(a)-[:R]->(b)")
      .cacheProperties("cache[a.prop]")
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected: Seq[Array[Node]] =
      for {
        a <- aNodes
        if a.getProperty("prop").asInstanceOf[Int] < 10
        b <- bNodes
        row <- List(Array(a, b))
      } yield row

    runtimeResult should beColumns("a", "b").withRows(expected)
  }

  test("should handle chained expands + filters on cached properties") {
    // given
    val size = 100

    val (aNodes, bNodes) = givenGraph {
      bipartiteGraph(
        size,
        "A",
        "B",
        "R",
        aProperties = {
          case i: Int => Map("prop" -> i)
        },
        bProperties = {
          case i: Int => Map("prop" -> i)
        }
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a2", "b")
      .filter("cache[b.prop] < 10 AND cache[a2.prop] < 10")
      .expandAll("(b)<-[:R]-(a2)")
      .filter("cache[a1.prop] < 10 AND cache[b.prop] >= 0")
      .expandAll("(a1)-[:R]->(b)")
      .cacheProperties("cache[a1.prop]")
      .nodeByLabelScan("a1", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected: Seq[Array[Node]] =
      for {
        a1 <- aNodes
        if a1.getProperty("prop").asInstanceOf[Int] < 10
        b <- bNodes
        a2 <- aNodes
        if a2.getProperty("prop").asInstanceOf[Int] < 10 && b.getProperty("prop").asInstanceOf[Int] < 10
        row <- List(Array(a2, b))
      } yield row

    runtimeResult should beColumns("a2", "b").withRows(expected)
  }
}
