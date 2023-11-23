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

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.GraphDatabaseSettings.dense_node_threshold
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RowCount
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.tests.ExpandAllTestBase.smallTestGraph
import org.neo4j.cypher.internal.runtime.spec.tests.ExpandIntoRandomTest.ThinRelationship
import org.neo4j.cypher.internal.util.test_helpers.CypherScalaCheckDrivenPropertyChecks
import org.neo4j.exceptions.ParameterWrongTypeException
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.RelationshipType
import org.scalacheck.Gen

abstract class ExpandIntoTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  protected val sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime)
    with ExpandIntoRandomTest[CONTEXT] {

  test("should expand into and provide variables for relationship - outgoing") {
    // given
    val n = sizeHint
    val relTuples = (for (i <- 0 until n) yield {
      Seq(
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
    val relTuples = (for (i <- 0 until n) yield {
      Seq(
        (i, (i + 2) % n, "SECONDNEXT"),
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
    val relTuples = (for (i <- 0 until n) yield {
      Seq(
        (i, (i + 2) % n, "SECONDNEXT"),
        (i, (i + 1) % n, "NEXT"),
        (i, i, "SELF")
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
    val (_, rels) = givenGraph { circleGraph(sizeHint) }

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
    val (_, rels) = givenGraph { circleGraph(sizeHint) }

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
    val (_, rels) = givenGraph { circleGraph(sizeHint) }

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
    givenGraph {
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
      .expandInto("(x)-[:R|S|T]->(y)")
      .expandAll("(x)--(y)")
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
      .|.expandInto("(a)-[:R]->(b)")
      .|.nodeByLabelScan("b", "B", IndexOrderNone, "a")
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

  test("should support expandInto on RHS of apply") {
    // given
    val size = sizeHint / 16
    val (as, bs) = givenGraph {
      nodeGraph(size, "A")
      nodeGraph(size, "B")
      bipartiteGraph(size, "A", "B", "R")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .apply()
      .|.expandInto("(a)-[:R]->(b)")
      .|.nodeByLabelScan("b", "B", IndexOrderNone, "a")
      .nodeByLabelScan("a", "A", IndexOrderNone)
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
    val (as, bs, as2, bs2) = givenGraph {
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
      .|.nodeByLabelScan("b", "B", IndexOrderNone, "a")
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val expected = (for { a <- as; b <- bs } yield Array(a, b)) ++ (for { a <- as2; b <- bs2 } yield Array(a, b))

    // then
    runtimeResult should beColumns("a", "b").withRows(expected)
  }

  test("expand into with dense nodes without the queried REL_TYPE") {
    val relsToCreate = edition.getSetting(dense_node_threshold).getOrElse(dense_node_threshold.defaultValue()) + 1

    // given
    givenGraph {
      // Two A nodes, and one dense B node.
      val a1 = tx.createNode(Label.label("A"))
      tx.createNode(Label.label("A"))
      val b = tx.createNode(Label.label("B"))
      // b has to be a dense node, but not have any REL relationships
      for (_ <- 1 to relsToCreate) a1.createRelationshipTo(b, RelationshipType.withName("ANOTHER_REL"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .expandInto("(a)-[:REL]->(b)")
      .cartesianProduct()
      .|.nodeByLabelScan("b", "B", IndexOrderNone)
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("a", "b").withNoRows()
  }

  test("expand into in plan with eager and let anti semi apply") {
    val (nodes, rels) = givenGraph {
      val nodes = nodeGraph(3, "A")
      val rels = for {
        a <- nodes
        b <- nodes
      } yield {
        a.createRelationshipTo(b, RelationshipType.withName("R"))
      }
      (nodes, rels)
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("var6", "var0", "var1", "var3", "var2")
      .letAntiSemiApply("var6")
      .|.expandInto("(var4)<-[var5*3..4]-(var1)")
      .|.eager()
      .|.nodeByElementIdSeek(
        "var4",
        Set("var0", "var1", "var2", "var3"),
        nodes(1).getElementId,
        nodes(2).getElementId
      )
      .expandInto("(var1)-[var3:R2|R]->(var2)")
      .sort("var0 ASC", "var1 ASC", "var2 ASC")
      .unionRelationshipTypesScan("(var1)-[var0:R]-(var2)", IndexOrderNone)
      .build()

    val result = execute(query, runtime)

    val expected = Seq(
      Array[Any](false, rels(0), nodes(0), rels(0), nodes(0)),
      Array[Any](false, rels(1), nodes(0), rels(1), nodes(1)),
      Array[Any](false, rels(1), nodes(1), rels(3), nodes(0)),
      Array[Any](false, rels(2), nodes(0), rels(2), nodes(2)),
      Array[Any](false, rels(2), nodes(2), rels(6), nodes(0)),
      Array[Any](false, rels(3), nodes(0), rels(1), nodes(1)),
      Array[Any](false, rels(3), nodes(1), rels(3), nodes(0)),
      Array[Any](false, rels(4), nodes(1), rels(4), nodes(1)),
      Array[Any](false, rels(5), nodes(1), rels(5), nodes(2)),
      Array[Any](false, rels(5), nodes(2), rels(7), nodes(1)),
      Array[Any](false, rels(6), nodes(0), rels(2), nodes(2)),
      Array[Any](false, rels(6), nodes(2), rels(6), nodes(0)),
      Array[Any](false, rels(7), nodes(1), rels(5), nodes(2)),
      Array[Any](false, rels(7), nodes(2), rels(7), nodes(1)),
      Array[Any](false, rels(8), nodes(2), rels(8), nodes(2))
    )
    result should beColumns("var6", "var0", "var1", "var3", "var2").withRows(inAnyOrder(expected))
  }
}

// Supported by interpreted, slotted, pipelined, parallel
trait ExpandIntoWithOtherOperatorsTestBase[CONTEXT <: RuntimeContext] {
  self: ExpandIntoTestBase[CONTEXT] =>

  test("should handle arguments spanning two morsels with sort") {
    // NOTE: This is a specific test for pipelined runtime with morsel size _4_
    // where an argument will span two morsels that are put into a MorselBuffer

    val (a1, a2, b1, b2, b3, c) = givenGraph { smallTestGraph(tx) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b", "c")
      .apply()
      .|.sort("a ASC", "b ASC")
      .|.expandAll("(b)-[:R]->(c)")
      .|.expandInto("(a)-[:R]->(b)")
      .|.nodeByLabelScan("b", "B", IndexOrderNone, "a")
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
    runtimeResult should beColumns("a", "b", "c").withRows(groupedBy("a").asc("b"))
    runtimeResult should beColumns("a", "b", "c").withRows(expected)
  }

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

  test("should handle node reference as input") {
    // given
    val n = sizeHint
    val relTuples = (for (i <- 0 until n) yield {
      Seq(
        (i, (i + 1) % n, "NEXT")
      )
    }).reduce(_ ++ _)
    val (nodes, rels) = givenGraph {
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
    val input = inputValues((1 to n).map(i => Array[Any](i, i + 1)): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .expandInto("(x)-[r]->(y)")
      .input(variables = Seq("x", "y"))
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
      .expandInto("(x)-->(y)")
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

  test("should filter on cached relationship property") {
    // given
    val rels = givenGraph {
      val (_, rs) = circleGraph(sizeHint)
      rs.indices.foreach(i => rs(i).setProperty("prop", i))
      rs
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .filter("r2.prop = cacheR[r1.prop]")
      .expandInto("(x)-[r2]-(y)")
      .cacheProperties("cacheR[r1.prop]")
      .expandAll("(x)<-[r1]-(y)")
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

  test("two connected dense nodes where one node appears twice I") {
    // given
    val (a, b) = givenGraph {
      val a = tx.createNode(Label.label("A"))
      makeDense(a)

      val ba = tx.createNode(Label.label("B"), Label.label("A"))
      // make it dense
      makeDense(ba)
      val b = tx.createNode(Label.label("B"))
      makeDense(b)
      ba.createRelationshipTo(b, RelationshipType.withName("T"))
      (ba, b)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .expandInto("(a)-[r:T]->(b)")
      .cartesianProduct()
      .|.nodeByLabelScan("b", "B", "a")
      .nodeByLabelScan("a", "A")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("a", "b").withSingleRow(a, b)
  }

  test("two connected dense nodes where one node appears twice II") {
    // given
    val (a, b) = givenGraph {
      val a = tx.createNode(Label.label("A"), Label.label("B"))
      makeDense(a)
      a.createRelationshipTo(tx.createNode(Label.label("FOO")), RelationshipType.withName("T"))
      val b = tx.createNode(Label.label("B"))
      makeDense(b)
      a.createRelationshipTo(b, RelationshipType.withName("T"))
      (a, b)

    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .expandInto("(a)-[r:T]->(b)")
      .cartesianProduct()
      .|.nodeByLabelScan("b", "B", "a")
      .nodeByLabelScan("a", "A")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("a", "b").withSingleRow(a, b)
  }

  test("two connected dense nodes where one node appears twice III") {
    // given
    val (a, b) = givenGraph {
      val b = tx.createNode(Label.label("B"))
      val ab = tx.createNode(Label.label("A"), Label.label("B"))
      val a = tx.createNode(Label.label("A"))

      makeDense(a)
      makeDense(ab)
      makeDense(b)
      tx.createNode(Label.label("FOO")).createRelationshipTo(b, RelationshipType.withName("T"))

      a.createRelationshipTo(ab, RelationshipType.withName("T"))
      (a, ab)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .expandInto("(a)-[r:T]->(b)")
      .cartesianProduct()
      .|.nodeByLabelScan("b", "B", "a")
      .nodeByLabelScan("a", "A")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("a", "b").withSingleRow(a, b)
  }

  test("one dense and one sparse node where one node appears twice I") {
    // given
    val (a, b) = givenGraph {
      val a = tx.createNode(Label.label("A"))
      makeDense(a)
      val ba = tx.createNode(Label.label("B"), Label.label("A"))
      makeDense(ba)
      val b = tx.createNode(Label.label("B"))
      ba.createRelationshipTo(b, RelationshipType.withName("T"))
      (ba, b)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .expandInto("(a)-[r:T]->(b)")
      .cartesianProduct()
      .|.nodeByLabelScan("b", "B", "a")
      .nodeByLabelScan("a", "A")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("a", "b").withSingleRow(a, b)
  }

  test("one dense node and one sparse node where one node appears twice II") {
    // given
    val (a, b) = givenGraph {
      val a = tx.createNode(Label.label("A"), Label.label("B"))
      makeDense(a)
      a.createRelationshipTo(tx.createNode(Label.label("FOO")), RelationshipType.withName("T"))
      val b = tx.createNode(Label.label("B"))
      a.createRelationshipTo(b, RelationshipType.withName("T"))
      (a, b)

    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .expandInto("(a)-[r:T]->(b)")
      .cartesianProduct()
      .|.nodeByLabelScan("b", "B", "a")
      .nodeByLabelScan("a", "A")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("a", "b").withSingleRow(a, b)
  }

  test("one dense node and one sparse node where one node appears twice III") {
    // given
    val (a, b) = givenGraph {
      val b = tx.createNode(Label.label("B"))
      val ab = tx.createNode(Label.label("A"), Label.label("B"))
      val a = tx.createNode(Label.label("A"))

      makeDense(a)
      makeDense(ab)
      tx.createNode(Label.label("FOO")).createRelationshipTo(b, RelationshipType.withName("T"))

      a.createRelationshipTo(ab, RelationshipType.withName("T"))
      (a, ab)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .expandInto("(a)-[r:T]->(b)")
      .cartesianProduct()
      .|.nodeByLabelScan("b", "B", "a")
      .nodeByLabelScan("a", "A")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("a", "b").withSingleRow(a, b)
  }

  test("one sparse and one dense node where one node appears twice I") {
    // given
    val (a, b) = givenGraph {
      tx.createNode(Label.label("A"))
      val ba = tx.createNode(Label.label("B"), Label.label("A"))
      val b = tx.createNode(Label.label("B"))
      makeDense(b)
      ba.createRelationshipTo(b, RelationshipType.withName("T"))
      (ba, b)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .expandInto("(a)-[r:T]->(b)")
      .cartesianProduct()
      .|.nodeByLabelScan("b", "B", "a")
      .nodeByLabelScan("a", "A")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("a", "b").withSingleRow(a, b)
  }

  test("one sparse node and one dense node where one node appears twice II") {
    // given
    val (a, b) = givenGraph {
      val a = tx.createNode(Label.label("A"), Label.label("B"))
      a.createRelationshipTo(tx.createNode(Label.label("FOO")), RelationshipType.withName("T"))
      val b = tx.createNode(Label.label("B"))
      makeDense(b)
      a.createRelationshipTo(b, RelationshipType.withName("T"))
      (a, b)

    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .expandInto("(a)-[r:T]->(b)")
      .cartesianProduct()
      .|.nodeByLabelScan("b", "B", "a")
      .nodeByLabelScan("a", "A")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("a", "b").withSingleRow(a, b)
  }

  test("one sparse node and dense node where one node appears twice III") {
    // given
    val (a, b) = givenGraph {
      val b = tx.createNode(Label.label("B"))
      val ab = tx.createNode(Label.label("A"), Label.label("B"))
      val a = tx.createNode(Label.label("A"))

      makeDense(b)
      tx.createNode(Label.label("FOO")).createRelationshipTo(b, RelationshipType.withName("T"))

      a.createRelationshipTo(ab, RelationshipType.withName("T"))
      (a, ab)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .expandInto("(a)-[r:T]->(b)")
      .cartesianProduct()
      .|.nodeByLabelScan("b", "B", "a")
      .nodeByLabelScan("a", "A")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("a", "b").withSingleRow(a, b)
  }

  test("two connected sparse nodes where one node appears twice I") {
    // given
    val (a, b) = givenGraph {
      tx.createNode(Label.label("A"))
      val ba = tx.createNode(Label.label("B"), Label.label("A"))
      val b = tx.createNode(Label.label("B"))
      ba.createRelationshipTo(b, RelationshipType.withName("T"))
      (ba, b)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .expandInto("(a)-[r:T]->(b)")
      .cartesianProduct()
      .|.nodeByLabelScan("b", "B", "a")
      .nodeByLabelScan("a", "A")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("a", "b").withSingleRow(a, b)
  }

  test("wo connected sparse nodes where one node appears twice II") {
    // given
    val (a, b) = givenGraph {
      val a = tx.createNode(Label.label("A"), Label.label("B"))
      a.createRelationshipTo(tx.createNode(Label.label("FOO")), RelationshipType.withName("T"))
      val b = tx.createNode(Label.label("B"))
      a.createRelationshipTo(b, RelationshipType.withName("T"))
      (a, b)

    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .expandInto("(a)-[r:T]->(b)")
      .cartesianProduct()
      .|.nodeByLabelScan("b", "B", "a")
      .nodeByLabelScan("a", "A")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("a", "b").withSingleRow(a, b)
  }

  test("two connected sparse nodes where one node appears twice III") {
    // given
    val (a, b) = givenGraph {
      val b = tx.createNode(Label.label("B"))
      val ab = tx.createNode(Label.label("A"), Label.label("B"))
      val a = tx.createNode(Label.label("A"))

      tx.createNode(Label.label("FOO")).createRelationshipTo(b, RelationshipType.withName("T"))

      a.createRelationshipTo(ab, RelationshipType.withName("T"))
      (a, ab)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .expandInto("(a)-[r:T]->(b)")
      .cartesianProduct()
      .|.nodeByLabelScan("b", "B", "a")
      .nodeByLabelScan("a", "A")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("a", "b").withSingleRow(a, b)
  }

  test("two connected sparse nodes where one node appears twice IV") {
    // given
    val (a, b) = givenGraph {
      val a = tx.createNode(Label.label("A"))
      val ab = tx.createNode(Label.label("A"), Label.label("B"))
      val ba = tx.createNode(Label.label("B"), Label.label("A"))
      makeDense(a)

      a.createRelationshipTo(tx.createNode(Label.label("FOO")), RelationshipType.withName("T"))
      ab.createRelationshipTo(ba, RelationshipType.withName("T"))
      (ab, ba)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .expandInto("(a)-[r:T]->(b)")
      .cartesianProduct()
      .|.nodeByLabelScan("b", "B", "a")
      .nodeByLabelScan("a", "A")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("a", "b").withSingleRow(a, b)
  }

  test("should handle doubly-connected loop with skip and limit") {

    //         (a3)
    //        ↙↗  ↖↘
    //     (a2)    (a4)
    //    ↙↗          ↖↘
    //  (a1)          (a5)
    //      ↖↘      ↙↗
    //         (a7)
    givenGraph {
      val nNodes = 7
      val rType = RelationshipType.withName("R")

      val nodes = for (_ <- 0 until nNodes) yield {
        runtimeTestSupport.tx.createNode()
      }
      for (i <- 0 until nNodes) {
        val a = nodes(i)
        val b = nodes((i + 1) % nNodes)
        a.createRelationshipTo(b, rType)
        b.createRelationshipTo(a, rType)
      }
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("ID")
      .projection("id(a1) AS ID")
      .skip(2)
      .limit(3)
      .filter("r2 <> r7", "r1 <> r7", "r7 <> r6", "r7 <> r5", "r7 <> r4", "r7 <> r3")
      .expandInto("(a7)-[r7]-(a1)")
      .filter("r2 <> r6", "r1 <> r6", "r6 <> r5", "r6 <> r4", "r6 <> r3")
      .expandAll("(a6)-[r6]-(a7)")
      .filter("r2 <> r5", "r1 <> r5", "r5 <> r4", "r5 <> r3")
      .expandAll("(a5)-[r5]-(a6)")
      .filter("r2 <> r4", "r1 <> r4", "r4 <> r3")
      .expandAll("(a4)-[r4]-(a5)")
      .filter("r2 <> r3", "r1 <> r2")
      .expandAll("(a3)-[r3]-(a4)")
      .filter("r2 <> r1")
      .expandAll("(a2)-[r2]-(a3)")
      .allRelationshipsScan("(a1)-[r1]->(a2)")
      .build()
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("ID").withRows(rowCount(1))
  }

  private def makeDense(node: Node): Unit = {
    (1 to GraphDatabaseSettings.dense_node_threshold.defaultValue() + 1).foreach(_ =>
      node.createRelationshipTo(tx.createNode(Label.label("IGNORE")), RelationshipType.withName("IGNORE"))
    )
  }
}

/**
 * Tests expand into with random graphs.
 */
trait ExpandIntoRandomTest[CONTEXT <: RuntimeContext] extends CypherScalaCheckDrivenPropertyChecks {
  self: RuntimeTestSuite[CONTEXT] =>

  test("expand into should handle random graphs") {
    // Plan that tries to stress the expand into caches
    def expandIntoPlan(relPattern: String) = {
      new LogicalQueryBuilder(this)
        .produceResults("from", "to", "rel")
        .projection("elementId(a) AS from", "elementId(b) AS to", "elementId(r) AS rel")
        .expandInto(s"(a)$relPattern(b)")
        .sort("rand ASC")
        .projection("rand() as rand")
        .unwind("[0,1] as x")
        .cartesianProduct()
        .|.allNodeScan("b")
        .allNodeScan("a")
        .build()
    }
    val aLikesBPlan = expandIntoPlan("-[r:LIKES]->")
    val bLikesAPlan = expandIntoPlan("<-[r:LIKES]-")
    val aLikesBOrBLikesAPlan = expandIntoPlan("-[r:LIKES]-")

    val aLovesB = expandIntoPlan("-[r:LOVES]->")
    val aLikesOrLovesB = expandIntoPlan("-[r:LIKES|LOVES]->")

    forAll(genRandomGraph(Seq("LIKES", "LOVES")), minSuccessful(20)) { createGraph =>
      val relationships = givenGraph {
        // Clean previous data
        tx.getAllRelationships.forEach(r => r.delete())
        tx.getAllNodes.forEach(n => n.delete())

        createGraph()
      }

      def expected(relTypePredicate: String => Boolean) = relationships
        .filter(r => relTypePredicate(r.relType))
        .map(r => Array(r.from, r.to, r.id))
        .flatMap(r => Seq(r, r)) // The unwind

      execute(aLikesBPlan, runtime) should beColumns("from", "to", "rel")
        .withRows(inAnyOrder(expected(_ == "LIKES")))
      val expectedReversed = relationships.filter(_.relType == "LIKES")
        .map(r => Array(r.to, r.from, r.id))
        .flatMap(r => Seq(r, r)) // The unwind
      execute(bLikesAPlan, runtime) should beColumns("from", "to", "rel")
        .withRows(inAnyOrder(expectedReversed))

      val expectedBoth = relationships
        .filter(_.relType == "LIKES")
        .flatMap {
          case r if r.from == r.to => Seq(Array(r.from, r.to, r.id))
          case r                   => Seq(Array(r.from, r.to, r.id), Array(r.to, r.from, r.id))
        }
        .flatMap(r => Seq(r, r)) // The unwind
      execute(aLikesBOrBLikesAPlan, runtime) should beColumns("from", "to", "rel")
        .withRows(inAnyOrder(expectedBoth))

      execute(aLovesB, runtime) should beColumns("from", "to", "rel")
        .withRows(inAnyOrder(expected(_ == "LOVES")))

      execute(aLikesOrLovesB, runtime) should beColumns("from", "to", "rel")
        .withRows(inAnyOrder(expected(relType => relType == "LIKES" || relType == "LOVES")))
    }
  }

  private def genRandomGraph(relTypes: Seq[String]): Gen[() => Seq[ThinRelationship]] = {
    val minNodes = 10
    val maxNodes = 40
    val relationshipProb = 0.1
    for {
      nodeCount <- Gen.choose(minNodes, maxNodes)
      shouldRelate <- Gen.infiniteStream(Gen.prob(relationshipProb)).map(_.iterator)
    } yield {
      () =>
        val nodes = Range(0, nodeCount)
          .map(_ => tx.createNode())
          .toIndexedSeq

        for {
          relTypeName <- relTypes :+ "Unrelated"
          relType = RelationshipType.withName(relTypeName)
          nodeA <- nodes
          nodeB <- nodes
          if shouldRelate.next()
        } yield {
          val rel = nodeA.createRelationshipTo(nodeB, relType)
          ThinRelationship(rel.getElementId, relTypeName, nodeA.getElementId, nodeB.getElementId)
        }
    }
  }
}

object ExpandIntoRandomTest {
  case class ThinRelationship(id: String, relType: String, from: String, to: String)
}
