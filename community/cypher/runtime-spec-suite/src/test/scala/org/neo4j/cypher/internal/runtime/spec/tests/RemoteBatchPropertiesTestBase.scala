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
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.TrailParameters
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeWithProperties
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RecordingRuntimeResult
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSupport
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSupport.WorkloadMode
import org.neo4j.cypher.internal.runtime.spec.tests.index.PropertyIndexTestSupport
import org.neo4j.cypher.internal.util.UpperBound
import org.neo4j.cypher.internal.util.UpperBound.Limited
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Label.label
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.RelationshipType.withName
import org.neo4j.graphdb.schema.IndexType
import org.neo4j.internal.helpers.collection.Iterables
import org.neo4j.kernel.api.KernelTransaction.Type
import org.neo4j.logging.InternalLogProvider

import java.util.Collections.emptyList

object RemoteBatchPropertiesTestBase

abstract class RemoteBatchPropertiesTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  val sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should return one node property column - on tiny graph") {
    givenGraph {
      tx.createNode(Label.label("L")).setProperty("prop", 10)
      tx.createNode(Label.label("L")).setProperty("prop", 20)
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("cache[x.prop] as prop")
      .remoteBatchProperties("cache[x.prop]")
      .nodeByLabelScan("x", "L")
      .build()

    val result = execute(query, runtime)
    result should beColumns("prop").withRows(singleColumn(Seq(10, 20)))
  }

  test("should return two node properties columns - on tiny graph") {
    givenGraph {
      val n1 = tx.createNode()
      n1.setProperty("prop1", 10)
      n1.setProperty("prop2", 11)
      val n2 = tx.createNode()
      n2.setProperty("prop1", 20)
      n2.setProperty("prop2", 21)
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("prop1", "prop2")
      .projection("cache[x.prop1] as prop1", "cache[x.prop2] as prop2")
      .remoteBatchProperties("cache[x.prop1]", "cache[x.prop2]")
      .allNodeScan("x")
      .build()

    val result = execute(query, runtime)
    result should beColumns("prop1", "prop2").withRows(Seq(Array(10, 11), Array(20, 21)))
  }

  test("should return two node properties columns - on tiny graph with many properties") {
    givenGraph {
      val n1 = tx.createNode()
      val n2 = tx.createNode()
      for (i <- 1 to 1000) {
        n1.setProperty(s"prop$i", s"n1:$i")
        n2.setProperty(s"prop$i", s"n2:$i")
      }
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("prop1", "prop1000")
      .projection("cache[x.prop1] as prop1", "cache[x.prop1000] as prop1000")
      .remoteBatchProperties("cache[x.prop1]", "cache[x.prop1000]")
      .allNodeScan("x")
      .build()

    val result = execute(query, runtime)
    result should beColumns("prop1", "prop1000").withRows(Seq(Array("n1:1", "n1:1000"), Array("n2:1", "n2:1000")))
  }

  test("should return two node property columns with one null value - on tiny graph") {
    givenGraph {
      val n1 = tx.createNode()
      n1.setProperty("prop1", 10)
      n1.setProperty("prop2", 11)
      val n2 = tx.createNode()
      n2.setProperty("prop1", 20)
      // n2.prop2 is null
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("prop1", "prop2")
      .projection("cache[x.prop1] as prop1", "cache[x.prop2] as prop2")
      .remoteBatchProperties("cache[x.prop1]", "cache[x.prop2]")
      .allNodeScan("x")
      .build()

    val result = execute(query, runtime)
    result should beColumns("prop1", "prop2").withRows(Seq(Array[Any](10, 11), Array[Any](20, null)))
  }

  test(s"should return nothing on empty graph") {
    val query = new LogicalQueryBuilder(this)
      .produceResults("prop1", "prop2")
      .projection("cache[x.prop1] as prop1", "cache[x.prop2] as prop2")
      .remoteBatchProperties("cache[x.prop1]", "cache[x.prop2]")
      .allNodeScan("x")
      .build()

    val result = execute(query, runtime)
    result should beColumns("prop1", "prop2").withNoRows()
  }

  test(s"should return two node properties columns") {
    givenGraph {
      nodePropertyGraph(sizeHint, { case i => Map("prop1" -> i, "prop2" -> i * 2) })
    }
    val query = new LogicalQueryBuilder(this)
      .produceResults("prop1", "prop2")
      .projection("cache[x.prop1] as prop1", "cache[x.prop2] as prop2")
      .remoteBatchProperties("cache[x.prop1]", "cache[x.prop2]")
      .allNodeScan("x")
      .build()

    val result = execute(query, runtime)
    val expected = (0 until sizeHint).map(i => Array(i, i * 2))
    result should beColumns("prop1", "prop2").withRows(expected)
  }

  test("should return two node properties columns with nulls") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i if i % 3 == 0 => Map("prop1" -> i, "prop2" -> i * 2)
          case i if i % 3 == 1 => Map("prop1" -> i)
          case i if i % 3 == 2 => Map("prop2" -> i * 2)
        }
      )
    }
    val query = new LogicalQueryBuilder(this)
      .produceResults("prop1", "prop2")
      .projection("cache[x.prop1] as prop1", "cache[x.prop2] as prop2")
      .remoteBatchProperties("cache[x.prop1]", "cache[x.prop2]")
      .allNodeScan("x")
      .build()

    val result = execute(query, runtime)
    val expected: Seq[Array[Any]] = (0 until sizeHint).map {
      case i if i % 3 == 0 => Array(i, i * 2)
      case i if i % 3 == 1 => Array(i, null)
      case i if i % 3 == 2 => Array(null, i * 2)
    }
    result should beColumns("prop1", "prop2").withRows(expected)
  }

  test("should return one node property columns when under Apply") {
    givenGraph {
      nodePropertyGraph(sizeHint, { case i => Map("prop1" -> i, "prop2" -> i * 2) })
    }
    val query = new LogicalQueryBuilder(this)
      .produceResults("prop1", "prop2")
      .apply()
      .|.projection("cache[x.prop1] as prop1", "cache[x.prop2] as prop2")
      .|.remoteBatchProperties("cache[x.prop1]", "cache[x.prop2]")
      .|.argument("x")
      .allNodeScan("x")
      .build()

    val result = execute(query, runtime)
    val expected = (0 until sizeHint).map(i => Array(i, i * 2))
    result should beColumns("prop1", "prop2").withRows(expected)
  }

  test("should return one node property columns when under Apply with Sort") {
    givenGraph {
      nodePropertyGraph(sizeHint, { case i => Map("prop1" -> i, "prop2" -> i * 2) })
    }
    val query = new LogicalQueryBuilder(this)
      .produceResults("prop1", "prop2")
      .apply()
      .|.sort("prop1 ASC", "prop2 ASC")
      .|.projection("cache[x.prop1] as prop1", "cache[x.prop2] as prop2")
      .|.remoteBatchProperties("cache[x.prop1]", "cache[x.prop2]")
      .|.argument("x")
      .allNodeScan("x")
      .build()

    val result = execute(query, runtime)
    val expected = (0 until sizeHint).map(i => Array(i, i * 2))
    result should beColumns("prop1", "prop2").withRows(expected)
  }

  test("should return one node property columns when under Apply with Sort, also on top") {
    givenGraph {
      nodePropertyGraph(sizeHint, { case i => Map("prop1" -> i, "prop2" -> i * 2, "prop3" -> i * 3) })
    }
    val query = new LogicalQueryBuilder(this)
      .produceResults("prop1", "prop2")
      .remoteBatchProperties("cache[x.prop3]")
      .apply()
      .|.sort("prop1 ASC", "prop2 ASC")
      .|.projection("cache[x.prop1] as prop1", "cache[x.prop2] as prop2")
      .|.remoteBatchProperties("cache[x.prop1]", "cache[x.prop2]")
      .|.argument("x")
      .allNodeScan("x")
      .build()

    val result = execute(query, runtime)
    val expected = (0 until sizeHint).map(i => Array(i, i * 2))
    result should beColumns("prop1", "prop2").withRows(expected)
  }

  test("should return one node property columns when split into two plans under Apply with Sort") {
    givenGraph {
      nodePropertyGraph(sizeHint, { case i => Map("prop1" -> i, "prop2" -> i * 2) })
    }
    val query = new LogicalQueryBuilder(this)
      .produceResults("prop1", "prop2")
      .apply()
      .|.sort("prop1 ASC", "prop2 ASC")
      .|.projection("cache[x.prop1] as prop1", "cache[x.prop2] as prop2")
      .|.remoteBatchProperties("cache[x.prop2]")
      .|.remoteBatchProperties("cache[x.prop1]")
      .|.argument("x")
      .allNodeScan("x")
      .build()

    val result = execute(query, runtime)
    val expected = (0 until sizeHint).map(i => Array(i, i * 2))
    result should beColumns("prop1", "prop2").withRows(expected)
  }

  test(s"should handle missing long entities") {
    // given
    val size = 10
    val nodes = givenGraph { nodeGraph(size) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .projection("cacheN[m.p] AS x", "cacheR[r.p] AS y")
      .remoteBatchProperties("cacheN[m.p]", "cacheR[r.p]")
      .optionalExpandAll("(n)-[r]->(m)")
      .input(nodes = Seq("n"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(nodes.map(n => Array[Any](n)): _*))

    // then
    val expected = nodes.map(_ => Array[Any](null, null))
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test(s"should handle missing ref entities") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .projection("cacheN[n.p] AS x", "cacheR[r.p] AS y")
      .remoteBatchProperties("cacheN[n.p]", "cacheR[r.p]")
      .input(variables = Seq("n", "r"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(Array(null, null)))

    // then
    runtimeResult should beColumns("x", "y").withSingleRow(null, null)
  }

  test(s"should handle missing property token") {
    // given
    val size = 10
    val nodes = givenGraph { nodeGraph(size) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .projection("cache[n.p] AS x")
      .remoteBatchProperties("cache[n.p]")
      .allNodeScan("n")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.map(_ => Array[Any](null))
    runtimeResult should beColumns("x").withRows(expected)
  }

  test(s"should handle missing property value") {
    // given
    val size = 10
    givenGraph { nodePropertyGraph(size, { case i => if (i % 2 == 0) Map() else Map("p" -> i) }) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .projection("cache[n.p] AS x")
      .remoteBatchProperties("cache[n.p]")
      .allNodeScan("n")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Array[Any](null, 1, null, 3, null, 5, null, 7, null, 9)
    runtimeResult should beColumns("x").withRows(singleColumn(expected))
  }

  test("should handle duplicated cached properties on rhs of nested cartesian product") {
    givenGraph {
      val n1 = tx.createNode(Label.label("A"))
      n1.setProperty("p", 10)
      n1.setProperty("p2", 11)
      val n2 = tx.createNode(Label.label("A"))
      n2.setProperty("p", 20)
      val n3 = tx.createNode(Label.label("C"))
      n3.setProperty("p3", 30)
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("p3")
      .projection("cache[n3.p3] as p3")
      .remoteBatchProperties("cache[n3.p3]")
      .apply()
      .|.cartesianProduct()
      .|.|.cartesianProduct()
      .|.|.|.filter("cache[n1.p2] = 11")
      .|.|.|.remoteBatchProperties("cache[n1.p2]")
      .|.|.|.nodeByLabelScan("n3", "C", "n1")
      .|.|.remoteBatchProperties("cache[n1.p2]")
      .|.|.argument("n1")
      .|.filter("cache[n2.p] = 20")
      .|.remoteBatchProperties("cache[n2.p]")
      .|.allNodeScan("n2")
      .allNodeScan("n1")
      .build()

    val result = execute(query, runtime)

    result should beColumns("p3").withSingleRow(30)
  }

  test("should handle duplicated cached properties on rhs of cartesian product with additional slots") {
    givenGraph {
      val n1 = tx.createNode(Label.label("A"))
      n1.setProperty("p", 10)
      n1.setProperty("p2", 11)
      val n2 = tx.createNode(Label.label("A"))
      n2.setProperty("p", 20)
      val n3 = tx.createNode(Label.label("C"))
      n3.setProperty("p3", 30)
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("p3")
      .projection("cache[n3.p3] as p3")
      .remoteBatchProperties("cache[n3.p3]")
      .apply()
      .|.cartesianProduct()
      .|.|.projection("2 as p5")
      .|.|.cartesianProduct()
      .|.|.|.projection("1 as p4")
      .|.|.|.filter("cache[n1.p2] = 11")
      .|.|.|.remoteBatchProperties("cache[n1.p2]")
      .|.|.|.nodeByLabelScan("n3", "C", "n1")
      .|.|.remoteBatchProperties("cache[n1.p2]")
      .|.|.argument("n1")
      .|.filter("cache[n2.p] = 20")
      .|.remoteBatchProperties("cache[n2.p]")
      .|.allNodeScan("n2")
      .allNodeScan("n1")
      .build()

    val result = execute(query, runtime)

    result should beColumns("p3").withSingleRow(30)
  }

  test("should handle duplicated cached properties on rhs of nested cartesian product with union on top") {
    givenGraph {
      val n1 = tx.createNode(Label.label("A"))
      n1.setProperty("p", 10)
      n1.setProperty("p2", 11)
      val n2 = tx.createNode(Label.label("A"))
      n2.setProperty("p", 20)
      val n3 = tx.createNode(Label.label("C"))
      n3.setProperty("p3", 30)
      val n4 = tx.createNode(Label.label("D"))
      n4.setProperty("p4", 40)
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .union()
      .|.projection("cache[n4.p4] as prop")
      .|.filter("cache[n4.p4] = 40")
      .|.remoteBatchProperties("cache[n4.p4]")
      .|.allNodeScan("n4")
      .projection("cache[n3.p3] as prop")
      .remoteBatchProperties("cache[n3.p3]")
      .apply()
      .|.cartesianProduct()
      .|.|.cartesianProduct()
      .|.|.|.filter("cache[n1.p2] = 11")
      .|.|.|.remoteBatchProperties("cache[n1.p2]")
      .|.|.|.nodeByLabelScan("n3", "C")
      .|.|.remoteBatchProperties("cache[n1.p2]")
      .|.|.argument("n1")
      .|.filter("cache[n2.p] = 20")
      .|.remoteBatchProperties("cache[n2.p]")
      .|.allNodeScan("n2")
      .allNodeScan("n1")
      .build()

    val result = execute(query, runtime)

    result should beColumns("prop").withRows(inAnyOrder(Seq(Array(30), Array(40))))
  }

  test(s"should work with trail(repeat) single hop") {
    givenGraph {
      val (nodes, _) = circleGraph(nNodes = sizeHint, relType = "R", outDegree = 1)
      for (node <- nodes) {
        node.setProperty("foo", 42)
      }
    }

    val `(start) [(a)-[r]->(b)]{1,1} (end)` = TrailParameters(
      min = 1,
      max = Limited(1),
      start = "start",
      end = "end",
      innerStart = "a_inner",
      innerEnd = "b_inner",
      groupNodes = Set(("a_inner", "a"), ("b_inner", "b")),
      groupRelationships = Set(("r_inner", "r")),
      innerRelationships = Set("r_inner"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("count(*) AS c"))
      .repeatTrail(`(start) [(a)-[r]->(b)]{1,1} (end)`)
      .|.filter(isRepeatTrailUnique("r_inner"))
      .|.remoteBatchProperties("cache[b_inner.foo]")
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("start", "a_inner")
      .allNodeScan("start")
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("c").withSingleRow(sizeHint)
  }

  test(s"should work with trail(repeat) multiple hops") {
    givenGraph {
      val (nodes, _) = circleGraph(nNodes = sizeHint, relType = "R", outDegree = 1)
      for (node <- nodes) {
        node.setProperty("foo", 42)
      }
    }

    val `(start) [(a)-[r]->(b)]{1,3} (end)` = TrailParameters(
      min = 1,
      max = Limited(3),
      start = "start",
      end = "end",
      innerStart = "a_inner",
      innerEnd = "b_inner",
      groupNodes = Set(("a_inner", "a"), ("b_inner", "b")),
      groupRelationships = Set(("r_inner", "r")),
      innerRelationships = Set("r_inner"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("count(*) AS c"))
      .repeatTrail(`(start) [(a)-[r]->(b)]{1,3} (end)`)
      .|.filter(isRepeatTrailUnique("r_inner"))
      .|.remoteBatchProperties("cache[b_inner.foo]")
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("start", "a_inner")
      .allNodeScan("start")
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("c").withSingleRow(sizeHint * 3)
  }

  test("should work with trail(repeat) including zero repetition") {
    givenGraph {
      val (nodes, _) = circleGraph(nNodes = sizeHint, relType = "R", outDegree = 1)
      for (node <- nodes) {
        node.setProperty("foo", 42)
      }
    }

    val `(start) [(a)-[r]->(b)]{0,1} (end)` = TrailParameters(
      min = 0,
      max = Limited(1),
      start = "start",
      end = "end",
      innerStart = "a_inner",
      innerEnd = "b_inner",
      groupNodes = Set(("a_inner", "a"), ("b_inner", "b")),
      groupRelationships = Set(("r_inner", "r")),
      innerRelationships = Set("r_inner"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("count(*) AS c"))
      .repeatTrail(`(start) [(a)-[r]->(b)]{0,1} (end)`)
      .|.filter(isRepeatTrailUnique("r_inner"))
      .|.remoteBatchProperties("cache[b_inner.foo]")
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("start", "a_inner")
      .allNodeScan("start")
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("c").withSingleRow(sizeHint * 2)
  }

  test("should work with trail(repeat) single hop, also on top") {
    givenGraph {
      val (nodes, _) = circleGraph(nNodes = sizeHint, relType = "R", outDegree = 1)
      for (node <- nodes) {
        node.setProperty("foo", 42)
      }
    }

    val `(start) [(a)-[r]->(b)]{1,1} (end)` = TrailParameters(
      min = 1,
      max = Limited(1),
      start = "start",
      end = "end",
      innerStart = "a_inner",
      innerEnd = "b_inner",
      groupNodes = Set(("a_inner", "a"), ("b_inner", "b")),
      groupRelationships = Set(("r_inner", "r")),
      innerRelationships = Set("r_inner"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("count(*) AS c"))
      .remoteBatchProperties("cache[end.foo]")
      .repeatTrail(`(start) [(a)-[r]->(b)]{1,1} (end)`)
      .|.filter(isRepeatTrailUnique("r_inner"))
      .|.remoteBatchProperties("cache[b_inner.foo]")
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("start", "a_inner")
      .allNodeScan("start")
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("c").withSingleRow(sizeHint)
  }

  test("should work with trail(repeat) multiple hops, also on top") {
    givenGraph {
      val (nodes, _) = circleGraph(nNodes = sizeHint, relType = "R", outDegree = 1)
      for (node <- nodes) {
        node.setProperty("foo", 42)
      }
    }

    val `(start) [(a)-[r]->(b)]{1,3} (end)` = TrailParameters(
      min = 1,
      max = Limited(3),
      start = "start",
      end = "end",
      innerStart = "a_inner",
      innerEnd = "b_inner",
      groupNodes = Set(("a_inner", "a"), ("b_inner", "b")),
      groupRelationships = Set(("r_inner", "r")),
      innerRelationships = Set("r_inner"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("count(*) AS c"))
      .remoteBatchProperties("cache[end.foo]")
      .repeatTrail(`(start) [(a)-[r]->(b)]{1,3} (end)`)
      .|.filter(isRepeatTrailUnique("r_inner"))
      .|.remoteBatchProperties("cache[b_inner.foo]")
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("start", "a_inner")
      .allNodeScan("start")
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("c").withSingleRow(sizeHint * 3)
  }

  test("should work with trail(repeat) multiple hops, also below and on top") {
    givenGraph {
      val (nodes, _) = circleGraph(nNodes = sizeHint, relType = "R", outDegree = 1)
      for (node <- nodes) {
        node.setProperty("foo", 42)
      }
    }

    val `(start) [(a)-[r]->(b)]{1,3} (end)` = TrailParameters(
      min = 1,
      max = Limited(3),
      start = "start",
      end = "end",
      innerStart = "a_inner",
      innerEnd = "b_inner",
      groupNodes = Set(("a_inner", "a"), ("b_inner", "b")),
      groupRelationships = Set(("r_inner", "r")),
      innerRelationships = Set("r_inner"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("count(*) AS c"))
      .remoteBatchProperties("cache[end.foo]")
      .repeatTrail(`(start) [(a)-[r]->(b)]{1,3} (end)`)
      .|.filter(isRepeatTrailUnique("r_inner"))
      .|.remoteBatchProperties("cache[b_inner.foo]")
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("start", "a_inner")
      .remoteBatchProperties("cache[start.foo]")
      .allNodeScan("start")
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("c").withSingleRow(sizeHint * 3)
  }

  test("should work with trail(repeat) multiple hops, also below and on top - 2") {
    givenGraph {
      val (nodes, _) = circleGraph(nNodes = sizeHint, relType = "R", outDegree = 1)
      for (node <- nodes) {
        node.setProperty("foo", 42)
      }
    }

    val `(start) [(a)-[r]->(b)]{1,3} (end)` = TrailParameters(
      min = 1,
      max = Limited(3),
      start = "start",
      end = "end",
      innerStart = "a_inner",
      innerEnd = "b_inner",
      groupNodes = Set(("a_inner", "a"), ("b_inner", "b")),
      groupRelationships = Set(("r_inner", "r")),
      innerRelationships = Set("r_inner"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("count(*) AS c"))
      .remoteBatchProperties("cache[end.foo]")
      .repeatTrail(`(start) [(a)-[r]->(b)]{1,3} (end)`)
      .|.filter(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.remoteBatchProperties("cache[a_inner.foo]")
      .|.argument("start", "a_inner")
      .remoteBatchProperties("cache[start.foo]")
      .allNodeScan("start")
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("c").withSingleRow(sizeHint * 3)
  }

  test("should work with trail(repeat) multiple hops, also below and on top - 3") {
    givenGraph {
      val (nodes, _) = circleGraph(nNodes = sizeHint, relType = "R", outDegree = 1)
      for (node <- nodes) {
        node.setProperty("foo", 42)
      }
    }

    val `(start) [(a)-[r]->(b)]{1,3} (end)` = TrailParameters(
      min = 1,
      max = Limited(3),
      start = "start",
      end = "end",
      innerStart = "a_inner",
      innerEnd = "b_inner",
      groupNodes = Set(("a_inner", "a"), ("b_inner", "b")),
      groupRelationships = Set(("r_inner", "r")),
      innerRelationships = Set("r_inner"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("count(*) AS c"))
      .remoteBatchProperties("cache[end.foo]")
      .repeatTrail(`(start) [(a)-[r]->(b)]{1,3} (end)`)
      .|.filter(isRepeatTrailUnique("r_inner"))
      .|.remoteBatchProperties("cache[b_inner.foo]")
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.remoteBatchProperties("cache[a_inner.foo]")
      .|.argument("start", "a_inner")
      .remoteBatchProperties("cache[start.foo]")
      .allNodeScan("start")
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("c").withSingleRow(sizeHint * 3)
  }

  test("should work with trail(repeat) including zero repetition, also on top") {
    givenGraph {
      val (nodes, _) = circleGraph(nNodes = sizeHint, relType = "R", outDegree = 1)
      for (node <- nodes) {
        node.setProperty("foo", 42)
      }
    }

    val `(start) [(a)-[r]->(b)]{0,1} (end)` = TrailParameters(
      min = 0,
      max = Limited(1),
      start = "start",
      end = "end",
      innerStart = "a_inner",
      innerEnd = "b_inner",
      groupNodes = Set(("a_inner", "a"), ("b_inner", "b")),
      groupRelationships = Set(("r_inner", "r")),
      innerRelationships = Set("r_inner"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      ExpandAll,
      accumulators = Set.empty
    )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("count(*) AS c"))
      .remoteBatchProperties("cache[end.foo]")
      .repeatTrail(`(start) [(a)-[r]->(b)]{0,1} (end)`)
      .|.filter(isRepeatTrailUnique("r_inner"))
      .|.remoteBatchProperties("cache[b_inner.foo]")
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("start", "a_inner")
      .allNodeScan("start")
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("c").withSingleRow(sizeHint * 2)
  }

  test(s"should join on a remote batched property") {
    // given
    val nodeProperties = givenGraph {
      val nodes = nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        }
      )
      nodes.map(_.getProperty("prop"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("aProp", "bProp")
      .projection("cache[a.prop] AS aProp", "cache[b.prop] AS bProp")
      .valueHashJoin("cache[a.prop]=cache[b.prop]")
      .|.filter("cache[b.prop] < 10")
      .|.remoteBatchProperties("cache[b.prop]")
      .|.allNodeScan("b")
      .filter("cache[a.prop] < 20")
      .remoteBatchProperties("cache[a.prop]")
      .allNodeScan("a")
      .build()
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodeProperties.map(prop => Array(prop, prop)).take(10)
    runtimeResult should beColumns("aProp", "bProp").withRows(expected)
  }

  test(s"should work with nested trails on rhs") {
    // (n1:A) <- (n2) -> (n3)
    val (n1, n2, n3, r21, r23) = givenGraph {
      val n1 = tx.createNode(label("A"))
      val n2 = tx.createNode()
      val n3 = tx.createNode()
      val r21 = n2.createRelationshipTo(n1, withName("R"))
      val r23 = n2.createRelationshipTo(n3, withName("R"))
      (n1, n2, n3, r21, r23)
    }

    val `(b_inner)((bb)-[rr]->(aa:A)){0,}(a)`: TrailParameters = TrailParameters(
      min = 0,
      max = UpperBound.Unlimited,
      start = "b_inner",
      end = "a",
      innerStart = "bb_inner",
      innerEnd = "aa_inner",
      groupNodes = Set(("bb_inner", "bb"), ("aa_inner", "aa")),
      groupRelationships = Set(("rr_inner", "rr")),
      innerRelationships = Set("rr_inner"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    val `(me)( (b)-[r]->(c) WHERE EXISTS { (b)( (bb)-[rr]->(aa:A) ){0,}(a) } ){0,}(you)`: TrailParameters =
      TrailParameters(
        min = 0,
        max = UpperBound.Unlimited,
        start = "me",
        end = "you",
        innerStart = "b_inner",
        innerEnd = "c_inner",
        groupNodes = Set(("b_inner", "b"), ("c_inner", "c")),
        groupRelationships = Set(("r_inner", "r")),
        innerRelationships = Set("r_inner"),
        previouslyBoundRelationships = Set.empty,
        previouslyBoundRelationshipGroups = Set.empty,
        reverseGroupVariableProjections = false,
        expansionMode = ExpandAll,
        accumulators = Set.empty
      )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("meId", "youId", "bIds", "cIds", "rIds")
      // NOTE: only return entity IDs to avoid single row property retrieval in SPD
      .projection(
        "id(me) AS meId",
        "id(you) AS youId",
        "[x IN b | id(x)] AS bIds",
        "[x IN c | id(x)] AS cIds",
        "[x IN r | id(x)] AS rIds"
      )
      .remoteBatchProperties("cache[you.prop]")
      .repeatTrail(`(me)( (b)-[r]->(c) WHERE EXISTS { (b)( (bb)-[rr]->(aa:A) ){0,}(a) } ){0,}(you)`)
      .|.apply()
      .|.|.remoteBatchProperties("cache[a.prop]")
      .|.|.limit(1)
      .|.|.filter("a:A")
      .|.|.repeatTrail(`(b_inner)((bb)-[rr]->(aa:A)){0,}(a)`)
      .|.|.|.filter("aa_inner:A")
      .|.|.|.filter(isRepeatTrailUnique("rr_inner"))
      .|.|.|.remoteBatchProperties("cache[bb_inner.prop]")
      .|.|.|.expandAll("(bb_inner)-[rr_inner]->(aa_inner)")
      .|.|.|.argument("bb_inner", "b_inner")
      .|.|.argument("b_inner")
      .|.filter(isRepeatTrailUnique("r_inner"))
      .|.expandAll("(b_inner)-[r_inner]->(c_inner)")
      .|.argument("b_inner")
      .remoteBatchProperties("cache[me.prop]")
      .allNodeScan("me")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    def listOf(values: java.lang.Long*) = RepeatTrailTestBase.listOf(values: _*)

    // then
    runtimeResult should beColumns("meId", "youId", "bIds", "cIds", "rIds").withRows(
      inAnyOrder(
        Seq(
          Array(n1.getId, n1.getId, emptyList(), emptyList(), emptyList()),
          Array(n2.getId, n2.getId, emptyList(), emptyList(), emptyList()),
          Array(n3.getId, n3.getId, emptyList(), emptyList(), emptyList()),
          Array(n2.getId, n1.getId, listOf(n2.getId), listOf(n1.getId), listOf(r21.getId)),
          Array(n2.getId, n3.getId, listOf(n2.getId), listOf(n3.getId), listOf(r23.getId))
        )
      )
    )
  }

  test("should work with optional on rhs") {
    // given
    val nodes = givenGraph {
      val startNodes = nodePropertyGraph(
        100,
        {
          case i => Map("p" -> i)
        },
        "START"
      )

      startNodes.foreach(start => {
        val c = tx.createNode(Label.label("C"))
        c.createRelationshipTo(start, RelationshipType.withName("R"))

        val d = tx.createNode(Label.label("D"))
        d.createRelationshipTo(start, RelationshipType.withName("R"))
      })

      nodePropertyGraph(
        1,
        {
          case i => Map("p" -> i)
        },
        "NOT_CONNECTED"
      )

      startNodes
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("s", "p")
      .projection("start as s", "cache[nc.p] as p")
      .remoteBatchProperties("cache[nc.p]")
      .apply()
      .|.apply()
      .|.|.optional("start", "c")
      .|.|.filter("nc:NOT_CONNECTED")
      .|.|.expandAll("(start)<-[:NOT]-(nc)")
      .|.|.argument("start", "c")
      .|.filter("d:D")
      .|.expandAll("(start)<-[:R]-(d)")
      .|.argument("start", "c")
      .optionalExpandAll("(start)<-[:R]-(c)", Some("c:C"))
      .nodeByLabelScan("start", "START")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val expected = nodes.map(n => Array(n, null))
    runtimeResult should beColumns("s", "p").withRows(expected)
  }
}

trait UpdatingRemoteBatchPropertiesTestBase[CONTEXT <: RuntimeContext] extends RuntimeTestSuite[CONTEXT] {
  self: RemoteBatchPropertiesTestBase[CONTEXT] =>

  test("should be able to merge after RemoteBatchProperties (READ)") {
    givenGraph {
      tx.createNode(Label.label("L")).setProperty("prop", 10)
      tx.createNode(Label.label("L")).setProperty("prop", 20)
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("cache[x.prop] as prop")
      .merge(nodes = Seq(createNodeWithProperties("x", Seq.empty, "{prop: 20}")))
      .filter("cache[x.prop] = 20")
      .remoteBatchProperties("cache[x.prop]")
      .nodeByLabelScan("x", "L")
      .build(readOnly = false)

    val result = execute(query, runtime)
    result should beColumns("prop").withSingleRow(20).withNoUpdates()
  }

  test("should be able to merge after RemoteBatchProperties (WRITE)") {
    givenGraph {
      tx.createNode(Label.label("L")).setProperty("prop", 10)
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("cache[x.prop] as prop")
      .merge(nodes = Seq(createNodeWithProperties("x", Seq.empty, "{prop: 20}")))
      .filter("cache[x.prop] = 20")
      .remoteBatchProperties("cache[x.prop]")
      .nodeByLabelScan("x", "L")
      .build(readOnly = false)

    val result = execute(query, runtime)
    result should beColumns("prop").withSingleRow(20).withStatistics(nodesCreated = 1, propertiesSet = 1)
  }

  test("merge handle deeply nested merge and RemoteBatchProperties") {
    // given no nodes
    tx
    // when
    // query with 21 merges
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .merge(nodes = Seq(createNodeWithProperties("n", Seq.empty, "{prop: 1}")))
      .merge(nodes = Seq(createNodeWithProperties("n", Seq.empty, "{prop: 1}")))
      .merge(nodes = Seq(createNodeWithProperties("n", Seq.empty, "{prop: 1}")))
      .merge(nodes = Seq(createNodeWithProperties("n", Seq.empty, "{prop: 1}")))
      .merge(nodes = Seq(createNodeWithProperties("n", Seq.empty, "{prop: 1}")))
      .merge(nodes = Seq(createNodeWithProperties("n", Seq.empty, "{prop: 1}")))
      .merge(nodes = Seq(createNodeWithProperties("n", Seq.empty, "{prop: 1}")))
      .merge(nodes = Seq(createNodeWithProperties("n", Seq.empty, "{prop: 1}")))
      .merge(nodes = Seq(createNodeWithProperties("n", Seq.empty, "{prop: 1}")))
      .merge(nodes = Seq(createNodeWithProperties("n", Seq.empty, "{prop: 1}")))
      .merge(nodes = Seq(createNodeWithProperties("n", Seq.empty, "{prop: 1}")))
      .merge(nodes = Seq(createNodeWithProperties("n", Seq.empty, "{prop: 1}")))
      .merge(nodes = Seq(createNodeWithProperties("n", Seq.empty, "{prop: 1}")))
      .merge(nodes = Seq(createNodeWithProperties("n", Seq.empty, "{prop: 1}")))
      .merge(nodes = Seq(createNodeWithProperties("n", Seq.empty, "{prop: 1}")))
      .merge(nodes = Seq(createNodeWithProperties("n", Seq.empty, "{prop: 1}")))
      .merge(nodes = Seq(createNodeWithProperties("n", Seq.empty, "{prop: 1}")))
      .merge(nodes = Seq(createNodeWithProperties("n", Seq.empty, "{prop: 1}")))
      .merge(nodes = Seq(createNodeWithProperties("n", Seq.empty, "{prop: 1}")))
      .merge(nodes = Seq(createNodeWithProperties("n", Seq.empty, "{prop: 1}")))
      .merge(nodes = Seq(createNodeWithProperties("n", Seq.empty, "{prop: 1}")))
      .remoteBatchProperties("cache[n.prop]")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val result = execute(logicalQuery, runtime)
    result should beColumns("n").withRows(rowCount(1)).withStatistics(nodesCreated = 1, propertiesSet = 1)
  }
}

trait UpdatingTransactionRemoteBatchPropertiesTestBase[CONTEXT <: RuntimeContext]
    extends RuntimeTestSuite[CONTEXT] with PropertyIndexTestSupport[CONTEXT] {
  self: RemoteBatchPropertiesTestBase[CONTEXT] =>

  override protected def createRuntimeTestSupport(
    graphDb: GraphDatabaseService,
    edition: Edition[CONTEXT],
    runtime: CypherRuntime[CONTEXT],
    workloadMode: WorkloadMode,
    logProvider: InternalLogProvider
  ): RuntimeTestSupport[CONTEXT] = {
    new RuntimeTestSupport[CONTEXT](
      graphDb,
      edition,
      runtime,
      workloadMode,
      logProvider,
      debugOptions,
      defaultTransactionType = Type.IMPLICIT
    )
  }

  test("should create data from returning subqueries remote projection on RHS") {
    givenGraph {
      tx.createNode(Label.label("L")).setProperty("prop", 10)
      tx.createNode(Label.label("L")).setProperty("prop", 20)
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("n")
      .transactionApply()
      .|.setProperty("n", "prop", "cache[l.prop]")
      .|.create(createNode("n", "N"))
      .|.filter("cache[l.prop] = x")
      .|.remoteBatchProperties("cache[l.prop]")
      .|.nodeByLabelScan("l", "L")
      .unwind("[10, 20] AS x")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(query, runtime)
    consume(runtimeResult)
    val nodes = Iterables.asList(tx.getAllNodes)
    nodes.size shouldBe 4
  }

  private val nodeCount = 50

  for (batchSize <- List(nodeCount - 1, nodeCount, nodeCount + 1)) {

    test(s"should create data in subqueries using batch size $batchSize with remote seek and projection on RHS") {
      givenGraph {
        for (i <- 1 to nodeCount) {
          tx.createNode(Label.label("L")).setProperty("prop", i)
        }
        nodeIndex(IndexType.RANGE, "L", "prop")
      }

      val query = new LogicalQueryBuilder(this)
        .produceResults("n")
        .transactionApply(batchSize)
        .|.setProperty("n", "prop", "cache[l.prop]")
        .|.create(createNode("n", "N"))
        .|.remoteBatchProperties("cache[l.prop]")
        .|.nodeIndexOperator(
          "l:L(prop = ???)",
          getValue = _ => DoNotGetValue,
          paramExpr = Some(varFor("x")),
          argumentIds = Set("x"),
          indexType = IndexType.RANGE
        )
        .unwind(s"range(1,$nodeCount) AS x")
        .argument()
        .build(readOnly = false)

      // then
      val runtimeResult: RecordingRuntimeResult = execute(query, runtime)
      consume(runtimeResult)
      val nodes = Iterables.asList(tx.getAllNodes)
      nodes.size shouldBe nodeCount * 2
    }

    test(s"should create data in subqueries using batch size $batchSize with label scan and projection on RHS") {
      givenGraph {
        for (i <- 1 to nodeCount) {
          tx.createNode(Label.label("L")).setProperty("prop", i)
        }
        nodeIndex(IndexType.RANGE, "L", "prop")
      }

      val query = new LogicalQueryBuilder(this)
        .produceResults("n")
        .transactionApply(batchSize)
        .|.setProperty("n", "prop", "cache[l.prop]")
        .|.create(createNode("n", "N"))
        .|.filter("cache[l.prop] = x")
        .|.remoteBatchProperties("cache[l.prop]")
        .|.nodeByLabelScan("l", "L")
        .unwind(s"range(1,$nodeCount) AS x")
        .argument()
        .build(readOnly = false)

      // then
      val runtimeResult: RecordingRuntimeResult = execute(query, runtime)
      consume(runtimeResult)
      val nodes = Iterables.asList(tx.getAllNodes)
      nodes.size shouldBe nodeCount * 2
    }

    test(
      s"should create data in subqueries using batch size $batchSize with remote seek and multiple projections on RHS"
    ) {
      givenGraph {
        for (i <- 1 to nodeCount) {
          val node = tx.createNode(Label.label("L"))
          node.setProperty("prop1", i)
          node.setProperty("prop2", i)
        }
        nodeIndex(IndexType.RANGE, "L", "prop1")
      }

      val query = new LogicalQueryBuilder(this)
        .produceResults("n")
        .transactionApply(batchSize)
        .|.setProperty("n", "prop2", "cache[l.prop2]")
        .|.setProperty("n", "prop1", "cache[l.prop1]")
        .|.create(createNode("n", "N"))
        .|.remoteBatchProperties("cache[l.prop2]")
        .|.nodeIndexOperator(
          "l:L(prop1 = ???)",
          getValue = _ => GetValue,
          paramExpr = Some(varFor("x")),
          argumentIds = Set("x"),
          indexType = IndexType.RANGE
        )
        .unwind(s"range(1,$nodeCount) AS x")
        .argument()
        .build(readOnly = false)

      // then
      val runtimeResult: RecordingRuntimeResult = execute(query, runtime)
      consume(runtimeResult)
      val nodes = Iterables.asList(tx.getAllNodes)
      nodes.size shouldBe nodeCount * 2
    }

    test(
      s"should create data in subqueries using batch size $batchSize with label scan and multiple remote projections on RHS"
    ) {
      givenGraph {
        for (i <- 1 to nodeCount) {
          val node = tx.createNode(Label.label("L"))
          node.setProperty("prop1", i)
          node.setProperty("prop2", i)
        }
      }

      val query = new LogicalQueryBuilder(this)
        .produceResults("n")
        .transactionApply(batchSize)
        .|.setProperty("n", "prop2", "cache[l.prop2]")
        .|.setProperty("n", "prop1", "cache[l.prop1]")
        .|.create(createNode("n", "N"))
        .|.remoteBatchProperties("cache[l.prop2]")
        .|.filter("cache[l.prop1] = x")
        .|.remoteBatchProperties("cache[l.prop1]")
        .|.nodeByLabelScan("l", "L")
        .unwind(s"range(1,$nodeCount) AS x")
        .argument()
        .build(readOnly = false)

      // then
      val runtimeResult: RecordingRuntimeResult = execute(query, runtime)
      consume(runtimeResult)
      val nodes = Iterables.asList(tx.getAllNodes)
      nodes.size shouldBe nodeCount * 2
    }
  }
}
