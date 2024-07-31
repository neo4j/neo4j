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

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.TrailParameters
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.util.UpperBound
import org.neo4j.cypher.internal.util.UpperBound.Limited
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Label.label
import org.neo4j.graphdb.RelationshipType.withName
import org.neo4j.graphdb.config.Setting

import java.util.Collections.emptyList

abstract class RemoteBatchPropertiesTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  val sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  def restartWithSizes(morselSize: Int, spdBatchSize: Int): Unit = {
    shutdownDatabase()
    val additionalConfigs: Array[(Setting[_], Object)] = Array(
      GraphDatabaseInternalSettings.cypher_pipelined_batch_size_small -> Int.box(morselSize),
      GraphDatabaseInternalSettings.cypher_pipelined_batch_size_big -> Int.box(morselSize),
      GraphDatabaseInternalSettings.sharded_property_database_batch_size -> Int.box(spdBatchSize)
    )
    setAdditionalConfigs(additionalConfigs)
    restartDB()
    createRuntimeTestSupport()
  }

  // TODO remove these loops
  for (morselSize <- 1 to 5) {
    for (spdBatchSize <- 1 to morselSize * 3) {
      test(
        s"should return one node property column - on tiny graph - morselSize($morselSize) spdBatchSize($spdBatchSize)"
      ) {
        restartWithSizes(morselSize, spdBatchSize)

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

      test(
        s"should return two node properties columns - on tiny graph - morselSize($morselSize) spdBatchSize($spdBatchSize)"
      ) {
        restartWithSizes(morselSize, spdBatchSize)

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

      test(
        s"should return two node property columns with one null value - on tiny graph - morselSize($morselSize) spdBatchSize($spdBatchSize)"
      ) {
        restartWithSizes(morselSize, spdBatchSize)

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

      test(s"should return nothing on empty graph - morselSize($morselSize) spdBatchSize($spdBatchSize)") {
        restartWithSizes(morselSize, spdBatchSize)

        val query = new LogicalQueryBuilder(this)
          .produceResults("prop1", "prop2")
          .projection("cache[x.prop1] as prop1", "cache[x.prop2] as prop2")
          .remoteBatchProperties("cache[x.prop1]", "cache[x.prop2]")
          .allNodeScan("x")
          .build()

        val result = execute(query, runtime)
        result should beColumns("prop1", "prop2").withNoRows()
      }

      test(s"should return two node properties columns - morselSize($morselSize) spdBatchSize($spdBatchSize)") {
        restartWithSizes(morselSize, spdBatchSize)

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

      test(
        s"should return two node properties columns with nulls - morselSize($morselSize) spdBatchSize($spdBatchSize)"
      ) {
        restartWithSizes(morselSize, spdBatchSize)

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

      test(
        s"should return one node property columns when under Apply - morselSize($morselSize) spdBatchSize($spdBatchSize)"
      ) {
        restartWithSizes(morselSize, spdBatchSize)

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

      test(
        s"should return one node property columns when under Apply with Sort - morselSize($morselSize) spdBatchSize($spdBatchSize)"
      ) {
        restartWithSizes(morselSize, spdBatchSize)

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

      test(
        s"should return one node property columns when under Apply with Sort, also on top - morselSize($morselSize) spdBatchSize($spdBatchSize)"
      ) {
        restartWithSizes(morselSize, spdBatchSize)

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

      test(
        s"should return one node property columns when split into two plans under Apply with Sort - morselSize($morselSize) spdBatchSize($spdBatchSize)"
      ) {
        restartWithSizes(morselSize, spdBatchSize)

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

      test(s"should handle missing long entities - morselSize($morselSize) spdBatchSize($spdBatchSize)") {
        restartWithSizes(morselSize, spdBatchSize)

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
        val expected = nodes.map(_ => Array(null, null))
        runtimeResult should beColumns("x", "y").withRows(expected)
      }

      test(s"should handle missing ref entities - morselSize($morselSize) spdBatchSize($spdBatchSize)") {
        restartWithSizes(morselSize, spdBatchSize)

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

      test(s"should handle missing property token - morselSize($morselSize) spdBatchSize($spdBatchSize)") {
        restartWithSizes(morselSize, spdBatchSize)

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
        val expected = nodes.map(_ => Array(null))
        runtimeResult should beColumns("x").withRows(expected)
      }

      test(s"should handle missing property value - morselSize($morselSize) spdBatchSize($spdBatchSize)") {
        restartWithSizes(morselSize, spdBatchSize)

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

      test(
        s"should handle duplicated cached properties on rhs of nested cartesian product - morselSize($morselSize) spdBatchSize($spdBatchSize)"
      ) {
        restartWithSizes(morselSize, spdBatchSize)

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
          .|.|.|.nodeByLabelScan("n3", "C")
          .|.|.remoteBatchProperties("cache[n1.p2]")
          .|.|.argument("n1")
          .|.filter("n2.p = 20")
          .|.allNodeScan("n2")
          .allNodeScan("n1")
          .build()

        val result = execute(query, runtime)

        result should beColumns("p3").withSingleRow(30)
      }

      test(
        s"should handle duplicated cached properties on rhs of cartesian product with additional slots - morselSize($morselSize) spdBatchSize($spdBatchSize)"
      ) {
        restartWithSizes(morselSize, spdBatchSize)

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
          .|.|.|.nodeByLabelScan("n3", "C")
          .|.|.remoteBatchProperties("cache[n1.p2]")
          .|.|.argument("n1")
          .|.filter("n2.p = 20")
          .|.allNodeScan("n2")
          .allNodeScan("n1")
          .build()

        val result = execute(query, runtime)

        result should beColumns("p3").withSingleRow(30)
      }

      test(
        s"should handle duplicated cached properties on rhs of nested cartesian product with union on top - morselSize($morselSize) spdBatchSize($spdBatchSize)"
      ) {
        restartWithSizes(morselSize, spdBatchSize)

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
          .|.projection("n4.p4 as prop")
          .|.filter("n4.p4 = 40")
          .|.allNodeScan("n4")
          .projection("cache[n3.p3] as prop")
          .remoteBatchProperties("cache[n3.p3]")
          .apply()
          .|.cartesianProduct()
          .|.|.cartesianProduct()
          .|.|.|.filter("cache[n1.p2] = 11")
          .|.|.|.nodeByLabelScan("n3", "C")
          .|.|.remoteBatchProperties("cache[n1.p2]")
          .|.|.argument("n1")
          .|.filter("n2.p = 20")
          .|.allNodeScan("n2")
          .allNodeScan("n1")
          .build()

        val result = execute(query, runtime)

        result should beColumns("prop").withRows(inAnyOrder(Seq(Array(30), Array(40))))
      }

      test(s"should work with trail(repeat) single hop - morselSize($morselSize) spdBatchSize($spdBatchSize)") {
        restartWithSizes(morselSize, spdBatchSize)

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
          reverseGroupVariableProjections = false
        )

        val logicalQuery = new LogicalQueryBuilder(this)
          .produceResults("c")
          .aggregation(Seq.empty, Seq("count(*) AS c"))
          .trail(`(start) [(a)-[r]->(b)]{1,1} (end)`)
          .|.filterExpression(isRepeatTrailUnique("r_inner"))
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

      test(s"should work with trail(repeat) multiple hops - morselSize($morselSize) spdBatchSize($spdBatchSize)") {
        restartWithSizes(morselSize, spdBatchSize)

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
          reverseGroupVariableProjections = false
        )

        val logicalQuery = new LogicalQueryBuilder(this)
          .produceResults("c")
          .aggregation(Seq.empty, Seq("count(*) AS c"))
          .trail(`(start) [(a)-[r]->(b)]{1,3} (end)`)
          .|.filterExpression(isRepeatTrailUnique("r_inner"))
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

      test(
        s"should work with trail(repeat) including zero repetition - morselSize($morselSize) spdBatchSize($spdBatchSize)"
      ) {
        restartWithSizes(morselSize, spdBatchSize)

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
          reverseGroupVariableProjections = false
        )

        val logicalQuery = new LogicalQueryBuilder(this)
          .produceResults("c")
          .aggregation(Seq.empty, Seq("count(*) AS c"))
          .trail(`(start) [(a)-[r]->(b)]{0,1} (end)`)
          .|.filterExpression(isRepeatTrailUnique("r_inner"))
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

      test(
        s"should work with trail(repeat) single hop, also on top - morselSize($morselSize) spdBatchSize($spdBatchSize)"
      ) {
        restartWithSizes(morselSize, spdBatchSize)

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
          reverseGroupVariableProjections = false
        )

        val logicalQuery = new LogicalQueryBuilder(this)
          .produceResults("c")
          .aggregation(Seq.empty, Seq("count(*) AS c"))
          .remoteBatchProperties("cache[end.foo]")
          .trail(`(start) [(a)-[r]->(b)]{1,1} (end)`)
          .|.filterExpression(isRepeatTrailUnique("r_inner"))
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

      test(
        s"should work with trail(repeat) multiple hops, also on top - morselSize($morselSize) spdBatchSize($spdBatchSize)"
      ) {
        restartWithSizes(morselSize, spdBatchSize)

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
          reverseGroupVariableProjections = false
        )

        val logicalQuery = new LogicalQueryBuilder(this)
          .produceResults("c")
          .aggregation(Seq.empty, Seq("count(*) AS c"))
          .remoteBatchProperties("cache[end.foo]")
          .trail(`(start) [(a)-[r]->(b)]{1,3} (end)`)
          .|.filterExpression(isRepeatTrailUnique("r_inner"))
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

      test(
        s"should work with trail(repeat) multiple hops, also below and on top - morselSize($morselSize) spdBatchSize($spdBatchSize)"
      ) {
        restartWithSizes(morselSize, spdBatchSize)

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
          reverseGroupVariableProjections = false
        )

        val logicalQuery = new LogicalQueryBuilder(this)
          .produceResults("c")
          .aggregation(Seq.empty, Seq("count(*) AS c"))
          .remoteBatchProperties("cache[end.foo]")
          .trail(`(start) [(a)-[r]->(b)]{1,3} (end)`)
          .|.filterExpression(isRepeatTrailUnique("r_inner"))
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

      test(
        s"should work with trail(repeat) multiple hops, also below and on top - morselSize($morselSize) spdBatchSize($spdBatchSize) - 2"
      ) {
        restartWithSizes(morselSize, spdBatchSize)

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
          reverseGroupVariableProjections = false
        )

        val logicalQuery = new LogicalQueryBuilder(this)
          .produceResults("c")
          .aggregation(Seq.empty, Seq("count(*) AS c"))
          .remoteBatchProperties("cache[end.foo]")
          .trail(`(start) [(a)-[r]->(b)]{1,3} (end)`)
          .|.filterExpression(isRepeatTrailUnique("r_inner"))
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

      test(
        s"should work with trail(repeat) multiple hops, also below and on top - morselSize($morselSize) spdBatchSize($spdBatchSize) - 3"
      ) {
        restartWithSizes(morselSize, spdBatchSize)

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
          reverseGroupVariableProjections = false
        )

        val logicalQuery = new LogicalQueryBuilder(this)
          .produceResults("c")
          .aggregation(Seq.empty, Seq("count(*) AS c"))
          .remoteBatchProperties("cache[end.foo]")
          .trail(`(start) [(a)-[r]->(b)]{1,3} (end)`)
          .|.filterExpression(isRepeatTrailUnique("r_inner"))
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

      test(
        s"should work with trail(repeat) including zero repetition, also on top - morselSize($morselSize) spdBatchSize($spdBatchSize)"
      ) {
        restartWithSizes(morselSize, spdBatchSize)

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
          reverseGroupVariableProjections = false
        )

        val logicalQuery = new LogicalQueryBuilder(this)
          .produceResults("c")
          .aggregation(Seq.empty, Seq("count(*) AS c"))
          .remoteBatchProperties("cache[end.foo]")
          .trail(`(start) [(a)-[r]->(b)]{0,1} (end)`)
          .|.filterExpression(isRepeatTrailUnique("r_inner"))
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

      test(s"should join on a remote batched property - morselSize($morselSize) spdBatchSize($spdBatchSize)") {
        restartWithSizes(morselSize, spdBatchSize)

        // given
        val nodes = givenGraph {
          nodePropertyGraph(
            sizeHint,
            {
              case i => Map("prop" -> i)
            }
          )
        }

        // when
        val logicalQuery = new LogicalQueryBuilder(this)
          .produceResults("a", "b")
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
        val expected = nodes.map(n => Array(n, n)).take(10)
        runtimeResult should beColumns("a", "b").withRows(expected)
      }

      test(s"should work with nested trails on rhs - morselSize($morselSize) spdBatchSize($spdBatchSize)") {
        restartWithSizes(morselSize, spdBatchSize)

        // (n1:A) <- (n2) -> (n3)
        val (n1, n2, n3, r21, r23) = givenGraph {
          val n1 = tx.createNode(label("A"))
          val n2 = tx.createNode()
          val n3 = tx.createNode()
          val r21 = n2.createRelationshipTo(n1, withName("R"))
          val r23 = n2.createRelationshipTo(n3, withName("R"))
          (n1, n2, n3, r21, r23)
        }

        val `(b_inner)((bb)-[rr]->(aa:A)){0,}(a)` : TrailParameters = TrailParameters(
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
          reverseGroupVariableProjections = false
        )

        val `(me)( (b)-[r]->(c) WHERE EXISTS { (b)( (bb)-[rr]->(aa:A) ){0,}(a) } ){0,}(you)` : TrailParameters =
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
            reverseGroupVariableProjections = false
          )

        val logicalQuery = new LogicalQueryBuilder(this)
          .produceResults("me", "you", "b", "c", "r")
          .remoteBatchProperties("cache[you.prop]")
          .trail(`(me)( (b)-[r]->(c) WHERE EXISTS { (b)( (bb)-[rr]->(aa:A) ){0,}(a) } ){0,}(you)`)
          .|.apply()
          .|.|.remoteBatchProperties("cache[a.prop]")
          .|.|.limit(1)
          .|.|.filter("a:A")
          .|.|.trail(`(b_inner)((bb)-[rr]->(aa:A)){0,}(a)`)
          .|.|.|.filter("aa_inner:A")
          .|.|.|.filterExpressionOrString(isRepeatTrailUnique("rr_inner"))
          .|.|.|.remoteBatchProperties("cache[bb_inner.prop]")
          .|.|.|.expandAll("(bb_inner)-[rr_inner]->(aa_inner)")
          .|.|.|.argument("bb_inner", "b_inner")
          .|.|.argument("b_inner")
          .|.filterExpressionOrString(isRepeatTrailUnique("r_inner"))
          .|.expandAll("(b_inner)-[r_inner]->(c_inner)")
          .|.argument("b_inner")
          .remoteBatchProperties("cache[me.prop]")
          .allNodeScan("me")
          .build()

        val runtimeResult = execute(logicalQuery, runtime)

        def listOf(values: AnyRef*) = TrailTestBase.listOf(values: _*)

        // then
        runtimeResult should beColumns("me", "you", "b", "c", "r").withRows(
          inAnyOrder(
            Seq(
              Array(n1, n1, emptyList(), emptyList(), emptyList()),
              Array(n2, n2, emptyList(), emptyList(), emptyList()),
              Array(n3, n3, emptyList(), emptyList(), emptyList()),
              Array(n2, n1, listOf(n2), listOf(n1), listOf(r21)),
              Array(n2, n3, listOf(n2), listOf(n3), listOf(r23))
            )
          )
        )
      }
    }
  }
}
