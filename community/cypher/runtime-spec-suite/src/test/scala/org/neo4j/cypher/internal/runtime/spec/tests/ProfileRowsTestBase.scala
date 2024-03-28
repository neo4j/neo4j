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

import org.neo4j.collection.RawIterator
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.TrailParameters
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RecordingRuntimeResult
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.rewriters.TestPlanCombinationRewriter.NoRewrites
import org.neo4j.cypher.internal.util.UpperBound.Limited
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.schema.IndexType
import org.neo4j.internal.kernel.api.exceptions.ProcedureException
import org.neo4j.internal.kernel.api.procs.Neo4jTypes
import org.neo4j.internal.kernel.api.procs.ProcedureSignature
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.api.ResourceMonitor
import org.neo4j.kernel.api.procedure.CallableProcedure.BasicProcedure
import org.neo4j.kernel.api.procedure.Context
import org.neo4j.procedure.Mode
import org.neo4j.values.AnyValue

abstract class ProfileRowsTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  val sizeHint: Int,
  cartesianProductChunkSize: Int // The size of a LHS chunk for cartesian product
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test(
    "should profile rows of filter under limit correctly when there are downstream cardinality increasing operators"
  ) {
    givenGraph {
      val a = runtimeTestSupport.tx.createNode(Label.label("A"))
      val b1 = runtimeTestSupport.tx.createNode(Label.label("B"))
      val b2 = runtimeTestSupport.tx.createNode(Label.label("B"))

      a.createRelationshipTo(b1, RelationshipType.withName("R"))
      a.createRelationshipTo(b2, RelationshipType.withName("R"))
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("b")
      .limit(1)
      .expandAll("(a)--(b)")
      .filter("a: A")
      .allNodeScan("a")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe 1 // produce results
    queryProfile.operatorProfile(1).rows() shouldBe 1 // limit
    queryProfile.operatorProfile(2).rows() should (be >= 1L and be <= 2L) // expand all
    queryProfile.operatorProfile(3).rows() shouldBe 1 // filter
    queryProfile.operatorProfile(4).rows() should (be >= 1L and be <= 3L) // all node scan
  }

  test("should profile rows of all nodes scan + aggregation + produce results") {
    givenGraph { nodeGraph(sizeHint) }

    val aggregationGroups = sizeHint / 2

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("k", "c")
      .aggregation(
        groupingExpressions = Seq(s"id(x) % $aggregationGroups AS k"),
        aggregationExpression = Seq("count(*) AS c")
      )
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe aggregationGroups // produce results
    queryProfile.operatorProfile(1).rows() shouldBe aggregationGroups // aggregation
    queryProfile.operatorProfile(2).rows() shouldBe sizeHint // all nodes scan
  }

  test("should profile rows of all nodes scan + produce results") {
    givenGraph { nodeGraph(sizeHint) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe sizeHint // produce results
    queryProfile.operatorProfile(1).rows() shouldBe sizeHint // all nodes scan
  }

  test("should profile rows of input + produce results") {
    // given
    val nodes = givenGraph { nodeGraph(sizeHint) }
    val input = inputColumns(sizeHint / 4, 4, i => nodes(i % nodes.size))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .input(Seq("x"))
      .build()

    val runtimeResult = profile(logicalQuery, runtime, input)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe sizeHint // produce results
    queryProfile.operatorProfile(1).rows() shouldBe sizeHint // all nodes scan
  }

  test("should profile rows with filter (fused pipelines)") {
    // given
    val nodesPerLabel = 20
    givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .expand("(x)-[r]->(y)")
      .filter("true")
      .nodeByLabelScan("x", "A")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe (nodesPerLabel * nodesPerLabel) // produce results
    queryProfile.operatorProfile(1).rows() shouldBe (nodesPerLabel * nodesPerLabel) // nonFuseable
    queryProfile.operatorProfile(2).rows() shouldBe (nodesPerLabel * nodesPerLabel) // expand
    queryProfile.operatorProfile(3).rows() shouldBe nodesPerLabel // filter
    queryProfile.operatorProfile(4).rows() shouldBe nodesPerLabel // nodeByLabelScan
  }

  test("should profile rows with projection (fused pipelines)") {
    // given
    val nodesPerLabel = 20
    givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .expand("(x)-[r]->(y)")
      .projection("x AS x2")
      .nodeByLabelScan("x", "A")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe (nodesPerLabel * nodesPerLabel) // produce results
    queryProfile.operatorProfile(1).rows() shouldBe (nodesPerLabel * nodesPerLabel) // nonFuseable
    queryProfile.operatorProfile(2).rows() shouldBe (nodesPerLabel * nodesPerLabel) // expand
    queryProfile.operatorProfile(3).rows() shouldBe nodesPerLabel // projection
    queryProfile.operatorProfile(4).rows() shouldBe nodesPerLabel // nodeByLabelScan
  }

  test("should profile rows with project endpoints (fused pipelines)") {
    // given
    val nNodes = Math.sqrt(sizeHint).ceil.toInt
    val (aNodes, bNodes, aRels, _) = givenGraph { bidirectionalBipartiteGraph(nNodes, "A", "B", "R", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .nonFuseable()
      .projectEndpoints("(x)-[r]-(y)", startInScope = false, endInScope = false)
      .input(relationships = Seq("r"), nullable = false)
      .build()

    val input = aRels.map(r => Array[Any](r))

    // then
    val runtimeResult = profile(logicalQuery, runtime, inputValues(input.toSeq: _*))
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe 2 * aRels.size // produce results
    queryProfile.operatorProfile(1).rows() shouldBe 2 * aRels.size // non-fuseable
    queryProfile.operatorProfile(2).rows() shouldBe 2 * aRels.size // project endpoints
    queryProfile.operatorProfile(3).rows() shouldBe aRels.size // input
  }

  test("should profile rows with skip (fused pipelines)") {
    // given
    val nodesPerLabel = 20
    givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .expand("(x)-[r]->(y)")
      .skip(0)
      .nodeByLabelScan("x", "A")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe (nodesPerLabel * nodesPerLabel) // produce results
    queryProfile.operatorProfile(1).rows() shouldBe (nodesPerLabel * nodesPerLabel) // nonFuseable
    queryProfile.operatorProfile(2).rows() shouldBe (nodesPerLabel * nodesPerLabel) // expand
    queryProfile.operatorProfile(3).rows() shouldBe nodesPerLabel // skip
    queryProfile.operatorProfile(4).rows() shouldBe nodesPerLabel // nodeByLabelScan
  }

  test("should profile rows with limit (fused pipelines)") {
    // given
    val nodesPerLabel = 20
    givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .expand("(x)-[r]->(y)")
      .limit(Long.MaxValue)
      .nodeByLabelScan("x", "A")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe (nodesPerLabel * nodesPerLabel) // produce results
    queryProfile.operatorProfile(1).rows() shouldBe (nodesPerLabel * nodesPerLabel) // nonFuseable
    queryProfile.operatorProfile(2).rows() shouldBe (nodesPerLabel * nodesPerLabel) // expand
    queryProfile.operatorProfile(3).rows() shouldBe nodesPerLabel // limit
    queryProfile.operatorProfile(4).rows() shouldBe nodesPerLabel // nodeByLabelScan
  }

  test("should profile rows with argument & limit on RHS of Apply (fused pipelines)") {
    // given
    val nodesPerLabel = 20
    givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .expand("(x)-[r]->(y)")
      .apply()
      .|.limit(Long.MaxValue)
      .|.argument("x")
      .nodeByLabelScan("x", "A")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe (nodesPerLabel * nodesPerLabel) // produce results
    queryProfile.operatorProfile(1).rows() shouldBe (nodesPerLabel * nodesPerLabel) // nonFuseable
    queryProfile.operatorProfile(2).rows() shouldBe (nodesPerLabel * nodesPerLabel) // expand
    queryProfile.operatorProfile(3).rows() shouldBe nodesPerLabel // apply
    queryProfile.operatorProfile(4).rows() shouldBe nodesPerLabel // limit
    queryProfile.operatorProfile(5).rows() shouldBe nodesPerLabel // argument
    queryProfile.operatorProfile(6).rows() shouldBe nodesPerLabel // nodeByLabelScan
  }

  test("should profile rows with cacheProperties (fused pipelines)") {
    // given
    val nodesPerLabel = 20
    givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .expand("(x)-[r]->(y)")
      .cacheProperties("x.prop")
      .nodeByLabelScan("x", "A")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe (nodesPerLabel * nodesPerLabel) // produce results
    queryProfile.operatorProfile(1).rows() shouldBe (nodesPerLabel * nodesPerLabel) // nonFuseable
    queryProfile.operatorProfile(2).rows() shouldBe (nodesPerLabel * nodesPerLabel) // expand
    queryProfile.operatorProfile(3).rows() shouldBe nodesPerLabel // cacheProperties
    queryProfile.operatorProfile(4).rows() shouldBe nodesPerLabel // nodeByLabelScan
  }

  test("should profile rows with distinct (fused pipelines)") {
    // given
    val nodesPerLabel = 20
    givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .expand("(x)-[r]->(y)")
      .distinct("10 AS ten", "x AS x")
      .nodeByLabelScan("x", "A")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe (nodesPerLabel * nodesPerLabel) // produce results
    queryProfile.operatorProfile(1).rows() shouldBe (nodesPerLabel * nodesPerLabel) // nonFuseable
    queryProfile.operatorProfile(2).rows() shouldBe (nodesPerLabel * nodesPerLabel) // expand
    queryProfile.operatorProfile(3).rows() shouldBe nodesPerLabel // distinct
    queryProfile.operatorProfile(4).rows() shouldBe nodesPerLabel // nodeByLabelScan
  }

  test("should profile rows with primitive distinct (fused pipelines)") {
    // given
    val nodesPerLabel = 20
    givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .expand("(x)-[r]->(y)")
      .distinct("x AS x", "x AS x2")
      .nodeByLabelScan("x", "A")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe (nodesPerLabel * nodesPerLabel) // produce results
    queryProfile.operatorProfile(1).rows() shouldBe (nodesPerLabel * nodesPerLabel) // nonFuseable
    queryProfile.operatorProfile(2).rows() shouldBe (nodesPerLabel * nodesPerLabel) // expand
    queryProfile.operatorProfile(3).rows() shouldBe nodesPerLabel // distinct
    queryProfile.operatorProfile(4).rows() shouldBe nodesPerLabel // nodeByLabelScan
  }

  test("should profile rows with single primitive distinct (fused pipelines)") {
    // given
    val nodesPerLabel = 20
    givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .expand("(x)-[r]->(y)")
      .distinct("x AS x")
      .nodeByLabelScan("x", "A")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe (nodesPerLabel * nodesPerLabel) // produce results
    queryProfile.operatorProfile(1).rows() shouldBe (nodesPerLabel * nodesPerLabel) // nonFuseable
    queryProfile.operatorProfile(2).rows() shouldBe (nodesPerLabel * nodesPerLabel) // expand
    queryProfile.operatorProfile(3).rows() shouldBe nodesPerLabel // distinct
    queryProfile.operatorProfile(4).rows() shouldBe nodesPerLabel // nodeByLabelScan
  }

  test("should profile rows with distinct on the RHS of apply (fused pipelines)") {
    // given
    val nodesPerLabel = 20
    givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a")
      .nonFuseable()
      .apply()
      .|.expand("(a)-[r]->(b)")
      .|.distinct("10 AS ten", "a AS a")
      .|.nodeByLabelScan("a", "A", "x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = profile(logicalQuery, runtime, inputValues((1 to 10).map(Array[Any](_)): _*))
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe (10 * nodesPerLabel * nodesPerLabel) // produce results
    queryProfile.operatorProfile(1).rows() shouldBe (10 * nodesPerLabel * nodesPerLabel) // nonFuseable
    queryProfile.operatorProfile(2).rows() shouldBe (10 * nodesPerLabel * nodesPerLabel) // apply
    queryProfile.operatorProfile(3).rows() shouldBe (10 * nodesPerLabel * nodesPerLabel) // expand
    queryProfile.operatorProfile(4).rows() shouldBe 10 * nodesPerLabel // distinct
    queryProfile.operatorProfile(5).rows() shouldBe 10 * nodesPerLabel // nodeByLabelScan
    queryProfile.operatorProfile(6).rows() shouldBe 10 // input
  }

  test("should profile rows with expand (fused pipelines)") {
    // given
    val nodesPerLabel = 10
    givenGraph {
      bipartiteGraph(nodesPerLabel, "A", "B", "R")
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .expand("(x)-[r2]->(y2)")
      .expand("(x)-[r]->(y)")
      .nodeByLabelScan("x", "A")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe (nodesPerLabel * nodesPerLabel * nodesPerLabel) // produce results
    queryProfile.operatorProfile(1).rows() shouldBe (nodesPerLabel * nodesPerLabel * nodesPerLabel) // nonFuseable
    queryProfile.operatorProfile(2).rows() shouldBe (nodesPerLabel * nodesPerLabel * nodesPerLabel) // expand
    queryProfile.operatorProfile(3).rows() shouldBe (nodesPerLabel * nodesPerLabel) // expand
    queryProfile.operatorProfile(4).rows() shouldBe nodesPerLabel // nodeByLabelScan
  }

  test("should profile rows with optional expand (fused pipelines)") {
    // given
    val nodesPerLabel = 10
    givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .expand("(x)-[r2]->(y2)")
      .apply()
      .|.optionalExpandAll("(x)-[r:R]->(y)")
      .|.argument("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe (nodesPerLabel * nodesPerLabel * nodesPerLabel) // produce results
    queryProfile.operatorProfile(1).rows() shouldBe (nodesPerLabel * nodesPerLabel * nodesPerLabel) // nonFuseable
    queryProfile.operatorProfile(2).rows() shouldBe (nodesPerLabel * nodesPerLabel * nodesPerLabel) // expand
    queryProfile.operatorProfile(3).rows() shouldBe (nodesPerLabel * nodesPerLabel + nodesPerLabel) // apply
    queryProfile.operatorProfile(4).rows() shouldBe (nodesPerLabel * nodesPerLabel + nodesPerLabel) // optional expand
    queryProfile.operatorProfile(5).rows() shouldBe (2 * nodesPerLabel) // argument
    queryProfile.operatorProfile(6).rows() shouldBe (2 * nodesPerLabel) // all node scan
  }

  test("should profile rows with optional expand into (fused pipelines)") {
    // given
    val nodesPerLabel = 10
    givenGraph {
      bipartiteGraph(nodesPerLabel, "A", "B", "R")
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .expand("(x)-[r2]->(y2)")
      .apply()
      .|.optionalExpandInto("(x)-[r:R]->(y)")
      .|.argument("x", "y")
      .expand("(x)-[r0]-(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe (nodesPerLabel * nodesPerLabel * nodesPerLabel) // produce results
    queryProfile.operatorProfile(1).rows() shouldBe (nodesPerLabel * nodesPerLabel * nodesPerLabel) // nonFuseable
    queryProfile.operatorProfile(2).rows() shouldBe (nodesPerLabel * nodesPerLabel * nodesPerLabel) // expand
    queryProfile.operatorProfile(3).rows() shouldBe (2 * nodesPerLabel * nodesPerLabel) // apply
    queryProfile.operatorProfile(4).rows() shouldBe (2 * nodesPerLabel * nodesPerLabel) // optional expand
    queryProfile.operatorProfile(5).rows() shouldBe (2 * nodesPerLabel * nodesPerLabel) // argument
    queryProfile.operatorProfile(6).rows() shouldBe (2 * nodesPerLabel * nodesPerLabel) // expand
    queryProfile.operatorProfile(7).rows() shouldBe (2 * nodesPerLabel) // all node scan
  }

  test("should profile rows with union (fused pipelines)") {
    // given
    val nodesPerLabel = 20
    givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .expand("(x)-[r]->(y)")
      .union()
      .|.nodeByLabelScan("x", "A")
      .nodeByLabelScan("x", "A")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe (2 * nodesPerLabel * nodesPerLabel) // produce results
    queryProfile.operatorProfile(1).rows() shouldBe (2 * nodesPerLabel * nodesPerLabel) // nonFuseable
    queryProfile.operatorProfile(2).rows() shouldBe (2 * nodesPerLabel * nodesPerLabel) // expand
    queryProfile.operatorProfile(3).rows() shouldBe (2 * nodesPerLabel) // union
    queryProfile.operatorProfile(4).rows() shouldBe nodesPerLabel // nodeByLabelScan
    queryProfile.operatorProfile(5).rows() shouldBe nodesPerLabel // nodeByLabelScan
  }

  test("should profile rows with single node by id seek (fused pipelines)") {
    // given
    val nodesPerLabel = 20
    val (as, _) = givenGraph {
      bipartiteGraph(nodesPerLabel, "A", "B", "R")
    }
    val id = as.head.getId

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .expand("(x)-[r]->(y)")
      .nodeByIdSeek("x", Set.empty, id)
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe nodesPerLabel // produce results
    queryProfile.operatorProfile(1).rows() shouldBe nodesPerLabel // nonFuseable
    queryProfile.operatorProfile(2).rows() shouldBe nodesPerLabel // expand
    queryProfile.operatorProfile(3).rows() shouldBe 1 // nodeByIdSeek
  }

  test("should profile rows with multiple node by id seek (fused pipelines)") {
    // given
    val nodesPerLabel = 20
    val (as, _) = givenGraph {
      bipartiteGraph(nodesPerLabel, "A", "B", "R")
    }
    val ids = as.map(_.getId)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .expand("(x)-[r]->(y)")
      .nodeByIdSeek("x", Set.empty, ids.toSeq: _*)
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe (nodesPerLabel * nodesPerLabel) // produce results
    queryProfile.operatorProfile(1).rows() shouldBe (nodesPerLabel * nodesPerLabel) // nonFuseable
    queryProfile.operatorProfile(2).rows() shouldBe (nodesPerLabel * nodesPerLabel) // expand
    queryProfile.operatorProfile(3).rows() shouldBe nodesPerLabel // nodeByIdSeek
  }

  test("should profile rows with directed all relationships scan") {
    // given
    val nodesPerLabel = 20
    val (_, _, rs, _) = givenGraph {
      bidirectionalBipartiteGraph(nodesPerLabel, "A", "B", "R", "R2")
    }
    val id = rs.head.getId

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .filter(s"id(r) = $id")
      .allRelationshipsScan("(x)-[r]->(y)")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe 1 // produce results
    queryProfile.operatorProfile(1).rows() shouldBe 1 // nonFuseable
    queryProfile.operatorProfile(2).rows() shouldBe 1 // filter
    queryProfile.operatorProfile(3).rows() shouldBe 2 * nodesPerLabel * nodesPerLabel // all relationships scan
  }

  test("should profile rows undirected all relationships scan") {
    // given
    val nodesPerLabel = 20
    val (_, _, rs, _) = givenGraph {
      bidirectionalBipartiteGraph(nodesPerLabel, "A", "B", "R", "R2")
    }
    val id = rs.head.getId

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .filter(s"id(r) = $id")
      .allRelationshipsScan("(x)-[r]-(y)")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe 2 // produce results
    queryProfile.operatorProfile(1).rows() shouldBe 2 // nonFuseable
    queryProfile.operatorProfile(2).rows() shouldBe 2 // filter
    queryProfile.operatorProfile(3).rows() shouldBe 4 * nodesPerLabel * nodesPerLabel // all relationships scan
  }

  test("should profile rows with directed relationship type scan") {
    // given
    val nodesPerLabel = 20
    val (_, _, rs, _) = givenGraph {
      bidirectionalBipartiteGraph(nodesPerLabel, "A", "B", "R", "R2")
    }
    val id = rs.head.getId

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .filter(s"id(r) = $id")
      .relationshipTypeScan("(x)-[r:R]->(y)")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe 1 // produce results
    queryProfile.operatorProfile(1).rows() shouldBe 1 // nonFuseable
    queryProfile.operatorProfile(2).rows() shouldBe 1 // filter
    queryProfile.operatorProfile(3).rows() shouldBe nodesPerLabel * nodesPerLabel // relationship type scan
  }

  test("should profile rows undirected relationship type scan") {
    // given
    val nodesPerLabel = 20
    val (_, _, rs, _) = givenGraph {
      bidirectionalBipartiteGraph(nodesPerLabel, "A", "B", "R", "R2")
    }
    val id = rs.head.getId

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .filter(s"id(r) = $id")
      .relationshipTypeScan("(x)-[r:R]-(y)")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe 2 // produce results
    queryProfile.operatorProfile(1).rows() shouldBe 2 // nonFuseable
    queryProfile.operatorProfile(2).rows() shouldBe 2 // filter
    queryProfile.operatorProfile(3).rows() shouldBe 2 * nodesPerLabel * nodesPerLabel // relationship type scan
  }

  test("should profile rows with single directed rel by id seek (fused pipelines)") {
    // given
    val nodesPerLabel = 20
    val (_, _, rs, _) = givenGraph {
      bidirectionalBipartiteGraph(nodesPerLabel, "A", "B", "R", "R2")
    }
    val id = rs.head.getId

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .expand("(x)-[r2]->(y2)")
      .directedRelationshipByIdSeek("r", "x", "y", Set.empty, id)
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe nodesPerLabel // produce results
    queryProfile.operatorProfile(1).rows() shouldBe nodesPerLabel // nonFuseable
    queryProfile.operatorProfile(2).rows() shouldBe nodesPerLabel // expand
    queryProfile.operatorProfile(3).rows() shouldBe 1 // directedRelationshipByIdSeek
  }

  test("should profile rows with multiple directed rel by id seek (fused pipelines)") {
    // given
    val nodesPerLabel = 20
    val (_, _, rs, _) = givenGraph {
      bidirectionalBipartiteGraph(nodesPerLabel, "A", "B", "R", "R2")
    }
    val ids = rs.map(_.getId)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .expand("(x)-[r2]->(y2)")
      .directedRelationshipByIdSeek("r", "x", "y", Set.empty, ids.toSeq: _*)
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe (ids.size * nodesPerLabel) // produce results
    queryProfile.operatorProfile(1).rows() shouldBe (ids.size * nodesPerLabel) // nonFuseable
    queryProfile.operatorProfile(2).rows() shouldBe (ids.size * nodesPerLabel) // expand
    queryProfile.operatorProfile(3).rows() shouldBe ids.size // directedRelationshipByIdSeek
  }

  test("should profile rows with single undirected rel by id seek (fused pipelines)") {
    // given
    val nodesPerLabel = 20
    val (_, _, rs, _) = givenGraph {
      bidirectionalBipartiteGraph(nodesPerLabel, "A", "B", "R", "R2")
    }
    val id = rs.head.getId

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .expand("(x)-[r2]->(y2)")
      .undirectedRelationshipByIdSeek("r", "x", "y", Set.empty, id)
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe (2 * nodesPerLabel) // produce results
    queryProfile.operatorProfile(1).rows() shouldBe (2 * nodesPerLabel) // nonFuseable
    queryProfile.operatorProfile(2).rows() shouldBe (2 * nodesPerLabel) // expand
    queryProfile.operatorProfile(3).rows() shouldBe 2 // undirectedRelationshipByIdSeek
  }

  test("should profile rows with multiple undirected rel by id seek (fused pipelines)") {
    // given
    val nodesPerLabel = 20
    val (_, _, rs, _) = givenGraph {
      bidirectionalBipartiteGraph(nodesPerLabel, "A", "B", "R", "R2")
    }
    val ids = rs.map(_.getId)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .expand("(x)-[r2]->(y2)")
      .undirectedRelationshipByIdSeek("r", "x", "y", Set.empty, ids.toSeq: _*)
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe (2 * ids.size * nodesPerLabel) // produce results
    queryProfile.operatorProfile(1).rows() shouldBe (2 * ids.size * nodesPerLabel) // nonFuseable
    queryProfile.operatorProfile(2).rows() shouldBe (2 * ids.size * nodesPerLabel) // expand
    queryProfile.operatorProfile(3).rows() shouldBe 2 * ids.size // undirectedRelationshipByIdSeek
  }

  test("should profile rows with node count from count store (fused pipelines)") {
    // given
    val nodesPerLabel = 20
    givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .expand("(x)-[r]->(y)")
      .apply()
      .|.nodeCountFromCountStore("count", Seq(Some("A")))
      .nodeByLabelScan("x", "A")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe (nodesPerLabel * nodesPerLabel) // produce results
    queryProfile.operatorProfile(1).rows() shouldBe (nodesPerLabel * nodesPerLabel) // nonFuseable
    queryProfile.operatorProfile(2).rows() shouldBe (nodesPerLabel * nodesPerLabel) // expand
    queryProfile.operatorProfile(3).rows() shouldBe nodesPerLabel // apply
    queryProfile.operatorProfile(4).rows() shouldBe nodesPerLabel // nodeCountFromCountStore
    queryProfile.operatorProfile(5).rows() shouldBe nodesPerLabel // nodeByLabelScan
  }

  test("should profile rows with rel count from count store (fused pipelines)") {
    // given
    val nodesPerLabel = 20
    givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .expand("(x)-[r]->(y)")
      .apply()
      .|.relationshipCountFromCountStore("count", None, Seq("R"), None)
      .nodeByLabelScan("x", "A")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe (nodesPerLabel * nodesPerLabel) // produce results
    queryProfile.operatorProfile(1).rows() shouldBe (nodesPerLabel * nodesPerLabel) // nonFuseable
    queryProfile.operatorProfile(2).rows() shouldBe (nodesPerLabel * nodesPerLabel) // expand
    queryProfile.operatorProfile(3).rows() shouldBe nodesPerLabel // apply
    queryProfile.operatorProfile(4).rows() shouldBe nodesPerLabel // relationshipCountFromCountStore
    queryProfile.operatorProfile(5).rows() shouldBe nodesPerLabel // nodeByLabelScan
  }

  test("should profile rows of sort + filter") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        }
      )
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter(s"x.prop >= ${sizeHint / 2}")
      .sort("x ASC")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe sizeHint / 2 // produce results
    queryProfile.operatorProfile(1).rows() shouldBe sizeHint / 2 // filter
    queryProfile.operatorProfile(2).rows() shouldBe sizeHint // sort
    queryProfile.operatorProfile(3).rows() shouldBe sizeHint // all node scan
  }

  test("should profile rows of limit") {
    givenGraph { nodeGraph(sizeHint) }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .limit(10)
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe 10 // produce results
    queryProfile.operatorProfile(1).rows() shouldBe 10 // limit
    queryProfile.operatorProfile(2).rows() should be >= 10L // all node scan
  }

  test("should profile rows with limit + expand") {
    givenGraph {
      circleGraph(sizeHint * 10)
    }

    val limitSize = sizeHint * 2L

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .expand("(x)-->(y)")
      .limit(limitSize)
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe limitSize // produce results
    queryProfile.operatorProfile(1).rows() shouldBe limitSize // expand
    queryProfile.operatorProfile(2).rows() shouldBe limitSize // limit
    queryProfile.operatorProfile(3).rows() should be >= limitSize // all node scan
  }

  test("should profile rows with limit + expand on RHS of Apply") {
    // NOTE: there is an issue with row counting and parallel runtime whenever
    // you get a continuation at the same time we hit the limit. This shouls be adressed
    // at some point but not deemed important enough just now.
    assume(!isParallel)
    val nodeCount = sizeHint * 10
    givenGraph {
      circleGraph(nodeCount)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.limit(1)
      .|.expand("(x)--(y)")
      .|.argument("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe nodeCount // produce results
    queryProfile.operatorProfile(1).rows() shouldBe nodeCount // apply
    queryProfile.operatorProfile(2).rows() shouldBe nodeCount // limit
    // Depending on morsel size, the limit may or may not cancel the second row of the expand (which may be fused with argument).
    queryProfile.operatorProfile(3).rows() should (be >= 1L * nodeCount and be <= 2L * nodeCount) // expand
    queryProfile.operatorProfile(4).rows() should (be >= nodeCount / 2L and be <= 1L * nodeCount) // argument
    queryProfile.operatorProfile(5).rows() shouldBe nodeCount // all node scan
  }

  test("should profile rows with limit + expand on RHS of ConditionalApply non-nullable") {
    // NOTE: there is an issue with row counting and parallel runtime whenever
    // you get a continuation at the same time we hit the limit. This shouls be adressed
    // at some point but not deemed important enough just now.
    assume(!isParallel)
    val nodeCount = sizeHint
    givenGraph {
      circleGraph(nodeCount)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .conditionalApply("x")
      .|.limit(1)
      .|.expand("(x)--(y)")
      .|.argument("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe nodeCount // produce results
    queryProfile.operatorProfile(1).rows() shouldBe nodeCount // conditionalApply
    queryProfile.operatorProfile(2).rows() shouldBe nodeCount // limit
    // Depending on morsel size, the limit may or may not cancel the second row of the expand (which may be fused with argument).
    queryProfile.operatorProfile(3).rows() should (be >= 1L * nodeCount and be <= 2L * nodeCount) // expand
    queryProfile.operatorProfile(4).rows() should (be >= nodeCount / 2L and be <= 1L * nodeCount) // argument
    queryProfile.operatorProfile(5).rows() shouldBe nodeCount // all node scan for x
  }

  test("should profile rows with limit + expand on RHS of ConditionalApply nullable") {
    val values = (1 to sizeHint).map {
      case i if i % 2 == 0 => Array[Any](i)
      case _               => Array[Any](null)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .conditionalApply("x")
      .|.unwind("range(1, 10) AS i")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = profile(logicalQuery, runtime, inputValues(values: _*))
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe sizeHint / 2 + (10 * sizeHint / 2) // produce results
    queryProfile.operatorProfile(1).rows() shouldBe sizeHint / 2 + (10 * sizeHint / 2) // conditionalApply
    queryProfile.operatorProfile(2).rows() shouldBe 10 * sizeHint / 2 // unwind
    queryProfile.operatorProfile(3).rows() shouldBe sizeHint / 2 // argument
    queryProfile.operatorProfile(4).rows() shouldBe sizeHint // input
  }

  test("should profile rows of skip") {
    givenGraph { nodeGraph(sizeHint) }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .skip(sizeHint - 10)
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe 10 // produce results
    queryProfile.operatorProfile(1).rows() shouldBe 10 // skip
    queryProfile.operatorProfile(2).rows() shouldBe sizeHint // all node scan
  }

  test("should profile rows with skip + expand") {
    givenGraph {
      circleGraph(sizeHint * 10)
    }

    // when
    val skipSize = sizeHint * 2L

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .expand("(x)-->(y)")
      .skip(skipSize)
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe 8 * sizeHint // produce results
    queryProfile.operatorProfile(1).rows() shouldBe 8 * sizeHint // expand
    queryProfile.operatorProfile(2).rows() shouldBe 8 * sizeHint // skip
    queryProfile.operatorProfile(3).rows() should be >= skipSize // all node scan
  }

  test("should profile rows with optional expand all") {
    // given
    val nodesPerLabel = 100
    val extraANodes = 20
    givenGraph {
      bipartiteGraph(nodesPerLabel, "A", "B", "R")
      nodeGraph(extraANodes, "A")
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a")
      .optionalExpandAll("(a)-->(b)")
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe (nodesPerLabel * nodesPerLabel + extraANodes) // produce results
    queryProfile.operatorProfile(1).rows() shouldBe (nodesPerLabel * nodesPerLabel + extraANodes) // optional expand all
    queryProfile.operatorProfile(2).rows() shouldBe (nodesPerLabel + extraANodes).toLong // nodeByLabelScan
  }

  test("should profile rows with expand into") {
    // given
    val nodesPerLabel = 100
    givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .expandInto("(x)-[r]->(y)")
      .expandAll("(x)-->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe (nodesPerLabel * nodesPerLabel) // produce results
    queryProfile.operatorProfile(1).rows() shouldBe (nodesPerLabel * nodesPerLabel) // expand into
    queryProfile.operatorProfile(2).rows() shouldBe (nodesPerLabel * nodesPerLabel) // expand all
    queryProfile.operatorProfile(3).rows() shouldBe (nodesPerLabel * 2L) // all node scan
  }

  test("should profile rows with var-expand and expand into") {
    // given
    val nodesPerLabel = 100
    givenGraph {
      bipartiteGraph(nodesPerLabel, "A", "B", "R")
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .expandInto("(x)-[r]->(y)")
      .expand("(x)-[*1..1]->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe (nodesPerLabel * nodesPerLabel) // produce results
    queryProfile.operatorProfile(1).rows() shouldBe (nodesPerLabel * nodesPerLabel) // expand into
    queryProfile.operatorProfile(2).rows() shouldBe (nodesPerLabel * nodesPerLabel) // expand all
    queryProfile.operatorProfile(3).rows() shouldBe (nodesPerLabel * 2L) // all node scan
  }

  test("should profile rows with pruning var-expand") {
    // given
    val nodesPerLabel = 100
    givenGraph {
      bipartiteGraph(nodesPerLabel, "A", "B", "R")
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .distinct("y AS y")
      .pruningVarExpand("(x)-[*1..1]->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe nodesPerLabel // produce results
    queryProfile.operatorProfile(1).rows() shouldBe nodesPerLabel // distinct
    queryProfile.operatorProfile(
      2
    ).rows() shouldBe (nodesPerLabel * nodesPerLabel) // pruning var expand (but nothing can be pruned)
    queryProfile.operatorProfile(3).rows() shouldBe (nodesPerLabel * 2L) // all node scan
  }

  test("should profile rows with bfs pruning var-expand") {
    // given
    val nodesPerLabel = 100
    givenGraph {
      bipartiteGraph(nodesPerLabel, "A", "B", "R")
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .distinct("y AS y")
      .bfsPruningVarExpand("(x)-[*1..1]->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe nodesPerLabel // produce results
    queryProfile.operatorProfile(1).rows() shouldBe nodesPerLabel // distinct
    queryProfile.operatorProfile(
      2
    ).rows() shouldBe (nodesPerLabel * nodesPerLabel) // bfs pruning var expand (but nothing can be pruned)
    queryProfile.operatorProfile(3).rows() shouldBe (nodesPerLabel * 2L) // all node scan
  }

  test("should profile rows with shortest path") {
    // TODO fails because of shortestPath, uses an ambient cursor via slotted pipe operator
    assume(!isParallel) // Parallel does not yet support `FindShortestPaths`
    // given
    val nodesPerLabel = 100
    givenGraph {
      for (_ <- 0 until nodesPerLabel)
        sineGraph()
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .shortestPath("(x)-[r*]-(y)", Some("path"))
      .cartesianProduct()
      .|.nodeByLabelScan("y", "END", IndexOrderNone)
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe nodesPerLabel // produce results
    queryProfile.operatorProfile(1).rows() shouldBe nodesPerLabel // shortest path
  }

  test("should profile rows with label scan and expand") {
    // given
    val nodesPerLabel = 10
    givenGraph {
      bipartiteGraph(nodesPerLabel, "A", "B", "R")
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .expandInto("(x)-[r]->(y)")
      .expandAll("(x)-->(y)")
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe (nodesPerLabel * nodesPerLabel) // produce results
    queryProfile.operatorProfile(1).rows() shouldBe (nodesPerLabel * nodesPerLabel) // expand into
    queryProfile.operatorProfile(2).rows() shouldBe (nodesPerLabel * nodesPerLabel) // expand all
    queryProfile.operatorProfile(3).rows() shouldBe (nodesPerLabel) // node by label scan
  }

  test("should profile rows with index scan and expand") {
    // given
    val nodesPerLabel = 100
    givenGraph {
      nodeIndex("A", "prop")
      val (aNodes, _) = bipartiteGraph(nodesPerLabel, "A", "B", "R")
      aNodes.foreach(_.setProperty("prop", 42))
    }
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .expandInto("(x)-[r]->(y)")
      .expandAll("(x)-->(y)")
      .nodeIndexOperator("x:A(prop)")
      .build()
    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe (nodesPerLabel * nodesPerLabel) // produce results
    queryProfile.operatorProfile(1).rows() shouldBe (nodesPerLabel * nodesPerLabel) // expand into
    queryProfile.operatorProfile(2).rows() shouldBe (nodesPerLabel * nodesPerLabel) // expand all
    queryProfile.operatorProfile(3).rows() shouldBe (nodesPerLabel) // node index scan
  }

  test("should profile rows with index seek and expand") {
    // given
    val nodesPerLabel = 100
    givenGraph {
      nodeIndex("A", "prop")
      val (aNodes, _) = bipartiteGraph(nodesPerLabel, "A", "B", "R")
      aNodes.foreach(_.setProperty("prop", 42))
    }
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .expandInto("(x)-[r]->(y)")
      .expandAll("(x)-->(y)")
      .nodeIndexOperator("x:A(prop=42)")
      .build()
    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe (nodesPerLabel * nodesPerLabel) // produce results
    queryProfile.operatorProfile(1).rows() shouldBe (nodesPerLabel * nodesPerLabel) // expand into
    queryProfile.operatorProfile(2).rows() shouldBe (nodesPerLabel * nodesPerLabel) // expand all
    queryProfile.operatorProfile(3).rows() shouldBe (nodesPerLabel) // node index seek
  }

  test("should profile rows with multiple index seek and expand") {
    // given
    val nodesPerLabel = 100
    givenGraph {
      nodeIndex("A", "prop")
      val (aNodes, _) = bipartiteGraph(nodesPerLabel, "A", "B", "R")
      aNodes.zipWithIndex.foreach {
        case (node, i) if i % 2 == 0 => node.setProperty("prop", 42)
        case (node, i)               => node.setProperty("prop", 1337)
      }
    }
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .expandInto("(x)-[r]->(y)")
      .expandAll("(x)-->(y)")
      .nodeIndexOperator("x:A(prop = 42 OR 1337)")
      .build()
    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe (nodesPerLabel * nodesPerLabel) // produce results
    queryProfile.operatorProfile(1).rows() shouldBe (nodesPerLabel * nodesPerLabel) // expand into
    queryProfile.operatorProfile(2).rows() shouldBe (nodesPerLabel * nodesPerLabel) // expand all
    queryProfile.operatorProfile(3).rows() shouldBe (nodesPerLabel) // node index seek
  }

  test("should profile rows with string search and expand") {
    // given
    val nodesPerLabel = 100
    givenGraph {
      nodeIndex(IndexType.TEXT, "A", "prop")
      val (aNodes, _) = bipartiteGraph(nodesPerLabel, "A", "B", "R")
      aNodes.foreach(_.setProperty("prop", "hello"))
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .expandInto("(x)-[r]->(y)")
      .expandAll("(x)-->(y)")
      .nodeIndexOperator("x:A(prop ENDS WITH 'lo')", indexType = IndexType.TEXT)
      .build()
    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe (nodesPerLabel * nodesPerLabel) // produce results
    queryProfile.operatorProfile(1).rows() shouldBe (nodesPerLabel * nodesPerLabel) // expand into
    queryProfile.operatorProfile(2).rows() shouldBe (nodesPerLabel * nodesPerLabel) // expand all
    queryProfile.operatorProfile(3).rows() shouldBe (nodesPerLabel) // ends with
  }

  test("should profile rows with single node-by id seek and expand") {
    // given
    val nodesPerLabel = 100
    val node = givenGraph {
      nodeIndex("A", "prop")
      val (aNodes, _) = bipartiteGraph(nodesPerLabel, "A", "B", "R")
      aNodes.foreach(_.setProperty("prop", "hello"))
      aNodes.head
    }
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .expandInto("(x)-[r]->(y)")
      .expandAll("(x)-->(y)")
      .nodeByIdSeek("x", Set.empty, node.getId)
      .build()
    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe nodesPerLabel // produce results
    queryProfile.operatorProfile(1).rows() shouldBe nodesPerLabel // expand into
    queryProfile.operatorProfile(2).rows() shouldBe nodesPerLabel // expand all
    queryProfile.operatorProfile(3).rows() shouldBe 1 // node-by-d
  }

  test("should profile rows with input and expand") {
    // given
    val nodesPerLabel = 100
    val aNodes = givenGraph {
      nodeIndex("A", "prop")
      val (aNodes, _) = bipartiteGraph(nodesPerLabel, "A", "B", "R")
      aNodes.foreach(_.setProperty("prop", 42))
      aNodes
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .expandInto("(x)-[r]->(y)")
      .expandAll("(x)-->(y)")
      .input(nodes = Seq("x"))
      .build()
    val runtimeResult = profile(logicalQuery, runtime, inputValues(aNodes.map(n => Array[Any](n)): _*).stream())
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe (nodesPerLabel * nodesPerLabel) // produce results
    queryProfile.operatorProfile(1).rows() shouldBe (nodesPerLabel * nodesPerLabel) // expand into
    queryProfile.operatorProfile(2).rows() shouldBe (nodesPerLabel * nodesPerLabel) // expand all
    queryProfile.operatorProfile(3).rows() shouldBe (nodesPerLabel) // input
  }

  test("should profile rows with projection and expand") {
    // given
    val nodesPerLabel = 100
    val aNodes = givenGraph {
      nodeIndex("A", "prop")
      val (aNodes, _) = bipartiteGraph(nodesPerLabel, "A", "B", "R")
      aNodes.foreach(_.setProperty("prop", 42))
      aNodes
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .expandInto("(x)-[r]->(y)")
      .expandAll("(x)-->(y)")
      .projection("xx AS x")
      .input(nodes = Seq("xx"))
      .build()
    val runtimeResult = profile(logicalQuery, runtime, inputValues(aNodes.map(n => Array[Any](n)): _*).stream())
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe (nodesPerLabel * nodesPerLabel) // produce results
    queryProfile.operatorProfile(1).rows() shouldBe (nodesPerLabel * nodesPerLabel) // expand into
    queryProfile.operatorProfile(2).rows() shouldBe (nodesPerLabel * nodesPerLabel) // expand all
    queryProfile.operatorProfile(3).rows() shouldBe (nodesPerLabel) // projection
    queryProfile.operatorProfile(4).rows() shouldBe (nodesPerLabel) // input
  }

  test("should profile rows with double expand") {
    // given
    val nodesPerLabel = 20
    givenGraph {
      bipartiteGraph(nodesPerLabel, "A", "B", "R")
    }
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .expandAll("(y)<--(z)")
      .expandAll("(x)-->(y)")
      .nonFuseable()
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe (nodesPerLabel * nodesPerLabel * nodesPerLabel) // produce results
    queryProfile.operatorProfile(1).rows() shouldBe (nodesPerLabel * nodesPerLabel * nodesPerLabel) // nonFuseable
    queryProfile.operatorProfile(2).rows() shouldBe (nodesPerLabel * nodesPerLabel * nodesPerLabel) // expand all
    queryProfile.operatorProfile(3).rows() shouldBe (nodesPerLabel * nodesPerLabel) // expand all
    queryProfile.operatorProfile(4).rows() shouldBe (nodesPerLabel * 2L) // nonFuseable
    queryProfile.operatorProfile(5).rows() shouldBe (nodesPerLabel * 2L) // all node scan
  }

  test("should profile rows with argument and expand") {
    // given
    val nodesPerLabel = 20
    givenGraph {
      bipartiteGraph(nodesPerLabel, "A", "B", "R")
    }
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.nonFuseable()
      .|.expandAll("(x)-->(y)")
      .|.argument("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe (nodesPerLabel * nodesPerLabel) // produce results
    queryProfile.operatorProfile(1).rows() shouldBe (nodesPerLabel * nodesPerLabel) // apply
    queryProfile.operatorProfile(2).rows() shouldBe (nodesPerLabel * nodesPerLabel) // nonFuseable
    queryProfile.operatorProfile(3).rows() shouldBe (nodesPerLabel * nodesPerLabel) // expand all
    queryProfile.operatorProfile(4).rows() shouldBe (nodesPerLabel * 2L) // argument
    queryProfile.operatorProfile(5).rows() shouldBe (nodesPerLabel * 2L) // all node scan
  }

  test("should profile rows with argument and two expands") {
    // given
    val nodesPerLabel = 5
    givenGraph {
      bipartiteGraph(nodesPerLabel, "A", "B", "R")
    }
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.optional("x") // The optional is to prevent fusing the produce results
      .|.expandAll("(x)-->(z)")
      .|.expandAll("(x)-->(y)")
      .|.argument("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(
      0
    ).rows() shouldBe (nodesPerLabel * nodesPerLabel * nodesPerLabel + nodesPerLabel) // produce results
    queryProfile.operatorProfile(
      1
    ).rows() shouldBe (nodesPerLabel * nodesPerLabel * nodesPerLabel + nodesPerLabel) // apply
    queryProfile.operatorProfile(
      2
    ).rows() shouldBe (nodesPerLabel * nodesPerLabel * nodesPerLabel + nodesPerLabel) // optional
    queryProfile.operatorProfile(3).rows() shouldBe (nodesPerLabel * nodesPerLabel * nodesPerLabel) // expand all
    queryProfile.operatorProfile(4).rows() shouldBe (nodesPerLabel * nodesPerLabel) // expand all
    queryProfile.operatorProfile(5).rows() shouldBe (nodesPerLabel * 2L) // argument
    queryProfile.operatorProfile(6).rows() shouldBe (nodesPerLabel * 2L) // all node scan
  }

  test("should profile rows with argument and var-expand") {
    // given
    val nodesPerLabel = 20
    givenGraph {
      bipartiteGraph(nodesPerLabel, "A", "B", "R")
    }
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.optional("x") // The optional is to prevent fusing the produce results
      .|.expandAll("(x)-->(z)")
      .|.expand("(x)-[*1..1]->(y)")
      .|.argument("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(
      0
    ).rows() shouldBe (nodesPerLabel * nodesPerLabel * nodesPerLabel + nodesPerLabel) // produce results
    queryProfile.operatorProfile(
      1
    ).rows() shouldBe (nodesPerLabel * nodesPerLabel * nodesPerLabel + nodesPerLabel) // apply
    queryProfile.operatorProfile(
      2
    ).rows() shouldBe (nodesPerLabel * nodesPerLabel * nodesPerLabel + nodesPerLabel) // optional
    queryProfile.operatorProfile(3).rows() shouldBe (nodesPerLabel * nodesPerLabel * nodesPerLabel) // expand all
    queryProfile.operatorProfile(4).rows() shouldBe (nodesPerLabel * nodesPerLabel) // var-expand
    queryProfile.operatorProfile(5).rows() shouldBe (nodesPerLabel * 2L) // argument
    queryProfile.operatorProfile(6).rows() shouldBe (nodesPerLabel * 2L) // all node scan
  }

  test("should profile rows with many node-by id seek and expand") {
    // given
    val nodesPerLabel = 100
    val aNodes = givenGraph {
      nodeIndex("A", "prop")
      val (aNodes, _) = bipartiteGraph(nodesPerLabel, "A", "B", "R")
      aNodes.foreach(_.setProperty("prop", "hello"))
      aNodes
    }
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .expandInto("(x)-[r]->(y)")
      .expandAll("(x)-->(y)")
      .nodeByIdSeek("x", Set.empty, aNodes.map(_.getId): _*)
      .build()
    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe (nodesPerLabel * nodesPerLabel) // produce results
    queryProfile.operatorProfile(1).rows() shouldBe (nodesPerLabel * nodesPerLabel) // expand into
    queryProfile.operatorProfile(2).rows() shouldBe (nodesPerLabel * nodesPerLabel) // expand all
    queryProfile.operatorProfile(3).rows() shouldBe (nodesPerLabel) // node-by-id
  }

  test("should not count invalid rows with many node-by id seek and expand") {
    // given
    val nodesPerLabel = 100
    val aNodes = givenGraph {
      nodeIndex("A", "prop")
      val (aNodes, _) = bipartiteGraph(nodesPerLabel, "A", "B", "R")
      aNodes.foreach(_.setProperty("prop", "hello"))
      aNodes
    }
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .expandInto("(x)-[r]->(y)")
      .expandAll("(x)-->(y)")
      .nodeByIdSeek("x", Set.empty, aNodes.indices.map(i => if (i % 2 == 0) aNodes(i).getId else -1): _*)
      .build()
    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe ((nodesPerLabel / 2) * nodesPerLabel) // produce results
    queryProfile.operatorProfile(1).rows() shouldBe ((nodesPerLabel / 2) * nodesPerLabel) // expand into
    queryProfile.operatorProfile(2).rows() shouldBe ((nodesPerLabel / 2) * nodesPerLabel) // expand all
    queryProfile.operatorProfile(3).rows() shouldBe (nodesPerLabel / 2) // node-by-id
  }

  test("should profile rows with unwind and expand") {
    // given
    val nodesPerLabel = 20
    val unwindCardinality = 7
    givenGraph {
      bipartiteGraph(nodesPerLabel, "A", "B", "R")
    }
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.optional("x") // The optional is to prevent fusing the produce results
      .|.expandAll("(x)-->(y)")
      .|.unwind(s"range(1, $unwindCardinality) AS i")
      .|.argument("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(
      0
    ).rows() shouldBe (nodesPerLabel * nodesPerLabel * unwindCardinality + nodesPerLabel) // produce results
    queryProfile.operatorProfile(
      1
    ).rows() shouldBe (nodesPerLabel * nodesPerLabel * unwindCardinality + nodesPerLabel) // apply
    queryProfile.operatorProfile(
      2
    ).rows() shouldBe (nodesPerLabel * nodesPerLabel * unwindCardinality + nodesPerLabel) // optional
    queryProfile.operatorProfile(3).rows() shouldBe (nodesPerLabel * nodesPerLabel * unwindCardinality) // expand all
    queryProfile.operatorProfile(4).rows() shouldBe (nodesPerLabel * 2L * unwindCardinality) // unwind
    queryProfile.operatorProfile(5).rows() shouldBe (nodesPerLabel * 2L) // argument
    queryProfile.operatorProfile(6).rows() shouldBe (nodesPerLabel * 2L) // all node scan
  }

  test("should profile rows with optional expand into") {
    // given
    val nodesPerLabel = 100
    givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .optionalExpandInto("(x)-[r]->(y)")
      .expandAll("(x)-->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe (nodesPerLabel * nodesPerLabel) // produce results
    queryProfile.operatorProfile(1).rows() shouldBe (nodesPerLabel * nodesPerLabel) // optional expand into
    queryProfile.operatorProfile(2).rows() shouldBe (nodesPerLabel * nodesPerLabel) // expand all
    queryProfile.operatorProfile(3).rows() shouldBe (nodesPerLabel * 2L) // all node scan
  }

  test("should profile rows with node hash join") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        }
      )
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeHashJoin("x")
      .|.filter("x.prop % 2 = 0")
      .|.allNodeScan("x")
      .filter(s"x.prop < ${sizeHint / 4}")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe sizeHint / 2 / 4 // produce results
    queryProfile.operatorProfile(1).rows() shouldBe sizeHint / 2 / 4 // node hash join
    queryProfile.operatorProfile(2).rows() shouldBe sizeHint / 2 // filter
    queryProfile.operatorProfile(3).rows() shouldBe sizeHint // all node scan
    queryProfile.operatorProfile(4).rows() shouldBe sizeHint / 4 // filter
    queryProfile.operatorProfile(5).rows() shouldBe sizeHint // all node scan
  }

  test("should profile rows with value hash join") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        }
      )
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .valueHashJoin("x.prop = y.prop")
      .|.filter("y.prop % 2 = 0")
      .|.allNodeScan("y")
      .filter(s"x.prop < ${sizeHint / 4}")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe sizeHint / 2 / 4 // produce results
    queryProfile.operatorProfile(1).rows() shouldBe sizeHint / 2 / 4 // value hash join
    queryProfile.operatorProfile(2).rows() shouldBe sizeHint / 2 // filter
    queryProfile.operatorProfile(3).rows() shouldBe sizeHint // all node scan
    queryProfile.operatorProfile(4).rows() shouldBe sizeHint / 4 // filter
    queryProfile.operatorProfile(5).rows() shouldBe sizeHint // all node scan
  }

  test("should profile rows with cartesian product") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        }
      )
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .cartesianProduct()
      .|.filter("y.prop % 2 = 0")
      .|.allNodeScan("y")
      .filter(s"x.prop < ${sizeHint / 4}")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val expectedRows = (sizeHint / 2) * (sizeHint / 4)
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe expectedRows // produce results
    queryProfile.operatorProfile(1).rows() shouldBe expectedRows // cartesian product
  }

  test("should profile rows with apply") {
    // given
    val size = sizeHint / 10
    givenGraph {
      nodePropertyGraph(
        size,
        {
          case i => Map("prop" -> i)
        }
      )
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .filter(s"x.prop < ${size / 4}")
      .apply()
      .|.filter("y.prop % 2 = 0")
      .|.allNodeScan("y", "x")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe size / 2 * size / 4 // produce results
    queryProfile.operatorProfile(1).rows() shouldBe size / 2 * size / 4 // filter
    queryProfile.operatorProfile(2).rows() shouldBe size / 2 * size // apply
    queryProfile.operatorProfile(3).rows() shouldBe size / 2 * size // filter
    queryProfile.operatorProfile(4).rows() shouldBe size * size // all node scan
    queryProfile.operatorProfile(5).rows() shouldBe size // all node scan
  }

  test("should profile rows of labelscan + produce results") {
    givenGraph {
      nodeGraph(sizeHint, "L1")
      nodeGraph(sizeHint, "L2")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeByLabelScan("x", "L1", IndexOrderNone)
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe sizeHint // produce results
    queryProfile.operatorProfile(1).rows() shouldBe sizeHint // label scan
  }

  test("should profile rows of nodeIndexSeek + produce results") {
    givenGraph {
      nodeIndex("L1", "prop")
      nodePropertyGraph(
        sizeHint,
        {
          case i if i % 10 == 0 => Map("prop" -> i)
        },
        "L1"
      )
      nodeGraph(sizeHint, "L2")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator("x:L1(prop = 20)")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe 1 // produce results
    queryProfile.operatorProfile(1).rows() shouldBe 1 // node index seek
  }

  test("should profile rows of directed relationshipIndexSeek + produce results") {
    givenGraph {
      relationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => r.setProperty("prop", i)
        case _                     =>
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .relationshipIndexOperator("(x)-[r:R(prop = 20)]->(y)")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe 1 // produce results
    queryProfile.operatorProfile(1).rows() shouldBe 1 // relationship index seek
  }

  test("should profile rows of undirected relationshipIndexSeek + produce results") {
    givenGraph {
      relationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => r.setProperty("prop", i)
        case _                     =>
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .relationshipIndexOperator("(x)-[r:R(prop = 20)]-(y)")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe 2 // produce results
    queryProfile.operatorProfile(1).rows() shouldBe 2 // relationship index seek
  }

  test("should profile rows of directed relationshipIndexScan + produce results") {
    givenGraph {
      relationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => r.setProperty("prop", i)
        case _                     =>
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .relationshipIndexOperator("(x)-[r:R(prop)]->(y)")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe sizeHint / 10 // produce results
    queryProfile.operatorProfile(1).rows() shouldBe sizeHint / 10 // relationship index scan
  }

  test("should profile rows of undirected relationshipIndexScan + produce results") {
    givenGraph {
      relationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => r.setProperty("prop", i)
        case _                     =>
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .relationshipIndexOperator("(x)-[r:R(prop)]-(y)")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe 2 * sizeHint / 10 // produce results
    queryProfile.operatorProfile(1).rows() shouldBe 2 * sizeHint / 10 // relationship index scan
  }

  test("should profile rows of cartesian product") {
    val size = Math.sqrt(sizeHint).toInt
    givenGraph { nodeGraph(size) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .cartesianProduct()
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()

    val result = profile(logicalQuery, runtime)
    consume(result)

    // then
    result.runtimeResult.queryProfile().operatorProfile(1).rows() shouldBe size * size // cartesian product
    val rhsAllNodesScan = result.runtimeResult.queryProfile().operatorProfile(2).rows().toInt // all node scan b
    if (isParallel) {
      // for parallel scans on the RHS the number of rows can vary depending how much the scan can be parallelized
      rhsAllNodesScan should (be >= size and be <= size * size)
    } else {
      val numberOfChunks = Math.ceil(size / cartesianProductChunkSize.toDouble).toLong
      rhsAllNodesScan shouldBe numberOfChunks * size
    }
    result.runtimeResult.queryProfile().operatorProfile(3).rows() shouldBe size // all node scan a
  }

  test("should profile rows of union") {
    val size = Math.sqrt(sizeHint).toInt
    givenGraph { nodeGraph(size) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a")
      .union()
      .|.allNodeScan("a")
      .allNodeScan("a")
      .build()

    val result = profile(logicalQuery, runtime)
    consume(result)

    // then
    result.runtimeResult.queryProfile().operatorProfile(1).rows() shouldBe size * 2 // union
    result.runtimeResult.queryProfile().operatorProfile(2).rows() shouldBe size // all node scan
    result.runtimeResult.queryProfile().operatorProfile(3).rows() shouldBe size // all node scan
  }

  test("should profile rows with project endpoints") {
    // given
    val nNodes = Math.sqrt(sizeHint).ceil.toInt
    val (aNodes, bNodes, aRels, _) = givenGraph { bidirectionalBipartiteGraph(nNodes, "A", "B", "R", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .projectEndpoints("(x)-[r]-(y)", startInScope = false, endInScope = false)
      .input(relationships = Seq("r"), nullable = false)
      .build()

    val input = aRels.map(r => Array[Any](r))

    // then
    val runtimeResult = profile(logicalQuery, runtime, inputValues(input.toSeq: _*))
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe 2 * aRels.size // produce results
    queryProfile.operatorProfile(1).rows() shouldBe 2 * aRels.size // project endpoints
    queryProfile.operatorProfile(2).rows() shouldBe aRels.size // input
  }

  test("should profile rows with union label scan") {
    // given
    givenGraph {
      nodeGraph(sizeHint, "A")
      nodeGraph(sizeHint, "B")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .unionNodeByLabelsScan("x", Seq("A", "B"), IndexOrderNone)
      .build()

    // then
    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe 2 * sizeHint // produce results
    queryProfile.operatorProfile(1).rows() shouldBe 2 * sizeHint // unionLabelScan
  }

  test("should profile rows with intersection label scan") {
    // given
    givenGraph {
      nodeGraph(sizeHint, "A", "B")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .intersectionNodeByLabelsScan("x", Seq("A", "B"), IndexOrderNone)
      .build()

    // then
    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe sizeHint // produce results
    queryProfile.operatorProfile(1).rows() shouldBe sizeHint // unionLabelScan
  }

  test("should profile rows with subtraction label scan") {
    // given
    givenGraph {
      nodeGraph(sizeHint, "A")
      nodeGraph(sizeHint, "B")
      nodeGraph(sizeHint, "A", "B")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .subtractionNodeByLabelsScan("x", "A", "B", IndexOrderNone)
      .build()

    // then
    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe sizeHint // produce results
    queryProfile.operatorProfile(1).rows() shouldBe sizeHint // filter
    queryProfile.operatorProfile(2).rows() shouldBe sizeHint // unionLabelScan
  }
}

trait EagerLimitProfileRowsTestBase[CONTEXT <: RuntimeContext] {
  self: ProfileRowsTestBase[CONTEXT] =>

  test("should profile rows with exhaustive limit + expand") {
    val nodeCount = sizeHint * 10
    givenGraph {
      circleGraph(nodeCount)
    }

    val limitSize = sizeHint * 2L

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .expand("(x)-->(y)")
      .exhaustiveLimit(limitSize)
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe limitSize // produce results
    queryProfile.operatorProfile(1).rows() shouldBe limitSize // expand
    queryProfile.operatorProfile(2).rows() shouldBe limitSize // exhaustive limit
    queryProfile.operatorProfile(3).rows() shouldBe nodeCount // all node scan
  }

  test("should profile rows with exhaustive limit + expand on RHS of Apply") {
    val nodeCount = sizeHint * 10
    givenGraph {
      circleGraph(nodeCount)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.exhaustiveLimit(1)
      .|.expand("(x)--(y)")
      .|.argument("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe nodeCount // produce results
    queryProfile.operatorProfile(1).rows() shouldBe nodeCount // apply
    queryProfile.operatorProfile(2).rows() shouldBe nodeCount // exhaustive limit
    queryProfile.operatorProfile(3).rows() shouldBe 2 * nodeCount // expand
    queryProfile.operatorProfile(4).rows() shouldBe nodeCount // argument
    queryProfile.operatorProfile(5).rows() shouldBe nodeCount // all node scan
  }

  test("should profile rows with exhaustive limit + expand on RHS of ConditionalApply non-nullable") {
    val nodeCount = sizeHint
    givenGraph {
      circleGraph(nodeCount)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .conditionalApply("x")
      .|.exhaustiveLimit(1)
      .|.expand("(x)--(y)")
      .|.argument("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe nodeCount // produce results
    queryProfile.operatorProfile(1).rows() shouldBe nodeCount // conditionalApply
    queryProfile.operatorProfile(2).rows() shouldBe nodeCount // exhaustive limit
    queryProfile.operatorProfile(3).rows() shouldBe 2L * nodeCount // expand
    queryProfile.operatorProfile(4).rows() shouldBe nodeCount // argument
    queryProfile.operatorProfile(5).rows() shouldBe nodeCount // all node scan for x
  }

  test("should profile rows with exhaustive limit (fused pipelines)") {
    // given
    val nodesPerLabel = 20
    givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .expand("(x)-[r]->(y)")
      .exhaustiveLimit(1)
      .nodeByLabelScan("x", "A")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe nodesPerLabel // produce results
    queryProfile.operatorProfile(1).rows() shouldBe nodesPerLabel // nonFuseable
    queryProfile.operatorProfile(2).rows() shouldBe nodesPerLabel // expand
    queryProfile.operatorProfile(3).rows() shouldBe 1 // exhaustive limit
    queryProfile.operatorProfile(4).rows() shouldBe nodesPerLabel // nodeByLabelScan
  }
}

trait NonParallelProfileRowsTestBase[CONTEXT <: RuntimeContext] {
  self: ProfileRowsTestBase[CONTEXT] =>

  test("should profile rows of procedure call") {
    // given
    registerProcedure(new BasicProcedure(ProcedureSignature.procedureSignature(Array[String](), "proc").mode(
      Mode.READ
    ).in("j", Neo4jTypes.NTInteger).out("i", Neo4jTypes.NTInteger).build()) {
      override def apply(
        ctx: Context,
        input: Array[AnyValue],
        resourceMonitor: ResourceMonitor
      ): RawIterator[Array[AnyValue], ProcedureException] = {
        RawIterator.of[Array[AnyValue], ProcedureException](input, input)
      }
    })

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("i")
      .procedureCall("proc(j) YIELD i AS i")
      .unwind(s"range(0, ${sizeHint - 1}) AS j")
      .argument()
      .build()

    val result = profile(logicalQuery, runtime)
    consume(result)

    // then
    result.runtimeResult.queryProfile().operatorProfile(1).rows() shouldBe sizeHint * 2 // procedure call
    result.runtimeResult.queryProfile().operatorProfile(2).rows() shouldBe sizeHint // unwind
  }

  test("should profile rows with ordered distinct") {
    // given
    val nodes = givenGraph {
      nodeGraph(sizeHint)
    }

    val input = for (n <- nodes) yield Array[Any](nodes.head, n)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .orderedDistinct(Seq("x"), "x AS x", "y AS y")
      .input(nodes = Seq("x", "y"))
      .build()

    val runtimeResult = profile(logicalQuery, runtime, inputValues(input.toSeq: _*))
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe sizeHint // produce results
    queryProfile.operatorProfile(1).rows() shouldBe sizeHint // orderedDistinct
    queryProfile.operatorProfile(2).rows() shouldBe sizeHint // input
  }

  test("should profile rows with ordered aggregation") {
    // given
    val nodes = givenGraph {
      nodeGraph(sizeHint)
    }

    val input = for (n <- nodes) yield Array[Any](nodes.head, n)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .orderedAggregation(Seq("x AS x", "y AS y"), Seq("collect(y) AS c"), Seq("x"))
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = profile(logicalQuery, runtime, inputValues(input.toSeq: _*))
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe sizeHint // produce results
    queryProfile.operatorProfile(1).rows() shouldBe sizeHint // orderedAggregation
    queryProfile.operatorProfile(2).rows() shouldBe sizeHint // input
  }

  test("should profile rows of partial sort") {
    val input = for (i <- 0 until sizeHint) yield Array[Any](1, i)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .partialSort(Seq("x ASC"), Seq("y ASC"))
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = profile(logicalQuery, runtime, inputValues(input: _*))
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe sizeHint // produce results
    queryProfile.operatorProfile(1).rows() shouldBe sizeHint // partial sort
    queryProfile.operatorProfile(2).rows() shouldBe sizeHint // input
  }

  test("should profile rows with partial top") {
    // given
    val nodes = givenGraph {
      nodeGraph(sizeHint)
    }

    val input = for (n <- nodes) yield Array[Any](nodes.head, n)

    val limit = 123

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .partialTop(Seq(Ascending(varFor("x"))), Seq(Ascending(varFor("y"))), limit)
      .input(nodes = Seq("x", "y"))
      .build()

    val runtimeResult = profile(logicalQuery, runtime, inputValues(input.toSeq: _*))
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe limit // produce results
    queryProfile.operatorProfile(1).rows() shouldBe limit // partial top
    queryProfile.operatorProfile(2).rows() shouldBe sizeHint // input
  }

  test("should profile rows with triadic selection") {
    // given
    givenGraph { chainGraphs(sizeHint, "A", "B") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z")
      .triadicSelection(positivePredicate = false, "x", "y", "z")
      .|.expandAll("(y)-->(z)")
      .|.argument("x", "y")
      .expandAll("(x)-->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime, testPlanCombinationRewriterHints = Set(NoRewrites))
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    val rows: Int => Long = queryProfile.operatorProfile(_).rows()

    rows(0) shouldBe sizeHint // produce results
    rows(2) shouldBe sizeHint // expand
    rows(3) shouldBe sizeHint * 2 // argument
    rows(4) shouldBe sizeHint * 2 // expand
    rows(5) shouldBe sizeHint * 3 // all node scan

    // in pipelined triadic selection is rewritten into build-apply-filter
    (rows(1), rows(6), rows(7), rows(8)) should {
      be((sizeHint, 0, 0, 0)) or // triadic selection
        be((
          0,
          sizeHint * 2, // triadic build
          sizeHint, // apply
          sizeHint
        )) // triadic filter
    }
  }
}

//Merge is supported in fused only so we need to break this one out
trait MergeProfileRowsTestBase[CONTEXT <: RuntimeContext] {
  self: ProfileRowsTestBase[CONTEXT] =>

  test("should profile rows of merge on match") {
    givenGraph { nodeGraph(sizeHint) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .merge(nodes = Seq(createNode("x")))
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe sizeHint // produce results
    queryProfile.operatorProfile(1).rows() shouldBe sizeHint // merge
    queryProfile.operatorProfile(2).rows() shouldBe sizeHint // all nodes scan
  }

  test("should profile rows of merge on create") {
    // given no nodes

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .merge(nodes = Seq(createNode("x")))
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe 1 // produce results
    queryProfile.operatorProfile(1).rows() shouldBe 1 // merge
    queryProfile.operatorProfile(2).rows() shouldBe 0 // all nodes scan
  }
}

trait TransactionForeachProfileRowsTestBase[CONTEXT <: RuntimeContext] {
  self: ProfileRowsTestBase[CONTEXT] =>

  test("should profile rows of operations in transactionForeach") {
    givenWithTransactionType(
      nodeGraph(sizeHint),
      KernelTransaction.Type.IMPLICIT
    )

    val query = new LogicalQueryBuilder(this)
      .produceResults("x")
      .transactionForeach()
      .|.emptyResult()
      .|.create(createNode("n", "N"))
      .|.allNodeScan("m")
      .unwind("[1, 2] AS x")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = profile(query, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()

    queryProfile.operatorProfile(0).rows() shouldBe 2 // produce results
    queryProfile.operatorProfile(1).rows() shouldBe 2 // transactionForeach
    queryProfile.operatorProfile(2).rows() shouldBe 0 // emptyResult
    queryProfile.operatorProfile(3).rows() shouldBe (1 + 2) * sizeHint // create
    queryProfile.operatorProfile(4).rows() shouldBe (1 + 2) * sizeHint // allNodeScan
    queryProfile.operatorProfile(5).rows() shouldBe 2 // unwind
    queryProfile.operatorProfile(6).rows() shouldBe 1 // argument
  }
}

trait TrailProfileRowsTestBase[CONTEXT <: RuntimeContext] {
  self: ProfileRowsTestBase[CONTEXT] =>

  test("should profile rows of operations in trail operator") {
    givenWithTransactionType(
      circleGraph(sizeHint),
      KernelTransaction.Type.IMPLICIT
    )

    val query = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r")
      .trail(TrailParameters(
        min = 0,
        max = Limited(2),
        start = "me",
        end = "you",
        innerStart = "a_inner",
        innerEnd = "b_inner",
        groupNodes = Set(("a_inner", "a"), ("b_inner", "b")),
        groupRelationships = Set(("r_inner", "r")),
        innerRelationships = Set("r_inner"),
        previouslyBoundRelationships = Set.empty,
        previouslyBoundRelationshipGroups = Set.empty,
        reverseGroupVariableProjections = false
      ))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .allNodeScan("me")
      .build()

    // then
    val runtimeResult: RecordingRuntimeResult = profile(query, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()

    queryProfile.operatorProfile(0).rows() shouldBe 3 * sizeHint // produce results
    queryProfile.operatorProfile(1).rows() shouldBe 3 * sizeHint // trail
    queryProfile.operatorProfile(2).rows() shouldBe 2 * sizeHint // expand
    queryProfile.operatorProfile(3).rows() shouldBe 2 * sizeHint // argument
    queryProfile.operatorProfile(4).rows() shouldBe 1 * sizeHint // nodeByLabelScan
  }
}
