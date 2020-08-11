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

import org.neo4j.collection.RawIterator
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.internal.kernel.api.exceptions.ProcedureException
import org.neo4j.internal.kernel.api.procs.Neo4jTypes
import org.neo4j.internal.kernel.api.procs.ProcedureSignature
import org.neo4j.kernel.api.ResourceTracker
import org.neo4j.kernel.api.procedure.CallableProcedure.BasicProcedure
import org.neo4j.kernel.api.procedure.Context
import org.neo4j.procedure.Mode
import org.neo4j.values.AnyValue

abstract class ProfileRowsTestBase[CONTEXT <: RuntimeContext](edition: Edition[CONTEXT],
                                                              runtime: CypherRuntime[CONTEXT],
                                                              val sizeHint: Int,
                                                              cartesianProductChunkSize: Int // The size of a LHS chunk for cartesian product
                                                             ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should profile rows of all nodes scan + aggregation + produce results") {
    given { nodeGraph(sizeHint) }

    val aggregationGroups = sizeHint / 2

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("k", "c")
      .aggregation(groupingExpressions = Seq(s"id(x) % $aggregationGroups AS k"), aggregationExpression = Seq("count(*) AS c"))
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe aggregationGroups // produce results
    queryProfile.operatorProfile(1).rows() shouldBe aggregationGroups // aggregation
    queryProfile.operatorProfile(2).rows() shouldBe sizeHint          // all nodes scan
  }

  test("should profile rows of all nodes scan + produce results") {
    given { nodeGraph(sizeHint) }

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
    val nodes = given { nodeGraph(sizeHint) }
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
    given {bipartiteGraph(nodesPerLabel, "A", "B", "R")}

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .expand("(x)-[r]->(y)")
      .filter("true")
      .nodeByLabelScan("x", "A", IndexOrderNone)
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
    given {bipartiteGraph(nodesPerLabel, "A", "B", "R")}

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .expand("(x)-[r]->(y)")
      .projection("x AS x2")
      .nodeByLabelScan("x", "A", IndexOrderNone)
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

  test("should profile rows with skip (fused pipelines)") {
    // given
    val nodesPerLabel = 20
    given {bipartiteGraph(nodesPerLabel, "A", "B", "R")}

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .expand("(x)-[r]->(y)")
      .skip(0)
      .nodeByLabelScan("x", "A", IndexOrderNone)
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
    given {bipartiteGraph(nodesPerLabel, "A", "B", "R")}

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .expand("(x)-[r]->(y)")
      .limit(Long.MaxValue)
      .nodeByLabelScan("x", "A", IndexOrderNone)
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
    given {bipartiteGraph(nodesPerLabel, "A", "B", "R")}

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .expand("(x)-[r]->(y)")
      .apply()
      .|.limit(Long.MaxValue)
      .|.argument("x")
      .nodeByLabelScan("x", "A", IndexOrderNone)
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
    given {bipartiteGraph(nodesPerLabel, "A", "B", "R")}

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .expand("(x)-[r]->(y)")
      .cacheProperties("x.prop")
      .nodeByLabelScan("x", "A", IndexOrderNone)
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
    given {bipartiteGraph(nodesPerLabel, "A", "B", "R")}

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .expand("(x)-[r]->(y)")
      .distinct("10 AS ten", "x AS x")
      .nodeByLabelScan("x", "A", IndexOrderNone)
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
    given {bipartiteGraph(nodesPerLabel, "A", "B", "R")}

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .expand("(x)-[r]->(y)")
      .distinct("x AS x", "x AS x2")
      .nodeByLabelScan("x", "A", IndexOrderNone)
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
    given {bipartiteGraph(nodesPerLabel, "A", "B", "R")}

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .expand("(x)-[r]->(y)")
      .distinct("x AS x")
      .nodeByLabelScan("x", "A", IndexOrderNone)
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

  test("should profile rows with expand (fused pipelines)") {
    // given
    val nodesPerLabel = 10
    given {
      bipartiteGraph(nodesPerLabel, "A", "B", "R")
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .expand("(x)-[r2]->(y2)")
      .expand("(x)-[r]->(y)")
      .nodeByLabelScan("x", "A", IndexOrderNone)
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
    given{bipartiteGraph(nodesPerLabel, "A", "B", "R")}

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
    given{
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
    given {bipartiteGraph(nodesPerLabel, "A", "B", "R")}

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .expand("(x)-[r]->(y)")
      .union()
      .|.nodeByLabelScan("x", "A", IndexOrderNone)
      .nodeByLabelScan("x", "A", IndexOrderNone)
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
    val (as, _) = given {
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
    val (as, _) = given {
      bipartiteGraph(nodesPerLabel, "A", "B", "R")
    }
    val ids = as.map(_.getId)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .expand("(x)-[r]->(y)")
      .nodeByIdSeek("x", Set.empty, ids :_*)
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

  test("should profile rows with single directed rel by id seek (fused pipelines)") {
    // given
    val nodesPerLabel = 20
    val (_, _, rs, _) = given {
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
    val (_, _, rs, _) = given {
      bidirectionalBipartiteGraph(nodesPerLabel, "A", "B", "R", "R2")
    }
    val ids = rs.map(_.getId)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .expand("(x)-[r2]->(y2)")
      .directedRelationshipByIdSeek("r", "x", "y", Set.empty, ids :_*)
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
    val (_, _, rs, _) = given {
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
    val (_, _, rs, _) = given {
      bidirectionalBipartiteGraph(nodesPerLabel, "A", "B", "R", "R2")
    }
    val ids = rs.map(_.getId)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .expand("(x)-[r2]->(y2)")
      .undirectedRelationshipByIdSeek("r", "x", "y", Set.empty, ids :_*)
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
    given {bipartiteGraph(nodesPerLabel, "A", "B", "R")}

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .expand("(x)-[r]->(y)")
      .apply()
      .|.nodeCountFromCountStore("count", Seq(Some("A")))
      .nodeByLabelScan("x", "A", IndexOrderNone)
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
    given {bipartiteGraph(nodesPerLabel, "A", "B", "R")}

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .expand("(x)-[r]->(y)")
      .apply()
      .|.relationshipCountFromCountStore("count", None, Seq("R"), None)
      .nodeByLabelScan("x", "A", IndexOrderNone)
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
    given {
      nodePropertyGraph(sizeHint, {
        case i => Map("prop" -> i)
      })
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter(s"x.prop >= ${sizeHint / 2}")
      .sort(Seq(Ascending("x")))
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
    given { nodeGraph(sizeHint) }

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
    given {
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
    val nodeCount = sizeHint * 10
    given {
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
    // Depending on morsel size, the limit might or might not cancel the second row of the expand.
    queryProfile.operatorProfile(3).rows() should (be >= 1L* nodeCount and be <= 2L*nodeCount) // expand
    queryProfile.operatorProfile(4).rows() shouldBe nodeCount // argument
    queryProfile.operatorProfile(5).rows() shouldBe nodeCount  // all node scan
  }

  test("should profile rows with limit + expand on RHS of ConditionalApply non-nullable") {
    val nodeCount = sizeHint
    given {
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
    // Depending on morsel size, the limit might or might not cancel the second row of the expand.
    queryProfile.operatorProfile(3).rows() should (be >= 1L* nodeCount and be <= 2L*nodeCount) // expand
    queryProfile.operatorProfile(4).rows() shouldBe nodeCount // argument
    queryProfile.operatorProfile(5).rows() shouldBe nodeCount  // all node scan for x
  }

  test("should profile rows with limit + expand on RHS of ConditionalApply nullable") {
    val values = (1 to sizeHint).map {
      case i if i % 2 == 0 => Array[Any](i)
      case _ => Array[Any](null)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .conditionalApply("x")
      .|.unwind("range(1, 10) AS i")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = profile(logicalQuery, runtime, inputValues(values:_*))
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe sizeHint / 2 + (10 * sizeHint / 2) // produce results
    queryProfile.operatorProfile(1).rows() shouldBe sizeHint / 2 + (10 * sizeHint / 2)// conditionalApply
    queryProfile.operatorProfile(2).rows() shouldBe 10 * sizeHint / 2 // unwind
    queryProfile.operatorProfile(3).rows() shouldBe sizeHint / 2 //argument
    queryProfile.operatorProfile(4).rows() shouldBe sizeHint  // input
  }

  test("should profile rows of skip") {
    given { nodeGraph(sizeHint) }

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
    given {
      circleGraph(sizeHint * 10)
    }

    //when
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
    given {
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
    given { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

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
    given {
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
    given {
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
    queryProfile.operatorProfile(2).rows() shouldBe (nodesPerLabel * nodesPerLabel) // pruning var expand (but nothing can be pruned)
    queryProfile.operatorProfile(3).rows() shouldBe (nodesPerLabel * 2L) // all node scan
  }

  test("should profile rows with shortest path") {
    // given
    val nodesPerLabel = 100
    given {
      for(_ <- 0 until nodesPerLabel)
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
    given {
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
    given {
      index("A", "prop")
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
    given {
      index("A", "prop")
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
    given {
      index("A", "prop")
      val (aNodes, _) = bipartiteGraph(nodesPerLabel, "A", "B", "R")
      aNodes.zipWithIndex.foreach {
        case (node, i) if i % 2 == 0 => node.setProperty("prop", 42)
        case (node, i) => node.setProperty("prop", 1337)
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
    given {
      index("A", "prop")
      val (aNodes, _) = bipartiteGraph(nodesPerLabel, "A", "B", "R")
      aNodes.foreach(_.setProperty("prop", "hello"))
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .expandInto("(x)-[r]->(y)")
      .expandAll("(x)-->(y)")
      .nodeIndexOperator("x:A(prop ENDS WITH 'lo')")
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
    val node = given {
      index("A", "prop")
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
    val aNodes = given {
      index("A", "prop")
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
    val runtimeResult = profile(logicalQuery, runtime, inputValues(aNodes.map(n => Array[Any](n)):_*).stream())
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
    val aNodes = given {
      index("A", "prop")
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
    val runtimeResult = profile(logicalQuery, runtime, inputValues(aNodes.map(n => Array[Any](n)):_*).stream())
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
    given {
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
    given {
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
    given {
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
    queryProfile.operatorProfile(0).rows() shouldBe (nodesPerLabel * nodesPerLabel * nodesPerLabel + nodesPerLabel) // produce results
    queryProfile.operatorProfile(1).rows() shouldBe (nodesPerLabel * nodesPerLabel * nodesPerLabel + nodesPerLabel) // apply
    queryProfile.operatorProfile(2).rows() shouldBe (nodesPerLabel * nodesPerLabel * nodesPerLabel + nodesPerLabel) // optional
    queryProfile.operatorProfile(3).rows() shouldBe (nodesPerLabel * nodesPerLabel * nodesPerLabel) // expand all
    queryProfile.operatorProfile(4).rows() shouldBe (nodesPerLabel * nodesPerLabel) // expand all
    queryProfile.operatorProfile(5).rows() shouldBe (nodesPerLabel * 2L) // argument
    queryProfile.operatorProfile(6).rows() shouldBe (nodesPerLabel * 2L) // all node scan
  }

  test("should profile rows with argument and var-expand") {
    // given
    val nodesPerLabel = 20
    given {
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
    queryProfile.operatorProfile(0).rows() shouldBe (nodesPerLabel * nodesPerLabel * nodesPerLabel + nodesPerLabel) // produce results
    queryProfile.operatorProfile(1).rows() shouldBe (nodesPerLabel * nodesPerLabel * nodesPerLabel + nodesPerLabel) // apply
    queryProfile.operatorProfile(2).rows() shouldBe (nodesPerLabel * nodesPerLabel * nodesPerLabel + nodesPerLabel) // optional
    queryProfile.operatorProfile(3).rows() shouldBe (nodesPerLabel * nodesPerLabel * nodesPerLabel) // expand all
    queryProfile.operatorProfile(4).rows() shouldBe (nodesPerLabel * nodesPerLabel) // var-expand
    queryProfile.operatorProfile(5).rows() shouldBe (nodesPerLabel * 2L) // argument
    queryProfile.operatorProfile(6).rows() shouldBe (nodesPerLabel * 2L) // all node scan
  }

  test("should profile rows with many node-by id seek and expand") {
    // given
    val nodesPerLabel = 100
    val aNodes = given {
      index("A", "prop")
      val (aNodes, _) = bipartiteGraph(nodesPerLabel, "A", "B", "R")
      aNodes.foreach(_.setProperty("prop", "hello"))
      aNodes
    }
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .expandInto("(x)-[r]->(y)")
      .expandAll("(x)-->(y)")
      .nodeByIdSeek("x", Set.empty, aNodes.map(_.getId):_*)
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
    val aNodes = given {
      index("A", "prop")
      val (aNodes, _) = bipartiteGraph(nodesPerLabel, "A", "B", "R")
      aNodes.foreach(_.setProperty("prop", "hello"))
      aNodes
    }
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .expandInto("(x)-[r]->(y)")
      .expandAll("(x)-->(y)")
      .nodeByIdSeek("x", Set.empty, aNodes.indices.map(i => if (i % 2 ==0) aNodes(i).getId else -1): _* )
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
    given {
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
    queryProfile.operatorProfile(0).rows() shouldBe (nodesPerLabel * nodesPerLabel * unwindCardinality + nodesPerLabel) // produce results
    queryProfile.operatorProfile(1).rows() shouldBe (nodesPerLabel * nodesPerLabel * unwindCardinality + nodesPerLabel) // apply
    queryProfile.operatorProfile(2).rows() shouldBe (nodesPerLabel * nodesPerLabel * unwindCardinality + nodesPerLabel) // optional
    queryProfile.operatorProfile(3).rows() shouldBe (nodesPerLabel * nodesPerLabel * unwindCardinality) // expand all
    queryProfile.operatorProfile(4).rows() shouldBe (nodesPerLabel * 2L * unwindCardinality) // unwind
    queryProfile.operatorProfile(5).rows() shouldBe (nodesPerLabel * 2L) // argument
    queryProfile.operatorProfile(6).rows() shouldBe (nodesPerLabel * 2L) // all node scan
  }

  test("should profile rows with optional expand into") {
    // given
    val nodesPerLabel = 100
    given { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

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
    given {
      nodePropertyGraph(sizeHint, {
        case i => Map("prop" -> i)
      })
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
    given {
      nodePropertyGraph(sizeHint, {
        case i => Map("prop" -> i)
      })
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
    given {
      nodePropertyGraph(sizeHint, {
        case i => Map("prop" -> i)
      })
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
    given {
      nodePropertyGraph(size, {
        case i => Map("prop" -> i)
      })
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
    given {
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
    given {
      index("L1", "prop")
      nodePropertyGraph(sizeHint, {
        case i if i % 10 == 0 => Map("prop" -> i)
      }, "L1")
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

  test("should profile rows of cartesian product") {
    val size = Math.sqrt(sizeHint).toInt
    given { nodeGraph(size) }

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
    val numberOfChunks = Math.ceil(size / cartesianProductChunkSize.toDouble).toInt
    result.runtimeResult.queryProfile().operatorProfile(1).rows() shouldBe size * size // cartesian product
    result.runtimeResult.queryProfile().operatorProfile(2).rows() shouldBe numberOfChunks * size // all node scan b
    result.runtimeResult.queryProfile().operatorProfile(3).rows() shouldBe size // all node scan a
  }

  test("should profile rows of union") {
    val size = Math.sqrt(sizeHint).toInt
    given { nodeGraph(size) }

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
}

trait NonParallelProfileRowsTestBase[CONTEXT <: RuntimeContext] {
  self: ProfileRowsTestBase[CONTEXT] =>

  test("should profile rows of procedure call") {
    // given
    registerProcedure(new BasicProcedure(ProcedureSignature.procedureSignature(Array[String](), "proc").mode(Mode.READ).in("j", Neo4jTypes.NTInteger).out("i", Neo4jTypes.NTInteger).build()) {
      override def apply(ctx: Context, input: Array[AnyValue], resourceTracker: ResourceTracker): RawIterator[Array[AnyValue], ProcedureException] = {
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
    result.runtimeResult.queryProfile().operatorProfile(1).rows() shouldBe sizeHint * 2// procedure call
    result.runtimeResult.queryProfile().operatorProfile(2).rows() shouldBe sizeHint // unwind
  }

  test("should profile rows with ordered distinct") {
    // given
    val nodes = given {
      nodeGraph(sizeHint)
    }

    val input = for (n <- nodes) yield Array[Any](nodes.head, n)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .orderedDistinct(Seq("x"), "y AS y")
      .input(nodes = Seq("x", "y"))
      .build()

    val runtimeResult = profile(logicalQuery, runtime, inputValues(input:_*))
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe sizeHint // produce results
    queryProfile.operatorProfile(1).rows() shouldBe sizeHint // orderedDistinct
    queryProfile.operatorProfile(2).rows() shouldBe sizeHint // input
  }

  test("should profile rows with ordered aggregation") {
    // given
    val nodes = given {
      nodeGraph(sizeHint)
    }

    val input = for (n <- nodes) yield Array[Any](nodes.head, n)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .orderedAggregation(Seq("x AS x", "y AS y"), Seq("collect(y) AS c"), Seq("x"))
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = profile(logicalQuery, runtime, inputValues(input:_*))
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
      .partialSort(Seq(Ascending("x")), Seq(Ascending("y")))
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
    val nodes = given {
      nodeGraph(sizeHint)
    }

    val input = for (n <- nodes) yield Array[Any](nodes.head, n)

    val limit = 123

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .partialTop(Seq(Ascending("x")), Seq(Ascending("y")), limit)
      .input(nodes = Seq("x", "y"))
      .build()

    val runtimeResult = profile(logicalQuery, runtime, inputValues(input:_*))
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe limit // produce results
    queryProfile.operatorProfile(1).rows() shouldBe limit // partial top
    queryProfile.operatorProfile(2).rows() shouldBe sizeHint // input
  }
}
