/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import org.neo4j.cypher.internal.{CypherRuntime, RuntimeContext}

abstract class ProfileRowsTestBase[CONTEXT <: RuntimeContext](edition: Edition[CONTEXT],
                                                              runtime: CypherRuntime[CONTEXT],
                                                              sizeHint: Int,
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
      val nodes = nodeGraph(sizeHint * 10)
      connect(nodes, Seq((1, 2, "REL")))
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .expand("(x)-->(y)")
      .limit(sizeHint * 2)
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe 1 // produce results
    queryProfile.operatorProfile(1).rows() shouldBe 1 // expand
    queryProfile.operatorProfile(2).rows() shouldBe sizeHint * 2 // limit
    queryProfile.operatorProfile(3).rows() should be >= (sizeHint * 2L) // all node scan
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
      .nodeByLabelScan("a", "A")
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
    queryProfile.operatorProfile(0).rows() shouldBe 10000 // produce results
    queryProfile.operatorProfile(1).rows() shouldBe 10000 // expand into
    queryProfile.operatorProfile(2).rows() shouldBe 10000 // expand all
    queryProfile.operatorProfile(3).rows() should be >= (nodesPerLabel * 2L) // all node scan
  }

  test("should profile rows with optional expand into") {
    // given
    val nodesPerLabel = 100
    given { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .optionalExpandInto("(x)-[r]->(y)", Some("true"))
      .expandAll("(x)-->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe 10000 // produce results
    queryProfile.operatorProfile(1).rows() shouldBe 10000 // optional expand into
    queryProfile.operatorProfile(2).rows() shouldBe 10000 // expand all
    queryProfile.operatorProfile(3).rows() should be >= (nodesPerLabel * 2L) // all node scan
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
      .nodeByLabelScan("x", "L1")
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
}
