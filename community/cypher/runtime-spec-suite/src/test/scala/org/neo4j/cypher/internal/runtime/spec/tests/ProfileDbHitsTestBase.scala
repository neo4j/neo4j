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
import org.neo4j.cypher.internal.runtime.spec.interpreted.LegacyDbHitsTestBase
import org.neo4j.cypher.result.OperatorProfile
import org.neo4j.cypher.result.QueryProfile

abstract class ProfileDbHitsTestBase[CONTEXT <: RuntimeContext](
                                                                 edition: Edition[CONTEXT],
                                                                 runtime: CypherRuntime[CONTEXT],
                                                                 val sizeHint: Int,
                                                                 costOfGetPropertyChain: Long, // the reported dbHits for getting the property chain
                                                                 costOfPropertyJumpedOverInChain: Long, // the reported dbHits for a property in the chain that needs to be traversed in order to read another property in the chain
                                                                 costOfProperty: Long, // the reported dbHits for a single property lookup, after getting the property chain and getting to the right position
                                                                 costOfLabelLookup: Long, // the reported dbHits for finding the id of a label
                                                                 costOfExpandGetRelCursor: Long, // the reported dbHits for obtaining a relationship cursor for expanding
                                                                 costOfExpandOneRel: Long, // the reported dbHits for expanding one relationship
                                                                 costOfRelationshipTypeLookup: Long, // the reported dbHits for finding the id of a relationship type
                                                                 cartesianProductChunkSize: Long // The size of a LHS chunk for cartesian product
                                                               ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  protected def canFuseOverPipelines: Boolean = false

  test("should profile dbHits of all nodes scan") {
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
    queryProfile.operatorProfile(1).dbHits() should (be (sizeHint) or be (sizeHint + 1)) // all nodes scan
  }

  test("should profile dbHits of label scan") {
    given {
      nodeGraph(3, "Dud")
      nodeGraph(sizeHint, "It")
      nodeGraph(3, "Decoy")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeByLabelScan("x", "It", IndexOrderNone)
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(1).dbHits() shouldBe (sizeHint + 1 + costOfLabelLookup) // label scan
  }

  test("should profile dbHits of node index seek") {
    given {
      index("Language", "difficulty")
      nodePropertyGraph(sizeHint, {
        case i if i % 10 == 0 => Map("difficulty" -> i)
      }, "Language")
    }

    // when
    val seekProfile = profileIndexSeek(s"x:Language(difficulty >= ${sizeHint / 2})")
    // then
    seekProfile.operatorProfile(1).dbHits() shouldBe (sizeHint / 10 / 2 + 1) // node index seek
  }

  test("should profile dbHits of node index scan") {
    given {
      index("Language", "difficulty")
      nodePropertyGraph(sizeHint, {
        case i if i % 10 == 0 => Map("difficulty" -> i)
      }, "Language")
    }

    // when
    val scanProfile = profileIndexSeek("x:Language(difficulty)")
    // then
    scanProfile.operatorProfile(1).dbHits() shouldBe (sizeHint / 10 + 1) // node index scan
  }

  test("should profile dbHits of node index contains") {
    given {
      index("Language", "difficulty")
      nodePropertyGraph(sizeHint, {
        case i => Map("difficulty" -> s"x${i % 2}")
      }, "Language")
    }

    // when
    val seekProfile = profileIndexSeek(s"x:Language(difficulty CONTAINS '1')")
    // then
    seekProfile.operatorProfile(1).dbHits() shouldBe (sizeHint / 2 + 1) // node index contains
  }

  private def profileIndexSeek(indexSeekString: String): QueryProfile = {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator(indexSeekString)
      .build()

    val result = profile(logicalQuery, runtime)
    consume(result)

    result.runtimeResult.queryProfile()
  }

  test("should profile dbHits of node by id") {
    // given
    val nodes = given { nodeGraph(17) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeByIdSeek("x", Set.empty, nodes(7).getId, nodes(11).getId, nodes(13).getId)
      .build()

    val result = profile(logicalQuery, runtime)
    consume(result)

    // then
    result.runtimeResult.queryProfile().operatorProfile(1).dbHits() shouldBe 3 // node by id
  }

  test("should profile dbHits of directed relationship by id") {
    // given
    val (_, rels) = given { circleGraph(17) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .directedRelationshipByIdSeek("r", "x", "y", Set.empty, rels(13).getId)
      .build()

    val result = profile(logicalQuery, runtime)
    consume(result)

    // then
    result.runtimeResult.queryProfile().operatorProfile(1).dbHits() shouldBe 1
  }

  test("should profile dbhits with expand into") {
    // given
    given { circleGraph(sizeHint) }

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
    // expand into. If no node is in the input twice the rel cache does not help and then you get
    // 2 (check start and end for `isDense`) + costOfExpand per row.
    queryProfile.operatorProfile(1).dbHits() shouldBe (sizeHint * (2L + (costOfExpandGetRelCursor + costOfExpandOneRel)))
    // No assertion on the expand because db hits can vary, given that the nodes have 2 relationships.
  }

  test("should profile dbhits with optional expand all") {
    // given
    val extraNodes = 20
    given {
      starGraph(sizeHint, "Center", "Ring")
      nodeGraph(extraNodes, "Ring")
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .optionalExpandAll("(x)-->(y)")
      .nodeByLabelScan("x", "Ring", IndexOrderNone)
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(1).dbHits() shouldBe (sizeHint * (LegacyDbHitsTestBase.costOfExpandGetRelCursor + LegacyDbHitsTestBase.costOfExpandOneRel) + extraNodes) // optional expand all
    queryProfile.operatorProfile(2).dbHits() shouldBe (sizeHint + extraNodes + 1 + costOfLabelLookup) // label scan
  }

  test("should profile dbhits with optional expand into") {
    // given
    val n = Math.sqrt(sizeHint).toInt
    val extraNodes = 20
    given {
      val xs = nodePropertyGraph(n, {
        case i: Int => Map("prop" -> i)
      }, "X")
      val ys = nodePropertyGraph(n, {
        case i: Int => Map("prop" -> i)
      }, "Y")

      connect(xs ++ ys, xs.indices.map(i => (i, i + xs.length, "R")))

      nodePropertyGraph(extraNodes, {
        case i: Int => Map("prop" -> (i + xs.length))
      }, "X")
      nodePropertyGraph(extraNodes, {
        case i: Int => Map("prop" -> (i + ys.length))
      }, "Y")
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .optionalExpandInto("(x)-->(y)")
      .apply()
      .|.filter("x.prop = y.prop") // Make sure we get pairs of x/y nodes with each node only appearing once
      .|.nodeByLabelScan("y", "Y", IndexOrderNone, "x")
      .nodeByLabelScan("x", "X", IndexOrderNone)
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    // optional expand into (uses legacy pipe). If no node is in the input twice the rel cache does not help and then you get
    // 2 (check start and end for `isDense`) + costOfExpand for each relationship on top.
    queryProfile.operatorProfile(1).dbHits() shouldBe ((n + extraNodes) * 2 + n * costOfExpandOneRel) // optional expand into
    queryProfile.operatorProfile(2).dbHits() shouldBe 0 // apply
    queryProfile.operatorProfile(3).dbHits() shouldBe ((n + extraNodes) * (n + extraNodes) * 2 * (costOfGetPropertyChain + costOfProperty)) // filter (reads 2 properties))
    queryProfile.operatorProfile(4).dbHits() shouldBe  ((n + extraNodes) * (n + extraNodes + 1) + costOfLabelLookup) // label scan OK
    queryProfile.operatorProfile(5).dbHits() shouldBe (n + extraNodes + 1 + costOfLabelLookup) // label scan OK
  }

  test("should profile dbHits with node hash join") {
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
    val fusedCostOfGetPropertyChain = if (canFuseOverPipelines) 0 else costOfGetPropertyChain
    queryProfile.operatorProfile(1).dbHits() shouldBe 0 // node hash join
    queryProfile.operatorProfile(2).dbHits() shouldBe sizeHint * (fusedCostOfGetPropertyChain + costOfProperty) // filter
    queryProfile.operatorProfile(3).dbHits() should (be (sizeHint) or be (sizeHint + 1)) // all node scan
    queryProfile.operatorProfile(4).dbHits() shouldBe sizeHint * (fusedCostOfGetPropertyChain + costOfProperty) // filter
    queryProfile.operatorProfile(5).dbHits() should (be (sizeHint) or be (sizeHint + 1)) // all node scan
  }

  test("should profile dbHits with value hash join") {
    given {
      nodePropertyGraph(sizeHint, {
        case i => Map("prop" -> i)
      })
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .valueHashJoin("x.prop = y.prop")
      .|.allNodeScan("y")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(1).dbHits() shouldBe 2 * sizeHint // value hash join
    queryProfile.operatorProfile(2).dbHits() should (be (sizeHint) or be (sizeHint + 1)) // all node scan
    queryProfile.operatorProfile(3).dbHits() should (be (sizeHint) or be (sizeHint + 1)) // all node scan
  }

  test("should profile dbHits of cached properties") {
    given {
      nodePropertyGraph(sizeHint, { case i => Map("p" -> i) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .projection("cache[n.p] AS x")
      .cacheProperties("cache[n.p]")
      .allNodeScan("n")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    val fusedCostOfGetPropertyChain = if (canFuseOverPipelines) 0 else costOfGetPropertyChain
    queryProfile.operatorProfile(1).dbHits() shouldBe 0 // projection
    queryProfile.operatorProfile(2).dbHits() shouldBe (sizeHint * (fusedCostOfGetPropertyChain + costOfProperty)) // cacheProperties
  }

  test("should profile dbHits with apply") {
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
    val fusedCostOfGetPropertyChain = if (canFuseOverPipelines) 0 else costOfGetPropertyChain
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(1).dbHits() shouldBe (size / 2 * size) * (costOfGetPropertyChain + costOfProperty) // filter
    queryProfile.operatorProfile(2).dbHits() shouldBe 0 // apply
    queryProfile.operatorProfile(3).dbHits() shouldBe size * size * (fusedCostOfGetPropertyChain + costOfProperty) // filter
    queryProfile.operatorProfile(4).dbHits() should (be (size * size) or be (size * (1+size))) // all node scan
    queryProfile.operatorProfile(5).dbHits() should (be (size) or be (size + 1)) // all node scan
  }

  test("should profile dbHits of expression reads") {
    given { nodePropertyGraph(sizeHint, { case i => Map("list" -> Array(i, i+1), "prop" -> i)}) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("exists")
      .projection("[p IN x.list | x.prop = p] AS exists")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val fusedCostOfGetPropertyChain = if (canFuseOverPipelines) 0 else costOfGetPropertyChain
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    // assertions on property dbHits are tricky because in pipelined more dbHits are reported for
    // properties late in the chain, while in interpreted/slotted all property reads cost only 1 dbHit
    queryProfile.operatorProfile(1).dbHits() should
      (be (sizeHint * (2 * (fusedCostOfGetPropertyChain + costOfProperty) + (fusedCostOfGetPropertyChain + costOfPropertyJumpedOverInChain + costOfProperty))) or // prop is the first prop
        be (sizeHint * ((fusedCostOfGetPropertyChain + costOfProperty) + 2 * (fusedCostOfGetPropertyChain + costOfPropertyJumpedOverInChain + costOfProperty)))) // prop is the second prop
  }

  test("should profile dbHits of aggregation") {
    given { nodePropertyGraph(sizeHint, { case i => Map("prop" -> i)}) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("avg")
      .aggregation(Seq.empty, Seq("avg(x.prop) AS avg"))
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val fusedCostOfGetPropertyChain = if (canFuseOverPipelines) 0 else costOfGetPropertyChain
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(1).dbHits() shouldBe sizeHint * (fusedCostOfGetPropertyChain + costOfProperty)
  }

  test("should profile dbHits of aggregation with grouping") {
    // given
    val aggregationGroups = sizeHint / 2
    given { nodePropertyGraph(sizeHint, { case i => Map("group" -> i % aggregationGroups, "prop" -> i)}) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("group", "avg")
      .aggregation(Seq("x.group AS group"), Seq("avg(x.prop) AS avg"))
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val fusedCostOfGetPropertyChain = if (canFuseOverPipelines) 0 else costOfGetPropertyChain
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(1).dbHits() shouldBe (sizeHint *
      ((fusedCostOfGetPropertyChain + costOfProperty) // first prop in chain
        + (fusedCostOfGetPropertyChain + costOfPropertyJumpedOverInChain + costOfProperty))) // second prop in chain
  }

  test("should profile dbHits of cartesian product") {
    // given
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
    result.runtimeResult.queryProfile().operatorProfile(1).dbHits() shouldBe 0 // cartesian product
    result.runtimeResult.queryProfile().operatorProfile(2).dbHits() should (be (numberOfChunks * size) or be (numberOfChunks * (size + 1))) // all node scan b
    result.runtimeResult.queryProfile().operatorProfile(3).dbHits() should (be (size) or be (size + 1)) // all node scan a
  }

  test("should profile dbHits of skip") {
    given {
      nodePropertyGraph(sizeHint, { case i => Map("p" -> i) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .projection("n.p AS x")
      .skip(sizeHint - 1)
      .allNodeScan("n")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val propCost = if (canFuseOverPipelines) 0 else costOfGetPropertyChain
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).dbHits() shouldBe 0 // produce result
    queryProfile.operatorProfile(1).dbHits() shouldBe 1 + propCost// projection
    queryProfile.operatorProfile(2).dbHits() shouldBe 0 // skip
    queryProfile.operatorProfile(3).dbHits() should (be (sizeHint) or be (sizeHint + 1))  //allNodesScan
  }

  test("should profile dbHits of union") {
    // given
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
    result.runtimeResult.queryProfile().operatorProfile(1).dbHits() shouldBe 0 // union
    result.runtimeResult.queryProfile().operatorProfile(2).dbHits() should (be (size) or be (size + 1)) // all node scan
    result.runtimeResult.queryProfile().operatorProfile(3).dbHits() should (be (size) or be (size + 1)) // all node scan
  }
}

trait ProcedureCallDbHitsTestBase[CONTEXT <: RuntimeContext] {
  self: ProfileDbHitsTestBase[CONTEXT] =>

  test("should profile dbHits of procedure call") {
    // given
    given { nodeGraph(sizeHint) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .procedureCall("db.awaitIndexes()")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(1).dbHits() shouldBe OperatorProfile.NO_DATA // procedure call
    queryProfile.operatorProfile(2).dbHits() should (be (sizeHint) or be (sizeHint + 1)) // all node scan
  }
}

trait NestedPlanDbHitsTestBase[CONTEXT <: RuntimeContext] {
  self: ProfileDbHitsTestBase[CONTEXT] =>

  test("should profile dbHits of nested plan expression") {
    val size = Math.sqrt(sizeHint).toInt
    given { nodeGraph(size) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "list")
      .nestedPlanCollectExpressionProjection("list", "b")
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()

    val result = profile(logicalQuery, runtime)
    consume(result)

    // then
    result.runtimeResult.queryProfile().operatorProfile(1).dbHits() should (be (size * size) or be (size * (size + 1))) // projection w. nested plan expression
  }
}
