/*
 * Copyright (c) "Neo4j"
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
import org.neo4j.cypher.internal.expressions.MultiRelationshipPathStep
import org.neo4j.cypher.internal.expressions.NilPathStep
import org.neo4j.cypher.internal.expressions.NodePathStep
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeWithProperties
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationship
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.interpreted.LegacyDbHitsTestBase
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.result.OperatorProfile
import org.neo4j.cypher.result.QueryProfile

abstract class ProfileDbHitsTestBase[CONTEXT <: RuntimeContext](
                                                                 val edition: Edition[CONTEXT],
                                                                 runtime: CypherRuntime[CONTEXT],
                                                                 val sizeHint: Int,
                                                                 costOfGetPropertyChain: Long, // the reported dbHits for getting the property chain
                                                                 val costOfPropertyToken: Long, // the reported dbHits for looking up a property token
                                                                 val costOfSetProperty: Long, // the reported dbHits for setting a property token
                                                                 costOfPropertyJumpedOverInChain: Long, // the reported dbHits for a property in the chain that needs to be traversed in order to read another property in the chain
                                                                 val costOfProperty: Long, // the reported dbHits for a single property lookup, after getting the property chain and getting to the right position
                                                                 costOfLabelLookup: Long, // the reported dbHits for finding the id of a label
                                                                 costOfExpandGetRelCursor: Long, // the reported dbHits for obtaining a relationship cursor for expanding
                                                                 costOfExpandOneRel: Long, // the reported dbHits for expanding one relationship
                                                                 costOfRelationshipTypeLookup: Long, // the reported dbHits for finding the id of a relationship type
                                                                 val costOfCompositeUniqueIndexCursorRow: Long, // the reported dbHits for finding one row from a composite unique index
                                                                 cartesianProductChunkSize: Long, // The size of a LHS chunk for cartesian product
                                                                 canFuseOverPipelines: Boolean,
                                                                 createsRelValueInExpand: Boolean
                                                               ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

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

  test("should profile dbHits of node index seek with range predicate") {
    given {
      nodeIndex("Language", "difficulty")
      nodePropertyGraph(sizeHint, {
        case i if i % 10 == 0 => Map("difficulty" -> i)
      }, "Language")
    }

    // when
    val seekProfile = profileIndexSeek(s"x:Language(difficulty >= ${sizeHint / 2})")
    // then
    seekProfile.operatorProfile(1).dbHits() shouldBe (sizeHint / 10 / 2 + 1) // node index seek
  }

  test("should profile dbHits of node index seek with IN predicate") {
    val nodes = given {
      nodeIndex("Language", "difficulty")
      nodePropertyGraph(sizeHint, {
        case i if i % 10 == 0 => Map("difficulty" -> i)
      }, "Language")
    }

    val difficulties = nodes.filter(_.hasProperty("difficulty")).map(_.getProperty("difficulty").asInstanceOf[Int].longValue())

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator("x:Language(difficulty IN ???)", paramExpr = Some(listOfInt(difficulties:_*)))
      .build()

    val result = profile(logicalQuery, runtime)
    consume(result)

    val seekProfile = result.runtimeResult.queryProfile()

    // then
    seekProfile.operatorProfile(1).dbHits() shouldBe (difficulties.size /*row of seek*/ + 1 * difficulties.size /*last next per seek*/) // node index seek
  }

  test("should profile dbHits of node index seek with IN predicate on composite index") {
    val nodes = given {
      nodeIndex("Language", "difficulty", "usefulness")
      nodePropertyGraph(sizeHint, {
        case i if i % 10 == 0 => Map("difficulty" -> i, "usefulness" -> i)
      }, "Language")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator(s"x:Language(difficulty >= ${sizeHint / 2}, usefulness >= ${sizeHint / 2})")
      .build()

    val result = profile(logicalQuery, runtime)
    consume(result)
    val seekProfile = result.runtimeResult.queryProfile()

    // then
    val expectedRowCount: Int = sizeHint / 10 / 2
    result should beColumns("x").withRows(rowCount(expectedRowCount))
    seekProfile.operatorProfile(1).dbHits() shouldBe (expectedRowCount + 1 /*DB Hits are incremented per next() call, even though last call will return false*/)
  }

  test("should profile dbHits of node index scan") {
    given {
      nodeIndex("Language", "difficulty")
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
      nodeIndex("Language", "difficulty")
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

  test("should profile dbHits of directed relationship index exact seek") {
    // given
    given {
      relationshipIndex("R", "difficulty")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("difficulty", i % 10)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .relationshipIndexOperator(s"(x)-[r:R(difficulty = 3)]->(y)")
      .build()

    val result = profile(logicalQuery, runtime)
    consume(result)

    // then, in interpreted runtime it is free to call relationshipById whereas in the other cases this is counted as a dbhit
    result.runtimeResult.queryProfile().operatorProfile(1).dbHits() should(be (sizeHint / 10 + 1)  or be (2 * (sizeHint / 10) + 1))
  }

  test("should profile dbHits of directed relationship index range seek") {
    // given
    given {
      relationshipIndex("R", "difficulty")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => r.setProperty("difficulty", i)
        case _ =>
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .relationshipIndexOperator(s"(x)-[r:R(difficulty >= ${sizeHint / 2})]->(y)")
      .build()

    val result = profile(logicalQuery, runtime)
    consume(result)

    // then, in interpreted runtime it is free to call relationshipById whereas in the other cases this is counted as a dbhit
    result.runtimeResult.queryProfile().operatorProfile(1).dbHits() should(be (sizeHint / 10 / 2 + 1)  or be (2*(sizeHint / 10 / 2) + 1))
  }

  test("should profile dbHits of directed relationship multiple index exact seek") {
    // given
    given {
      relationshipIndex("R", "difficulty")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("difficulty", i % 10)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .relationshipIndexOperator(s"(x)-[r:R(difficulty = 3 OR 4)]->(y)")
      .build()

    val result = profile(logicalQuery, runtime)
    consume(result)

    // then, in interpreted runtime it is free to call relationshipById whereas in the other cases this is counted as a dbhit
    result.runtimeResult.queryProfile().operatorProfile(1).dbHits() should(be (sizeHint / 5 + 2)  or be (2 * (sizeHint / 5) + 2))
  }

  test("should profile dbHits of undirected relationship index exact seek") {
    // given
    given {
      relationshipIndex("R", "difficulty")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("difficulty", i % 10)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .relationshipIndexOperator(s"(x)-[r:R(difficulty = 3)]-(y)")
      .build()

    val result = profile(logicalQuery, runtime)
    consume(result)

    // then, in interpreted runtime it is free to call relationshipById whereas in the other cases this is counted as a dbhit
    result.runtimeResult.queryProfile().operatorProfile(1).dbHits() should(be (sizeHint / 10 + 1)  or be (2 * (sizeHint / 10) + 1))
  }

  test("should profile dbHits of undirected relationship range index seek") {
    // given
    given {
      relationshipIndex("R", "difficulty")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => r.setProperty("difficulty", i)
        case _ =>
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .relationshipIndexOperator(s"(x)-[r:R(difficulty >= ${sizeHint / 2})]-(y)")
      .build()

    val result = profile(logicalQuery, runtime)
    consume(result)

    // then, in interpreted runtime it is free to call relationshipById whereas in the other cases this is counted as a dbhit
    result.runtimeResult.queryProfile().operatorProfile(1).dbHits() should(be (sizeHint / 10 / 2 + 1)  or be (2*(sizeHint / 10 / 2) + 1))
  }

  test("should profile dbHits of undirected relationship multiple index exact seek") {
    // given
    given {
      relationshipIndex("R", "difficulty")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("difficulty", i % 10)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .relationshipIndexOperator(s"(x)-[r:R(difficulty = 3 OR 4)]-(y)")
      .build()

    val result = profile(logicalQuery, runtime)
    consume(result)

    // then, in interpreted runtime it is free to call relationshipById whereas in the other cases this is counted as a dbhit
    result.runtimeResult.queryProfile().operatorProfile(1).dbHits() should(be (sizeHint / 5 + 2)  or be (2 * (sizeHint / 5) + 2))
  }

  test("should profile dbHits of directed relationship index scan") {
    given {
      relationshipIndex("R", "difficulty")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i)  if i % 10 == 0 => r.setProperty("difficulty", i)
        case _ =>
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .relationshipIndexOperator(s"(x)-[r:R(difficulty)]->(y)")
      .build()

    val result = profile(logicalQuery, runtime)
    consume(result)

    // then, in interpreted runtime it is free to call relationshipById whereas in the other cases this is counted as a dbhit
    result.runtimeResult.queryProfile().operatorProfile(1).dbHits() should (be (sizeHint / 10 + 1) or be (2 * (sizeHint / 10) + 1))
  }

  test("should profile dbHits of undirected relationship index scan") {
    given {
      relationshipIndex("R", "difficulty")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i)  if i % 10 == 0 => r.setProperty("difficulty", i)
        case _ =>
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .relationshipIndexOperator(s"(x)-[r:R(difficulty)]-(y)")
      .build()

    val result = profile(logicalQuery, runtime)
    consume(result)

    // then, in interpreted runtime it is free to call relationshipById whereas in the other cases this is counted as a dbhit
    result.runtimeResult.queryProfile().operatorProfile(1).dbHits() should (be (sizeHint / 10 + 1) or be (2 * (sizeHint / 10) + 1))
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

  test("should profile dbHits with project endpoints") {
    // given
    val nNodes = Math.sqrt(sizeHint).ceil.toInt
    val (aNodes, bNodes, aRels, _) = given { bidirectionalBipartiteGraph(nNodes, "A", "B", "R", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .projectEndpoints("(x)-[r]-(y)", startInScope = false, endInScope = false)
      .input(relationships = Seq("r"), nullable = false)
      .build()

    val input = aRels.map(r => Array[Any](r))

    // then
    val runtimeResult = profile(logicalQuery, runtime, inputValues(input:_*))
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    //Note: in interpreted we don't count rel.getStart, rel.getEnd as dbhits
    queryProfile.operatorProfile(1).dbHits() should (be (0) or be (aRels.size)) // project endpoints
    queryProfile.operatorProfile(2).dbHits() shouldBe 0 // input
  }

  test("should profile dbHits of populating nodes in produceresults") {
    given { nodePropertyGraph(sizeHint, { case i => Map("p" -> i, "q" -> -i) }, "somelabel")}

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()

    queryProfile.operatorProfile(0).dbHits() should be (sizeHint * (1 /* read node */ + 2 * costOfProperty)) // produceresults
  }

  test("should profile dbHits of populating relationships in produceresults") {
    // given
    given {
      val nodes = nodeGraph(sizeHint)
      connectWithProperties(nodes, nodes.indices.map(i => (i, (i + 1) % nodes.length, "Rel", Map("p" -> i, "q" -> -i))))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .expandAll("(x)-[r]->(y)")
      .allNodeScan("x")
      .build()

    val result = profile(logicalQuery, runtime)
    consume(result)

    val dbHitsForRelRead = if (createsRelValueInExpand) 0 else 1

    // then
    result.runtimeResult.queryProfile().operatorProfile(0).dbHits() should be (sizeHint * (dbHitsForRelRead + 2 * costOfProperty)) // produceresults
  }

  test("should profile dbHits of populating collections in produceresults") {
    // given
    given {
      nodePropertyGraph(sizeHint, { case i => Map("p" -> i, "q" -> -i) }, "somelabel")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("list", "map")
      .projection("{key: list} AS map")
      .aggregation(Seq(), Seq("collect(x) AS list"))
      .allNodeScan("x")
      .build()

    val result = profile(logicalQuery, runtime)
    consume(result)

    // then
    result.runtimeResult.queryProfile().operatorProfile(0).dbHits() should be (sizeHint * (1 /* read node */ + 2 * costOfProperty)) // produceresults
  }
}

trait UniqueIndexDbHitsTestBase[CONTEXT <: RuntimeContext] {
  self: ProfileDbHitsTestBase[CONTEXT] =>

  test("should profile dbHits of node index seek with IN predicate on locking unique index") {
    val nodes = given {
      uniqueIndex("Language", "difficulty")
      nodePropertyGraph(sizeHint, {
        case i if i % 10 == 0 => Map("difficulty" -> i)
      }, "Language")
    }

    val difficulties = nodes.filter(_.hasProperty("difficulty")).map(_.getProperty("difficulty").asInstanceOf[Int].longValue())

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator("x:Language(difficulty IN ???)", paramExpr = Some(listOfInt(difficulties: _*)), unique = true)
      .build(readOnly = false)

    val result = profile(logicalQuery, runtime)
    consume(result)

    val seekProfile = result.runtimeResult.queryProfile()

    // then
    val expectedRowCount = difficulties.size * costOfCompositeUniqueIndexCursorRow /*DB Hits are incremented per next() call, even though last call will return false*/
    seekProfile.operatorProfile(1).dbHits() shouldBe expectedRowCount // locking unique node index seek
  }

  test("should profile dbHits of node index seek with node key") {
    val nodes = given {
      nodeKey("Language", "difficulty", "usefulness")
      nodePropertyGraph(sizeHint, {
        case i if i % 10 == 0 => Map("difficulty" -> i, "usefulness" -> i)
        case i => Map("difficulty" -> -i, "usefulness" -> -i)
      }, "Language")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator(s"x:Language(difficulty = 0, usefulness = 0)", unique = true)
      .build(readOnly = false)

    val result = profile(logicalQuery, runtime)
    consume(result)
    val seekProfile = result.runtimeResult.queryProfile()

    // then
    val expectedRowCount = 1
    result should beColumns("x").withRows(rowCount(expectedRowCount))

    val expectedDbHits = expectedRowCount * costOfCompositeUniqueIndexCursorRow
    seekProfile.operatorProfile(1).dbHits() shouldBe expectedDbHits
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

trait WriteOperatorsDbHitsTestBase[CONTEXT <: RuntimeContext] {
  self: ProfileDbHitsTestBase[CONTEXT] =>

  test("should profile rows of set property correctly") {
    // given
    given {
      bipartiteGraph(sizeHint, "A", "B", "R")
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .setProperty("x", "prop", "1")
      .nodeByLabelScan("x", "A")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    val produceResultProfile = queryProfile.operatorProfile(0)
    val setPropertyProfile = queryProfile.operatorProfile(1)

    setPropertyProfile.rows() shouldBe sizeHint
    setPropertyProfile.dbHits() shouldBe 3 * costOfPropertyToken /*LazyPropertyKey: look up - create property token - look up*/ + sizeHint * costOfProperty
    produceResultProfile.rows() shouldBe sizeHint
    produceResultProfile.dbHits() shouldBe sizeHint * (costOfProperty + costOfProperty)
  }

  test("should profile db hits of delete node") {
    // given
    val nodeCount = sizeHint
    nodeGraph(nodeCount, "Label", "OtherLabel")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .deleteNode("n")
      .allNodeScan("n")
      .build(readOnly = false)

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()

    val produceResultsProfile = queryProfile.operatorProfile(0)
    produceResultsProfile.dbHits() shouldBe 0

    val deleteNodeResultsProfile = queryProfile.operatorProfile(1)
    deleteNodeResultsProfile.dbHits() shouldBe nodeCount
  }

  test("should profile db hits of detach delete node") {
    // given
    val chainCount = 3
    val chainLength = sizeHint / chainCount
    val types = Range(0, chainLength).map(_ => "LIKES")
    chainGraphs(chainCount, types:_*)

    val nodeCount = (chainLength + 1) * chainCount
    val relationshipCount = chainCount * chainLength

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .detachDeleteNode("n")
      .allNodeScan("n")
      .build(readOnly = false)

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()

    val produceResultsProfile = queryProfile.operatorProfile(0)
    produceResultsProfile.dbHits() shouldBe 0

    val deleteNodeResultsProfile = queryProfile.operatorProfile(1)
    deleteNodeResultsProfile.dbHits() shouldBe (nodeCount + relationshipCount)
  }

  test("should profile db hits of delete relationship") {
    // given
    val chainCount = 3
    val chainLength = sizeHint / chainCount
    val types = Range(0, chainLength).map(_ => "LIKES")
    chainGraphs(chainCount, types:_*)
    val relationshipCount = chainCount * chainLength

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .deleteRelationship("r")
      .expand("(n)-[r]->()")
      .allNodeScan("n")
      .build(readOnly = false)

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()

    val produceResultsProfile = queryProfile.operatorProfile(0)
    produceResultsProfile.dbHits() shouldBe 0

    val deleteNodeResultsProfile = queryProfile.operatorProfile(1)
    deleteNodeResultsProfile.dbHits() shouldBe relationshipCount
  }

  test("should profile db hits of detach delete path") {
    // given
    val chainCount = 3
    val chainLength = sizeHint / chainCount
    val types = Range(0, chainLength - 1).map(_ => "LIKES") :+ "LAST_LIKE"
    chainGraphs(chainCount, types:_*)

    val nodeCount = (chainLength + 1) * chainCount
    val relationshipCount = chainCount * chainLength

    // when
    val path = PathExpression(
      NodePathStep(
        node = varFor("n"),
        MultiRelationshipPathStep(
          rel = varFor("r"),
          direction = OUTGOING,
          toNode = Some(varFor("m")),
          next = NilPathStep
        )
      )
    )(InputPosition.NONE)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .detachDeletePath("p")
      .projection(Map("p" -> path))
      .expand(s"(n)-[r:*$chainLength]-(m)")
      .nodeByLabelScan("n", "START")
      .build(readOnly = false)

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()

    val produceResultsProfile = queryProfile.operatorProfile(0)
    produceResultsProfile.dbHits() shouldBe 0

    val deleteNodeResultsProfile = queryProfile.operatorProfile(1)
    deleteNodeResultsProfile.dbHits() shouldBe (nodeCount + relationshipCount)
  }

  test("should profile db hits of delete path") {
    // given
    val nodeCount = sizeHint
    nodeGraph(nodeCount)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .deletePath("p")
      .projection(Map("p" -> PathExpression(NodePathStep(varFor("n"), NilPathStep))(InputPosition.NONE)))
      .allNodeScan("n")
      .build(readOnly = false)

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()

    val produceResultsProfile = queryProfile.operatorProfile(0)
    produceResultsProfile.dbHits() shouldBe 0

    val deleteNodeResultsProfile = queryProfile.operatorProfile(1)
    deleteNodeResultsProfile.dbHits() shouldBe nodeCount
  }

  test("should profile db hits of delete expression") {
    // given
    val nodeCount = sizeHint
    nodeGraph(nodeCount)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("map")
      .deleteExpression("map.node")
      .projection("{node: n} AS map")
      .allNodeScan("n")
      .build(readOnly = false)

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()

    val produceResultsProfile = queryProfile.operatorProfile(0)
    produceResultsProfile.dbHits() shouldBe 0

    val deleteNodeResultsProfile = queryProfile.operatorProfile(1)
    deleteNodeResultsProfile.dbHits() shouldBe nodeCount
  }

  test("should profile db hits on remove labels") {
    // given
    val nodeCount = sizeHint
    given {
      nodeGraph(nodeCount - 3, "Label", "OtherLabel", "ThirdLabel")
      nodeGraph(1, "Label")
      nodeGraph(1, "OtherLabel")
      nodeGraph(1, "ThirdLabel")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .removeLabels("n", "Label", "OtherLabel")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    val queryProfile = runtimeResult.runtimeResult.queryProfile()

    val removeLabelsProfile = queryProfile.operatorProfile(1)
    val expectedLabelLookups = 2
    removeLabelsProfile.dbHits() shouldBe (expectedLabelLookups + nodeCount)
  }

  test("should profile db hits on create nodes with labels and properties") {
    val createCount = 9
    val labels = Seq("A", "B", "C")
    val properties = Map("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4, "e" -> 5)
    // given empty db

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .create(createNodeWithProperties("n", labels, propertiesString(properties)))
      .unwind(s"${Range(0, createCount).mkString("[",",","]")} AS i")
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    val queryProfile = runtimeResult.runtimeResult.queryProfile()

    val createProfile = queryProfile.operatorProfile(1)
    val createLabelsHits = labels.size
    val createNodesHits = createCount
    val createPropKeyHits = createCount * properties.size
    val setPropertyHits = createCount * properties.size
    createProfile.dbHits() shouldBe (createNodesHits + createLabelsHits + createPropKeyHits + setPropertyHits)
  }

  protected def propertiesString(properties: Map[String, Any]): String = {
    properties.map { case (key, value) => s"$key: $value" }.mkString("{", ",", "}")
  }
}

trait NonFusedWriteOperatorsDbHitsTestBase[CONTEXT <: RuntimeContext] extends WriteOperatorsDbHitsTestBase[CONTEXT] {
  self: ProfileDbHitsTestBase[CONTEXT] =>

  test("should profile db hits on create nodes and relationships (fused)") {
    val createNodeCount = 7
    val createNodes = Range(0, createNodeCount).map(i => createNode(s"n$i"))
    val properties = Map("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4, "e" -> 5)
    val createRelationships = createNodes
      .sliding(2)
      .zipWithIndex
      .map {
        case (Seq(a, b), i) => createRelationship(s"r$i", a.idName, "REL", b.idName, properties = Some(propertiesString(properties)))
      }
      .toSeq
    // given empty db

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults()
      .emptyResult()
      .create(createNodes, createRelationships)
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    val queryProfile = runtimeResult.runtimeResult.queryProfile()

    val createProfile = queryProfile.operatorProfile(2)
    val propertyHits = createRelationships.size * properties.size * 2 // TODO Looks like we count to much here
    val relationshipHits = /* create relationships */ createRelationships.size + /* create relationship type */ createRelationships.size
    val nodeHits = createNodeCount
    createProfile.dbHits() shouldBe (nodeHits + relationshipHits + propertyHits)
  }
}
