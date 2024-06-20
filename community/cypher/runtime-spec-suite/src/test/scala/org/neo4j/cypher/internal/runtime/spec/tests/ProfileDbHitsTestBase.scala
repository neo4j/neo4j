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
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.MultiRelationshipPathStep
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.NilPathStep
import org.neo4j.cypher.internal.expressions.NodePathStep
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.column
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeWithProperties
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createPattern
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationship
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.delete
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.removeDynamicLabel
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.removeLabel
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setDynamicLabel
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setLabel
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setNodePropertiesFromMap
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setNodeProperty
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setRelationshipPropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.TransactionConcurrency
import org.neo4j.cypher.internal.runtime.ast.PropertiesUsingCachedProperties
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RecordingRuntimeResult
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.rewriters.TestPlanCombinationRewriter.NoRewrites
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.result.OperatorProfile
import org.neo4j.cypher.result.QueryProfile
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.schema.IndexType
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.values.virtual.NodeValue.DirectNodeValue
import org.neo4j.values.virtual.RelationshipValue.DirectRelationshipValue

abstract class ProfileDbHitsTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  val sizeHint: Int,
  costOfGetPropertyChain: Long, // the reported dbHits for getting the property chain
  val costOfPropertyToken: Long, // the reported dbHits for looking up a property token
  val costOfSetProperty: Long, // the reported dbHits for setting a property token
  costOfPropertyJumpedOverInChain: Long, // the reported dbHits for a property in the chain that needs to be traversed in order to read another property in the chain
  val costOfPropertyExists: Long, // the reported dbHits for a single checking if a property exists on a node
  val costOfProperty: Long, // the reported dbHits for a single property lookup, after getting the property chain and getting to the right position
  val costOfLabelCheck: Long, // the reported dbHits for getting label of a node
  val costOfLabelLookup: Long, // the reported dbHits for finding the id of a label
  costOfExpandGetRelCursor: Long, // the reported dbHits for obtaining a relationship cursor for expanding
  costOfExpandOneRel: Long, // the reported dbHits for expanding one relationship
  val costOfRelationshipTypeLookup: Long, // the reported dbHits for finding the id of a relationship type
  val costOfCompositeUniqueIndexCursorRow: Long, // the reported dbHits for finding one row from a composite unique index
  cartesianProductChunkSize: Long, // The size of a LHS chunk for cartesian product
  val canReuseAllScanLookup: Boolean, // operator following AllNodesScan or RelationshipScan does not need to lookup node again
  val useWritesWithProfiling: Boolean // writes with profiling count dbHits for each element of the input array and ignore when no actual write was performed e.g. there is no addLabel write when label already exists on the node
) extends RuntimeTestSuite[CONTEXT](edition, runtime, testPlanCombinationRewriterHints = Set(NoRewrites)) {

  test("HasLabel on top of AllNodesScan") {
    val cost = if (canReuseAllScanLookup) costOfLabelCheck - 1 else costOfLabelCheck
    hasLabelOnTopOfLeaf(_.allNodeScan("n"), expression = "n:Label", cost)
  }

  test("HasLabel on top of LabelScan") {
    hasLabelOnTopOfLeaf(_.nodeByLabelScan("n", "Label"), expression = "n:Label", costOfLabelCheck)
  }

  test("IS NOT NULL on top of AllNodesScan") {
    val cost = if (canReuseAllScanLookup) costOfPropertyExists - 1 else costOfPropertyExists
    hasLabelOnTopOfLeaf(_.allNodeScan("n"), expression = "n.prop IS NOT NULL", cost)
  }

  test("IS NOT NULL on top of LabelScan") {
    hasLabelOnTopOfLeaf(_.nodeByLabelScan("n", "Label"), expression = "n.prop IS NOT NULL", costOfPropertyExists)
  }

  private def hasLabelOnTopOfLeaf(
    leaf: LogicalQueryBuilder => LogicalQueryBuilder,
    expression: String,
    expressionPerRowCost: Long
  ) = {
    val nodes = givenGraph {
      nodeIndex("Label", "prop")
      nodePropertyGraph(sizeHint, { case i => Map("prop" -> i) }, "Label")
    }

    // when
    val builder = new LogicalQueryBuilder(this)
      .produceResults("n")
      .filter(expression)
    val logicalQuery = leaf(builder).build()

    // only used when Input is leaf
    val input = inputValues(nodes.map(n => Array[Any](n)): _*)

    val runtimeResult = profile(logicalQuery, runtime, input)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(1).dbHits() should be(sizeHint * expressionPerRowCost) // Filter
  }

  test("HasType on top of Input") {
    hasTypeOnTopOfLeaf(_.input(relationships = Seq("r")), hasTypePerRowCost = 1)
  }

  test("HasType on top of RelationshipTypeScan") {
    val cost = if (canReuseAllScanLookup) 0 else 1
    hasTypeOnTopOfLeaf(_.relationshipTypeScan("()-[r:R]->()"), hasTypePerRowCost = cost)
  }

  private def hasTypeOnTopOfLeaf(leaf: LogicalQueryBuilder => LogicalQueryBuilder, hasTypePerRowCost: Int) = {
    val (_, rels) = givenGraph {
      circleGraph(sizeHint, "R", 1)
    }

    // when
    val builder = new LogicalQueryBuilder(this)
      .produceResults("r")
      .filter("r:R")
    val logicalQuery = leaf(builder).build()

    // only used when Input is leaf
    val input = inputValues(rels.map(r => Array[Any](r)): _*)

    val runtimeResult = profile(logicalQuery, runtime, input)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(1).dbHits() should be(sizeHint * hasTypePerRowCost) // Filter(HasType)
  }

  test("should profile dbHits of all nodes scan") {
    givenGraph { nodeGraph(sizeHint) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val expectedDbHits = runtimeUsed match {
      case Interpreted | Slotted | Pipelined   => sizeHint + 1
      case Parallel if isParallelWithOneWorker => sizeHint + 1
      case Parallel                            => sizeHint
    }
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(1).dbHits() shouldBe expectedDbHits // all nodes scan
  }

  test("should profile dbHits of label scan") {
    givenGraph {
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
    val expectedDbHits = sizeHint + 1 + costOfLabelLookup
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(1).dbHits() shouldBe expectedDbHits // label scan
  }

  test("should profile dbHits of union label scan") {
    givenGraph {
      nodeGraph(3, "Dud")
      nodeGraph(sizeHint, "It")
      nodeGraph(3, "Decoy")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true") // here to make sure we fuse
      .unionNodeByLabelsScan("x", Seq("Dud", "Decoy"), IndexOrderNone)
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(2).dbHits() should (be(
      6 + 2 /*label scan*/ + 2 * costOfLabelLookup
    )) // union label scan
  }

  test("should profile dbHits of intersection label scan") {
    givenGraph {
      nodeGraph(3, "Dud", "Decoy")
      nodeGraph(sizeHint, "It")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true") // here to make sure we fuse
      .intersectionNodeByLabelsScan("x", Seq("Dud", "Decoy"), IndexOrderNone)
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(2).dbHits() should (be(
      6 + 2 /*label scan*/ + 2 * costOfLabelLookup
    )) // union label scan
  }

  test("should profile dbHits of subtraction label scan") {
    givenGraph {
      nodeGraph(3, "A")
      nodeGraph(sizeHint, "B")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true") // here to make sure we fuse
      .subtractionNodeByLabelsScan("x", "A", "B", IndexOrderNone)
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(2).dbHits() should equal(6L +- 4L)
  }

  test("should profile dbHits of node index seek with range predicate") {
    givenGraph {
      nodeIndex("Language", "difficulty")
      nodePropertyGraph(
        sizeHint,
        {
          case i if i % 10 == 0 => Map("difficulty" -> i)
        },
        "Language"
      )
    }

    // when
    val seekProfile = profileIndexSeek(s"x:Language(difficulty >= ${sizeHint / 2})")
    // then
    seekProfile.operatorProfile(2).dbHits() shouldBe (sizeHint / 10 / 2 + 1) // node index seek
  }

  test("should profile dbHits of node index seek with IN predicate") {
    val nodes = givenGraph {
      nodeIndex("Language", "difficulty")
      nodePropertyGraph(
        sizeHint,
        {
          case i if i % 10 == 0 => Map("difficulty" -> i)
        },
        "Language"
      )
    }

    val difficulties =
      nodes.filter(_.hasProperty("difficulty")).map(_.getProperty("difficulty").asInstanceOf[Int].longValue())

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator("x:Language(difficulty IN ???)", paramExpr = Some(listOfInt(difficulties.toSeq: _*)))
      .build()

    val result = profile(logicalQuery, runtime)
    consume(result)

    val seekProfile = result.runtimeResult.queryProfile()

    // then
    seekProfile.operatorProfile(
      1
    ).dbHits() shouldBe (difficulties.size /*row of seek*/ + 1 * difficulties.size /*last next per seek*/ ) // node index seek
  }

  test("should profile dbHits of node index seek with IN predicate on composite index") {
    givenGraph {
      nodeIndex("Language", "difficulty", "usefulness")
      nodePropertyGraph(
        sizeHint,
        {
          case i if i % 10 == 0 => Map("difficulty" -> i, "usefulness" -> i)
        },
        "Language"
      )
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
    seekProfile.operatorProfile(1).dbHits() shouldBe (expectedRowCount + 1 /*DB Hits are incremented per next() call, even though last call will return false*/ )
  }

  test("should profile dbHits of node index scan") {
    givenGraph {
      nodeIndex("Language", "difficulty")
      nodePropertyGraph(
        sizeHint,
        {
          case i if i % 10 == 0 => Map("difficulty" -> i)
        },
        "Language"
      )
    }

    // when
    val scanProfile = profileIndexSeek("x:Language(difficulty)")
    // then
    scanProfile.operatorProfile(2).dbHits() shouldBe (sizeHint / 10 + 1) // node index scan
  }

  test("should profile dbHits of node index contains") {
    givenGraph {
      nodeIndex(IndexType.TEXT, "Language", "difficulty")
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("difficulty" -> s"x${i % 2}")
        },
        "Language"
      )
    }

    // when
    val seekProfile = profileIndexSeek(s"x:Language(difficulty CONTAINS '1')", IndexType.TEXT)
    // then
    seekProfile.operatorProfile(2).dbHits() shouldBe (sizeHint / 2 + 1) // node index contains
  }

  private def profileIndexSeek(indexSeekString: String, indexType: IndexType = IndexType.RANGE): QueryProfile = {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true") // here to make sure we fuse
      .nodeIndexOperator(indexSeekString, indexType = indexType)
      .build()

    val result = profile(logicalQuery, runtime)
    consume(result)

    result.runtimeResult.queryProfile()
  }

  test("should profile dbHits of directed all relationships scan") {
    // given
    givenGraph { circleGraph(sizeHint) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .allRelationshipsScan("(x)-[r]->(y)")
      .build()

    val result = profile(logicalQuery, runtime)
    consume(result)

    // then
    val expectedDbHits: Int = sizeHint

    result.runtimeResult.queryProfile().operatorProfile(1).dbHits() shouldBe expectedDbHits
  }

  test("should profile dbHits of undirected all relationships scan") {
    // given
    givenGraph { circleGraph(sizeHint) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .allRelationshipsScan("(x)-[r]-(y)")
      .build()

    val result = profile(logicalQuery, runtime)
    consume(result)

    // then
    result.runtimeResult.queryProfile().operatorProfile(1).dbHits() shouldBe sizeHint
  }

  test("should profile dbHits of directed relationship type scan") {
    // given
    givenGraph { circleGraph(sizeHint) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .relationshipTypeScan("(x)-[r:R]->(y)")
      .build()

    val result = profile(logicalQuery, runtime)
    consume(result)

    // then
    val expectedDbHits: Int = runtimeUsed match {
      case Slotted | Interpreted => sizeHint + 1 + 1 /*costOfRelationshipTypeLookup*/
      case _                     => sizeHint + 1
    }

    result.runtimeResult.queryProfile().operatorProfile(1).dbHits() shouldBe expectedDbHits
  }

  test("should profile dbHits of undirected relationship type scan") {
    // given
    givenGraph { circleGraph(sizeHint) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .relationshipTypeScan("(x)-[r:R]-(y)")
      .build()

    val result = profile(logicalQuery, runtime)
    consume(result)

    // then
    val expectedDbHits: Int = runtimeUsed match {
      case Slotted | Interpreted => sizeHint + 1 + 1 /*costOfRelationshipTypeLookup*/
      case _                     => sizeHint + 1
    }
    result.runtimeResult.queryProfile().operatorProfile(1).dbHits() shouldBe expectedDbHits
  }

  test("should profile dbHits of directed relationship index exact seek") {
    // given
    givenGraph {
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
    val expectedDbHits: Int = sizeHint / 10 + 1
    result.runtimeResult.queryProfile().operatorProfile(1).dbHits() should be(expectedDbHits)
  }

  test("should profile dbHits of directed relationship index range seek") {
    // given
    givenGraph {
      relationshipIndex("R", "difficulty")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => r.setProperty("difficulty", i)
        case _                     =>
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .relationshipIndexOperator(s"(x)-[r:R(difficulty >= ${sizeHint / 2})]->(y)")
      .build()

    val result = profile(logicalQuery, runtime)
    consume(result)
    val expectedDbHits: Int = sizeHint / 10 / 2 + 1
    result.runtimeResult.queryProfile().operatorProfile(1).dbHits() should be(expectedDbHits)
  }

  test("should profile dbHits of directed relationship multiple index exact seek") {
    // given
    givenGraph {
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

    result.runtimeResult.queryProfile().operatorProfile(1).dbHits() should be(sizeHint / 5 + 2)
  }

  test("should profile dbHits of undirected relationship index exact seek") {
    // given
    givenGraph {
      relationshipIndex("R", "difficulty")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("difficulty", i % 10)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true") // here to make sure we fuse
      .relationshipIndexOperator(s"(x)-[r:R(difficulty = 3)]-(y)")
      .build()

    val result = profile(logicalQuery, runtime)
    consume(result)

    result.runtimeResult.queryProfile().operatorProfile(2).dbHits() should be(sizeHint / 10 + 1)
  }

  test("should profile dbHits of undirected relationship range index seek") {
    // given
    givenGraph {
      relationshipIndex("R", "difficulty")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => r.setProperty("difficulty", i)
        case _                     =>
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true") // here to make sure we fuse
      .relationshipIndexOperator(s"(x)-[r:R(difficulty >= ${sizeHint / 2})]-(y)")
      .build()

    val result = profile(logicalQuery, runtime)
    consume(result)

    result.runtimeResult.queryProfile().operatorProfile(2).dbHits() should be(sizeHint / 10 / 2 + 1)
  }

  test("should profile dbHits of undirected relationship multiple index exact seek") {
    // given
    givenGraph {
      relationshipIndex("R", "difficulty")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("difficulty", i % 10)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true") // here to make sure we fuse
      .relationshipIndexOperator(s"(x)-[r:R(difficulty = 3 OR 4)]-(y)")
      .build()

    val result = profile(logicalQuery, runtime)
    consume(result)

    result.runtimeResult.queryProfile().operatorProfile(2).dbHits() should (be(sizeHint / 5 + 2))
  }

  test("should profile dbHits of directed relationship index scan") {
    givenGraph {
      relationshipIndex("R", "difficulty")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => r.setProperty("difficulty", i)
        case _                     =>
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .relationshipIndexOperator(s"(x)-[r:R(difficulty)]->(y)")
      .build()

    val result = profile(logicalQuery, runtime)
    consume(result)

    // then
    val expectedDbHits = sizeHint / 10 + 1
    result.runtimeResult.queryProfile().operatorProfile(1).dbHits() shouldBe expectedDbHits
  }

  test("should profile dbHits of undirected relationship index scan") {
    givenGraph {
      relationshipIndex("R", "difficulty")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => r.setProperty("difficulty", i)
        case _                     =>
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true") // here to make sure we fuse
      .relationshipIndexOperator(s"(x)-[r:R(difficulty)]-(y)")
      .build()

    val result = profile(logicalQuery, runtime)
    consume(result)

    // then
    result.runtimeResult.queryProfile().operatorProfile(2).dbHits() shouldBe sizeHint / 10 + 1
  }

  test("should profile dbHits of node by id") {
    // given
    val nodes = givenGraph { nodeGraph(17) }

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
    val (_, rels) = givenGraph { circleGraph(17) }

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
    givenGraph { circleGraph(sizeHint) }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .expandInto("(x)-[r]->(y)")
      .expandAll("(x)-->(y)")
      .allNodeScan("x")
      .build()

    val expandConstantCost = if (hasFastRelationshipTo) 0 else 2
    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)
    // then
    val expectedExpandIntoDbHits = runtimeUsed match {
      case Interpreted | Slotted | Pipelined =>
        be(sizeHint * (expandConstantCost + (costOfExpandGetRelCursor + costOfExpandOneRel)))
      // caching results vary for parallel execution
      case Parallel => be <= sizeHint * (expandConstantCost + (costOfExpandGetRelCursor + costOfExpandOneRel))
    }

    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(1).dbHits() should expectedExpandIntoDbHits
    // No assertion on the expand because db hits can vary, given that the nodes have 2 relationships.
  }

  test("should profile dbhits with optional expand all") {
    // given
    val extraNodes = 20
    givenGraph {
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
    val expectedOptionalExpandAllDbHits = sizeHint * (costOfExpandGetRelCursor + costOfExpandOneRel) + extraNodes
    val expectedNodeByLabelScanDbHits = sizeHint + extraNodes + 1 + costOfLabelLookup
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(1).dbHits() shouldBe expectedOptionalExpandAllDbHits // optional expand all
    queryProfile.operatorProfile(2).dbHits() shouldBe expectedNodeByLabelScanDbHits // node by label scan
  }

  test("should profile dbhits with optional expand into") {
    // given
    val n = Math.sqrt(sizeHint).toInt
    val extraNodes = 20
    givenGraph {
      val xs = nodePropertyGraph(
        n,
        {
          case i: Int => Map("prop" -> i)
        },
        "X"
      )
      val ys = nodePropertyGraph(
        n,
        {
          case i: Int => Map("prop" -> i)
        },
        "Y"
      )

      connect(xs ++ ys, xs.indices.map(i => (i, i + xs.length, "R")))

      nodePropertyGraph(
        extraNodes,
        {
          case i: Int => Map("prop" -> (i + xs.length))
        },
        "X"
      )
      nodePropertyGraph(
        extraNodes,
        {
          case i: Int => Map("prop" -> (i + ys.length))
        },
        "Y"
      )
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
    val expectedLabelScanRHS = runtimeUsed match {
      case Interpreted | Slotted | Pipelined =>
        be((n + extraNodes) * (n + extraNodes + 1) + costOfLabelLookup)
      case Parallel => be >= (n + extraNodes) + costOfLabelLookup and
          be <= (n + extraNodes) * (n + extraNodes + 1) + costOfLabelLookup
    }
    val expectedLabelScanLHS = n + extraNodes + 1 + costOfLabelLookup
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    val expandConstantCost = if (hasFastRelationshipTo) 1 else 2
    queryProfile.operatorProfile(
      1
    ).dbHits() shouldBe ((n + extraNodes) * expandConstantCost + n * costOfExpandOneRel) // optional expand into
    queryProfile.operatorProfile(2).dbHits() shouldBe 0 // apply
    queryProfile.operatorProfile(
      3
    ).dbHits() shouldBe ((n + extraNodes) * (n + extraNodes) * 2 * (costOfGetPropertyChain + costOfProperty)) // filter (reads 2 properties))
    queryProfile.operatorProfile(4).dbHits() should expectedLabelScanRHS // label scan Y
    queryProfile.operatorProfile(5).dbHits() shouldBe expectedLabelScanLHS // // label scan X
  }

  test("should profile dbHits with node hash join") {
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
    val fusedCostOfGetPropertyChain = if (canReuseAllScanLookup) 0 else costOfGetPropertyChain
    val expectedFilterDbHits = sizeHint * (fusedCostOfGetPropertyChain + costOfProperty)
    val expectedAllNodesScanDbHits = runtimeUsed match {
      case Interpreted | Slotted | Pipelined                             => sizeHint + 1
      case Parallel if isParallelWithOneWorker && !canReuseAllScanLookup => sizeHint + 1
      case Parallel                                                      => sizeHint
    }

    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(1).dbHits() shouldBe 0 // node hash join
    queryProfile.operatorProfile(2).dbHits() shouldBe expectedFilterDbHits // filter
    queryProfile.operatorProfile(3).dbHits() shouldBe expectedAllNodesScanDbHits // all node scan
    queryProfile.operatorProfile(4).dbHits() shouldBe expectedFilterDbHits // filter
    queryProfile.operatorProfile(5).dbHits() shouldBe expectedAllNodesScanDbHits // all node scan
  }

  test("should profile dbHits with value hash join") {
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
      .|.allNodeScan("y")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val expectedAllNodeScanDbHits = runtimeUsed match {
      case Interpreted | Slotted | Pipelined   => sizeHint + 1
      case Parallel if isParallelWithOneWorker => sizeHint + 1
      case Parallel                            => sizeHint
    }

    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(1).dbHits() shouldBe 2 * sizeHint // value hash join
    queryProfile.operatorProfile(2).dbHits() shouldBe expectedAllNodeScanDbHits // all node scan
    queryProfile.operatorProfile(3).dbHits() shouldBe expectedAllNodeScanDbHits // all node scan
  }

  test("should profile dbHits of cached properties") {
    givenGraph {
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
    val fusedCostOfGetPropertyChain = if (canReuseAllScanLookup) 0 else costOfGetPropertyChain
    queryProfile.operatorProfile(1).dbHits() shouldBe 0 // projection
    queryProfile.operatorProfile(
      2
    ).dbHits() shouldBe (sizeHint * (fusedCostOfGetPropertyChain + costOfProperty)) // cacheProperties
  }

  test("should profile dbHits of many cached properties") {
    givenGraph {
      nodePropertyGraph(sizeHint, { case i => Map("p1" -> i, "p2" -> i) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .projection("cache[n.p1] AS p1", "cache[n.p2] AS p2")
      .cacheProperties("cache[n.p1]", "cache[n.p2]")
      .allNodeScan("n")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(1).dbHits() shouldBe 0 // projection
    val costOfPropertyReads = (if (canReuseAllScanLookup) 0 else costOfGetPropertyChain) + 2 * costOfProperty
    queryProfile.operatorProfile(2).dbHits() shouldBe (sizeHint * costOfPropertyReads) // cacheProperties
  }

  test("should profile dbHits with apply") {
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
    val fusedCostOfGetPropertyChain = if (canReuseAllScanLookup) 0 else costOfGetPropertyChain
    val expectedSecondFilterDbHits = (size / 2 * size) * (costOfGetPropertyChain + costOfProperty)
    val expectedFirstFilterDbHits = size * size * (fusedCostOfGetPropertyChain + costOfProperty)
    val expectedAllNodeScanRHS = runtimeUsed match {
      case Interpreted | Slotted | Pipelined                             => be(size * (size + 1))
      case Parallel if isParallelWithOneWorker && !canReuseAllScanLookup => be(size * (size + 1))
      case Parallel                                                      => be >= size and be <= size * size
    }
    val expectedAllNodeScanLHS = runtimeUsed match {
      case Interpreted | Slotted | Pipelined   => size + 1
      case Parallel if isParallelWithOneWorker => size + 1
      case Parallel                            => size
    }
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(1).dbHits() shouldBe expectedSecondFilterDbHits // filter
    queryProfile.operatorProfile(2).dbHits() shouldBe 0 // apply
    queryProfile.operatorProfile(3).dbHits() shouldBe expectedFirstFilterDbHits // filter
    queryProfile.operatorProfile(4).dbHits().toInt should expectedAllNodeScanRHS // all node scan Y
    queryProfile.operatorProfile(5).dbHits() shouldBe expectedAllNodeScanLHS // all node scan X
  }

  test("should profile dbHits of expression reads") {
    givenGraph { nodePropertyGraph(sizeHint, { case i => Map("list" -> Array(i, i + 1), "prop" -> i) }) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("exists")
      .projection("[p IN x.list | x.prop = p] AS exists")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val fusedCostOfGetPropertyChain = if (canReuseAllScanLookup) 0 else costOfGetPropertyChain
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(1).dbHits() should be(
      sizeHint * (2 * (fusedCostOfGetPropertyChain + costOfProperty) + (fusedCostOfGetPropertyChain + costOfProperty))
    )
  }

  test("should profile dbHits of aggregation") {
    givenGraph { nodePropertyGraph(sizeHint, { case i => Map("prop" -> i) }) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("avg")
      .aggregation(Seq.empty, Seq("avg(x.prop) AS avg"))
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val fusedCostOfGetPropertyChain = if (canReuseAllScanLookup) 0 else costOfGetPropertyChain
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(1).dbHits() shouldBe sizeHint * (fusedCostOfGetPropertyChain + costOfProperty)
  }

  test("should profile dbHits of aggregation with grouping") {
    // given
    val aggregationGroups = sizeHint / 2
    givenGraph { nodePropertyGraph(sizeHint, { case i => Map("group" -> i % aggregationGroups, "prop" -> i) }) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("group", "avg")
      .aggregation(Seq("x.group AS group"), Seq("avg(x.prop) AS avg"))
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val fusedCostOfGetPropertyChain = if (canReuseAllScanLookup) 0 else costOfGetPropertyChain
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(1).dbHits() shouldBe (sizeHint *
      ((fusedCostOfGetPropertyChain + costOfProperty) // first prop in chain
        + (fusedCostOfGetPropertyChain + costOfProperty))) // second prop in chain
  }

  test("should profile dbHits of cartesian product") {
    // given
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
    val numberOfChunks = Math.ceil(size / cartesianProductChunkSize.toDouble).toInt
    val expectedAllNodeScanRHS = runtimeUsed match {
      case Interpreted | Slotted | Pipelined   => be(numberOfChunks * (size + 1))
      case Parallel if isParallelWithOneWorker => be(numberOfChunks * (size + 1))
      case Parallel                            => be >= numberOfChunks * size and be <= size * size
    }
    val expectedAllNodeScanLHS = runtimeUsed match {
      case Interpreted | Slotted | Pipelined   => size + 1
      case Parallel if isParallelWithOneWorker => size + 1
      case Parallel                            => size
    }
    result.runtimeResult.queryProfile().operatorProfile(1).dbHits() shouldBe 0 // cartesian product
    result.runtimeResult.queryProfile().operatorProfile(
      2
    ).dbHits().toInt should expectedAllNodeScanRHS // all node scan b
    result.runtimeResult.queryProfile().operatorProfile(3).dbHits() shouldBe expectedAllNodeScanLHS // all node scan a
  }

  test("should profile dbHits of skip") {
    givenGraph {
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
    val expectedAllNodeScanDbHits = runtimeUsed match {
      case Interpreted | Slotted | Pipelined   => sizeHint + 1
      case Parallel if isParallelWithOneWorker => sizeHint + 1
      case Parallel                            => sizeHint
    }
    val propCost = if (canFuseOverPipelines) 0 else costOfGetPropertyChain
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).dbHits() shouldBe 0 // produce result
    queryProfile.operatorProfile(1).dbHits() shouldBe 1 + propCost // projection
    queryProfile.operatorProfile(2).dbHits() shouldBe 0 // skip
    queryProfile.operatorProfile(3).dbHits() shouldBe expectedAllNodeScanDbHits // allNodesScan
  }

  test("should profile dbHits of union") {
    // given
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
    val expectedAllNodeScanRHS = runtimeUsed match {
      case Interpreted | Slotted | Pipelined   => size + 1
      case Parallel if isParallelWithOneWorker => size + 1
      case Parallel                            => size
    }
    val expectedAllNodeScanLHS = runtimeUsed match {
      case Interpreted | Slotted | Pipelined   => size + 1
      case Parallel if isParallelWithOneWorker => size + 1
      case Parallel                            => size
    }
    result.runtimeResult.queryProfile().operatorProfile(1).dbHits() shouldBe 0 // union
    result.runtimeResult.queryProfile().operatorProfile(2).dbHits() shouldBe expectedAllNodeScanRHS // all node scan
    result.runtimeResult.queryProfile().operatorProfile(3).dbHits() shouldBe expectedAllNodeScanLHS // all node scan
  }

  test("should profile dbHits with project endpoints") {
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
    queryProfile.operatorProfile(1).dbHits() should (be(aRels.size) or be(0)) // project endpoints
    queryProfile.operatorProfile(2).dbHits() shouldBe 0 // input
  }

  test("should profile dbHits of populating nodes in produceresults") {
    givenGraph { nodePropertyGraph(sizeHint, { case i => Map("p" -> i, "q" -> -i) }, "somelabel") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()

    queryProfile.operatorProfile(0).dbHits() should be(
      sizeHint * (1 /* read node */ + 2 * costOfProperty)
    ) // produceresults
  }

  test("should not profile dbHits of populating nodes in produceresults if properties are cached") {
    givenGraph { nodePropertyGraph(sizeHint, { case i => Map("p" -> i, "q" -> -i) }, "somelabel") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(column("x", "cacheN[x.p]"))
      .cacheProperties("cacheN[x.p]")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    val consumed = consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()

    queryProfile.operatorProfile(0).dbHits() should be(
      sizeHint * (1 /* read node */ + /* only read x.q */ 1 * costOfProperty) + costOfPropertyToken
    ) // produceresults
    consumed.foreach {
      case c => c.head match {
          case n: DirectNodeValue =>
            val props = n.properties()
            props.containsKey("p") shouldBe true
            props.containsKey("q") shouldBe true
        }
      case _ => fail()
    }
  }

  test("should profile dbHits of populating relationships in produceresults") {
    // given
    givenGraph {
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

    // then
    result.runtimeResult.queryProfile().operatorProfile(0).dbHits() should be(
      sizeHint * (1 + 2 * costOfProperty)
    ) // produceresults
  }

  test("should not profile dbHits of populating relationships in produceresults if properties are cached") {
    // given
    givenGraph {
      val nodes = nodeGraph(sizeHint)
      connectWithProperties(nodes, nodes.indices.map(i => (i, (i + 1) % nodes.length, "Rel", Map("p" -> i, "q" -> -i))))
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(column("r", "cacheR[r.p]"))
      .cacheProperties("cacheR[r.p]")
      .expandAll("(x)-[r]->(y)")
      .allNodeScan("x")
      .build()

    val result = profile(logicalQuery, runtime)
    val consumed = consume(result)

    // then
    result.runtimeResult.queryProfile().operatorProfile(0).dbHits() should be(
      sizeHint * (1 + 1 * costOfProperty) + costOfPropertyToken
    ) // produceresults
    consumed.foreach {
      case c => c.head match {
          case r: DirectRelationshipValue =>
            val props = r.properties()
            props.containsKey("p") shouldBe true
            props.containsKey("q") shouldBe true
        }
      case _ => fail()
    }
  }

  test("should profile dbHits of populating collections in produceresults") {
    // given
    givenGraph {
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
    result.runtimeResult.queryProfile().operatorProfile(0).dbHits() should be(
      2 * (sizeHint * (1 /* read node */ + 2 * costOfProperty))
    ) // produceresults
  }

  test("should profile dbhits with bfs pruning var-expand") {
    // given
    givenGraph {
      val x = tx.createNode(Label.label("START"))
      val relType = RelationshipType.withName("R")
      x.createRelationshipTo(tx.createNode(), relType)
      x.createRelationshipTo(tx.createNode(), relType)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .distinct("y AS y")
      .bfsPruningVarExpand("(x)-[*1..1]->(y)")
      .nodeByLabelScan("x", "START")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    runtimeResult.runtimeResult.queryProfile().operatorProfile(2).dbHits() should be(3)
  }

  test("should use cached properties in projection function") {
    givenGraph { nodePropertyGraph(sizeHint, { case i => Map("p" -> i, "q" -> -i) }, "somelabel") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(column("x", "cacheN[x.p]"))
      .projection(Map("res" -> PropertiesUsingCachedProperties(
        varFor("x"),
        Set(CachedProperty(varFor("x"), varFor("x"), propName("p"), NODE_TYPE)(pos))
      )))
      .cacheProperties("cacheN[x.p]")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    val consumed = consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()

    queryProfile.operatorProfile(1).dbHits() should be(
      sizeHint * (1 /* read node */ + /* only read x.q */ 1 * costOfProperty)
    ) // produceresults
  }

  def hasFastRelationshipTo: Boolean = {
    val ktx = runtimeTestSupport.tx.kernelTransaction
    val cursor = ktx.ambientNodeCursor
    ktx.dataRead().allNodesScan(cursor) // Needs to position cursor to actually check fastRelationshipTo
    cursor.supportsFastRelationshipsTo
  }
}

trait UniqueIndexDbHitsTestBase[CONTEXT <: RuntimeContext] {
  self: ProfileDbHitsTestBase[CONTEXT] =>

  test("should profile dbHits of node index seek with IN predicate on locking unique index") {
    val nodes = givenGraph {
      uniqueNodeIndex("Language", "difficulty")
      nodePropertyGraph(
        sizeHint,
        {
          case i if i % 10 == 0 => Map("difficulty" -> i)
        },
        "Language"
      )
    }

    val difficulties =
      nodes.filter(_.hasProperty("difficulty")).map(_.getProperty("difficulty").asInstanceOf[Int].longValue())

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator(
        "x:Language(difficulty IN ???)",
        paramExpr = Some(listOfInt(difficulties.toSeq: _*)),
        unique = true
      )
      .build(readOnly = false)

    val result = profile(logicalQuery, runtime)
    consume(result)

    val seekProfile = result.runtimeResult.queryProfile()

    // then
    val expectedRowCount =
      difficulties.size * costOfCompositeUniqueIndexCursorRow /*DB Hits are incremented per next() call, even though last call will return false*/
    seekProfile.operatorProfile(1).dbHits() shouldBe expectedRowCount // locking unique node index seek
  }

  test("should profile dbHits of node index seek with node key") {
    givenGraph {
      nodeKey("Language", "difficulty", "usefulness")
      nodePropertyGraph(
        sizeHint,
        {
          case i if i % 10 == 0 => Map("difficulty" -> i, "usefulness" -> i)
          case i                => Map("difficulty" -> -i, "usefulness" -> -i)
        },
        "Language"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator(s"x:Language(difficulty = 0, usefulness = 0)", unique = true, indexType = IndexType.RANGE)
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

  test("should profile dbHits of directed relationship index unique seek") {
    // given
    givenGraph {
      uniqueRelationshipIndex("R", "difficulty")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => r.setProperty("difficulty", i)
        case _                     => // do nothing
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .relationshipIndexOperator(s"(x)-[r:R(difficulty = 3)]->(y)", unique = true)
      .build()

    val result = profile(logicalQuery, runtime)
    consume(result)
    result.runtimeResult.queryProfile().operatorProfile(1).dbHits() should be(1)
  }

  test("should profile dbHits of undirected relationship multiple index unique seek") {
    // given
    givenGraph {
      uniqueRelationshipIndex("R", "difficulty")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 10 == 0 => r.setProperty("difficulty", i)
        case _                     => // do nothing
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .relationshipIndexOperator(s"(x)-[r:R(difficulty = 3 OR 4)]-(y)", unique = true)
      .build()

    val result = profile(logicalQuery, runtime)
    consume(result)

    result.runtimeResult.queryProfile().operatorProfile(1).dbHits() should be(2)
  }

}

trait ProcedureCallDbHitsTestBase[CONTEXT <: RuntimeContext] {
  self: ProfileDbHitsTestBase[CONTEXT] =>

  test("should profile dbHits of procedure call") {
    // given
    givenGraph { nodeGraph(sizeHint) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .procedureCall("db.awaitIndexes()")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val expectedAllNodeScanDbHits = runtimeUsed match {
      case Pipelined if canFuseOverPipelines   => sizeHint
      case Interpreted | Slotted | Pipelined   => sizeHint + 1
      case Parallel if isParallelWithOneWorker => sizeHint + 1
      case Parallel                            => sizeHint
    }
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(1).dbHits() shouldBe OperatorProfile.NO_DATA // procedure call
    queryProfile.operatorProfile(2).dbHits() shouldBe expectedAllNodeScanDbHits // all node scan
  }
}

trait TransactionForeachDbHitsTestBase[CONTEXT <: RuntimeContext] {
  self: ProfileDbHitsTestBase[CONTEXT] =>

  test("should profile dbHits of populating nodes inside transactionForeach") {

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

    val queryProfile = runtimeResult.runtimeResult.queryProfile()

    val numberOfAllNodeScans = 2
    val numberOfNodesFoundByAllNodeScans = (1 + 2) * sizeHint
    // allNodeScan is the 5th operator in the plan
    val allNodeScanPlanId = 4
    val expectedDbHits = runtimeUsed match {
      case Interpreted | Slotted => numberOfNodesFoundByAllNodeScans + numberOfAllNodeScans
      case Pipelined | Parallel  => numberOfNodesFoundByAllNodeScans
    }
    queryProfile.operatorProfile(allNodeScanPlanId).dbHits() shouldBe expectedDbHits
  }

  Seq(
    TransactionConcurrency.Serial,
    TransactionConcurrency.Concurrent(2),
    TransactionConcurrency.Concurrent(None)
  ).foreach { concurrency =>
    test(s"should profile dbHits of populating nodes inside transactionForeach concurrency=$concurrency") {

      val nNodes = sizeHint / 10
      val nRows = 100
      val batchSize = 1

      givenWithTransactionType(
        nodeGraph(nNodes, "A"),
        KernelTransaction.Type.IMPLICIT
      )

      val query = new LogicalQueryBuilder(this)
        .produceResults("x")
        .transactionForeach(concurrency = concurrency, batchSize = batchSize)
        .|.emptyResult()
        .|.create(createNode("n"))
        .|.filter("m:A")
        .|.nodeByLabelScan("m", "A")
        .unwind(s"range(1,$nRows) AS x")
        .argument()
        .build(readOnly = false)

      // then
      val runtimeResult: RecordingRuntimeResult = profile(query, runtime)
      consume(runtimeResult)

      val queryProfile = runtimeResult.runtimeResult.queryProfile()

      // NOTE: With concurrency LazyLabel makes dbHits flaky on nodeByLabelScan
      val createPlanId = 3
      val filterPlanId = 4
      val expectedDbHits = nNodes * nRows
      queryProfile.operatorProfile(createPlanId).dbHits() shouldBe expectedDbHits
      queryProfile.operatorProfile(filterPlanId).dbHits() shouldBe expectedDbHits
    }
  }
}

trait NestedPlanDbHitsTestBase[CONTEXT <: RuntimeContext] {
  self: ProfileDbHitsTestBase[CONTEXT] =>

  test("should profile dbHits of nested plan expression") {
    val size = Math.sqrt(sizeHint).toInt
    givenGraph { nodeGraph(size) }

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
    val queryProfile = result.runtimeResult.queryProfile()
    queryProfile.operatorProfile(1).dbHits() shouldBe size * (size + 1) // projection w. nested plan expression
  }
}

trait WriteOperatorsDbHitsTestBase[CONTEXT <: RuntimeContext] {
  self: ProfileDbHitsTestBase[CONTEXT] =>

  test("should profile rows of set property correctly") {
    // given
    givenGraph {
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

    val expectedDbHits =
      if (useWritesWithProfiling & canFuseOverPipelines) {
        val propertyTokenDbHits = 1 * sizeHint
        val writeNodePropertyDbHits = 1 * sizeHint
        propertyTokenDbHits + writeNodePropertyDbHits
      } else {
        3 * costOfPropertyToken /*LazyPropertyKey: look up - create property token - look up*/ + sizeHint * costOfProperty
      }

    setPropertyProfile.rows() shouldBe sizeHint
    setPropertyProfile.dbHits() shouldBe expectedDbHits
    produceResultProfile.rows() shouldBe sizeHint
    produceResultProfile.dbHits() shouldBe sizeHint * (costOfProperty + costOfProperty)
  }

  test("should profile rows of set dynamic property correctly") {
    // given
    givenGraph {
      bipartiteGraph(sizeHint, "A", "B", "R")
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .setDynamicProperty("x", "'prop'", "1")
      .nodeByLabelScan("x", "A")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    val produceResultProfile = queryProfile.operatorProfile(0)
    val setPropertyProfile = queryProfile.operatorProfile(1)

    val expectedDbHits =
      if (useWritesWithProfiling & canFuseOverPipelines) {
        val propertyTokenDbHits = 1 * sizeHint
        val writeNodePropertyDbHits = 1 * sizeHint
        propertyTokenDbHits + writeNodePropertyDbHits
      } else {
        costOfPropertyToken /*create property token*/ + sizeHint * costOfProperty
      }

    setPropertyProfile.rows() shouldBe sizeHint
    setPropertyProfile.dbHits() shouldBe expectedDbHits
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
    chainGraphs(chainCount, types: _*)

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
    chainGraphs(chainCount, types: _*)
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
    chainGraphs(chainCount, types: _*)

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
          next = NilPathStep()(pos)
        )(pos)
      )(pos)
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
      .projection(Map("p" -> PathExpression(NodePathStep(varFor("n"), NilPathStep()(pos))(pos))(InputPosition.NONE)))
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

  test("should profile db hits on create nodes with labels and properties") {
    val createCount = 9
    val labels = Seq("A", "B", "C")
    val properties = Map("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4, "e" -> 5)
    // given empty db

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .create(createNodeWithProperties("n", labels, propertiesString(properties)))
      .unwind(s"${Range(0, createCount).mkString("[", ",", "]")} AS i")
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
    val setPropertyHits = createCount
    createProfile.dbHits() shouldBe (createNodesHits + createLabelsHits + createPropKeyHits + setPropertyHits)
  }

  test("should profile rows and dbhits of foreach + setNodeProperty correctly") {
    // given
    givenGraph {
      bipartiteGraph(sizeHint, "A", "B", "R")
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .foreach("i", "[1, 2, 3]", Seq(setNodeProperty("x", "prop", "i")))
      .nodeByLabelScan("x", "A")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    val produceResultProfile = queryProfile.operatorProfile(0)
    val foreachProfile = queryProfile.operatorProfile(1)

    val expectedDbHits =
      if (useWritesWithProfiling & canFuseOverPipelines) {
        val propertyTokenDbHits = 3 * sizeHint
        val writeNodePropertyDbHits = 3 * sizeHint
        propertyTokenDbHits + writeNodePropertyDbHits
      } else {
        3 * costOfPropertyToken /*LazyPropertyKey: look up - create property token - look up*/ + 3 * sizeHint * costOfProperty
      }

    foreachProfile.rows() shouldBe sizeHint
    foreachProfile.dbHits() shouldBe expectedDbHits
    produceResultProfile.rows() shouldBe sizeHint
    produceResultProfile.dbHits() shouldBe sizeHint * (costOfProperty + costOfProperty)
  }

  test("should profile rows and dbhits of foreach + set node properties from map") {
    // given
    givenGraph {
      nodeGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .foreach("i", "[1, 2, 3]", Seq(setNodePropertiesFromMap("n", "{prop: 42, foo: i}", removeOtherProps = false)))
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)
    val setPropertiesFromMapProfile = runtimeResult.runtimeResult.queryProfile().operatorProfile(1)

    val expectedDbHits =
      if (useWritesWithProfiling & canFuseOverPipelines) {
        val propertyTokenDbHits = sizeHint * 3
        val writeNodePropertyDbHits = sizeHint * 3 * 2
        propertyTokenDbHits + writeNodePropertyDbHits
      } else if (useWritesWithProfiling) {
        val setPropertyDbHits = sizeHint * 3 * 2
        val tokenDbHits = sizeHint * 2 * 3 * costOfPropertyToken
        setPropertyDbHits + tokenDbHits
      } else {
        val setPropertyDbHits = sizeHint * 3
        val tokenDbHits = sizeHint * 2 * 3 * costOfPropertyToken
        setPropertyDbHits + tokenDbHits
      }

    setPropertiesFromMapProfile.dbHits() shouldBe expectedDbHits
  }

  test("should profile rows and dbhits of foreach + set relationship properties from map") {
    // given
    val relationship = givenGraph {
      val (_, rels) = circleGraph(sizeHint)
      rels
    }

    val relationshipCount = relationship.size

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .foreach(
        "i",
        "[1,2,3]",
        Seq(setRelationshipPropertiesFromMap("r", "{prop: 42, foo: i}", removeOtherProps = false))
      )
      .expandAll("(n)-[r]->()")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)
    val setPropertiesFromMapProfile = runtimeResult.runtimeResult.queryProfile().operatorProfile(1)

    val expectedDbHits =
      if (useWritesWithProfiling & canFuseOverPipelines) {
        val setPropertyDbHits = relationshipCount * 3
        val tokenDbHits = relationshipCount * 3 * 2
        setPropertyDbHits + tokenDbHits
      } else if (useWritesWithProfiling) {
        val setPropertyDbHits = relationshipCount * 3 * 2
        val tokenDbHits = relationshipCount * 3 * 2 * costOfPropertyToken
        setPropertyDbHits + tokenDbHits
      } else {
        val setPropertyDbHits = relationshipCount * 3
        val tokenDbHits = relationshipCount * 3 * 2 * costOfPropertyToken
        setPropertyDbHits + tokenDbHits
      }

    setPropertiesFromMapProfile.dbHits() shouldBe expectedDbHits
  }

  test("should profile rows and dbhits of foreach + create") {
    // given
    givenGraph {
      bipartiteGraph(sizeHint, "A", "B", "R")
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .foreach(
        "i",
        "[1, 2, 3]",
        Seq(createPattern(
          nodes = Seq(createNode("n"), createNode("m")),
          relationships = Seq(createRelationship("r", "x", "R", "x"))
        ))
      )
      .nodeByLabelScan("x", "A")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    val foreachProfile = queryProfile.operatorProfile(1)

    foreachProfile.rows() shouldBe sizeHint
    foreachProfile.dbHits() shouldBe 3 * (2 /*nodes*/ + 1 /*relationship*/ + costOfRelationshipTypeLookup) * sizeHint
  }

  test("should profile rows and dbhits of foreach + set label correctly") {
    // given
    givenGraph {
      bipartiteGraph(sizeHint, "A", "B", "R")
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .foreach("i", "[1, 2, 3]", Seq(setLabel("x", "L", "M")))
      .nodeByLabelScan("x", "A")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    val foreachProfile = queryProfile.operatorProfile(1)

    val expectedDbHits =
      if (useWritesWithProfiling & canFuseOverPipelines) {
        val tokenLookup = 2 * 3 * sizeHint
        val setLabelToNode = 2 * sizeHint
        tokenLookup + setLabelToNode
      } else {
        2 * costOfLabelLookup + 3 * sizeHint
      }

    foreachProfile.rows() shouldBe sizeHint
    foreachProfile.dbHits() shouldBe expectedDbHits
  }

  test("should profile rows and dbhits of foreach + set dynamic label correctly") {
    // given
    givenGraph {
      bipartiteGraph(sizeHint, "A", "B", "R")
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .foreach("i", "[1, 2, 3]", Seq(setDynamicLabel("x", "'L'", "'M'")))
      .nodeByLabelScan("x", "A")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    val foreachProfile = queryProfile.operatorProfile(1)

    val expectedDbHits =
      if (useWritesWithProfiling & canFuseOverPipelines) {
        val tokenLookup = 2 * 3 * sizeHint
        val setLabelToNode = 2 * sizeHint
        tokenLookup + setLabelToNode
      } else {
        2 * costOfLabelLookup + 3 * sizeHint
      }

    foreachProfile.rows() shouldBe sizeHint
    foreachProfile.dbHits() shouldBe expectedDbHits
  }

  test("should profile rows and dbhits of foreach + remove label correctly") {
    // given
    givenGraph {
      bipartiteGraph(sizeHint, "A", "B", "R")
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .foreach("i", "[1, 2, 3]", Seq(removeLabel("x", "A")))
      .nodeByLabelScan("x", "A")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    val foreachProfile = queryProfile.operatorProfile(1)

    val expectedDbHits =
      if (useWritesWithProfiling & canFuseOverPipelines) {
        val tokenLookup = 1 * sizeHint
        val setLabelToNode = 3 * sizeHint
        tokenLookup + setLabelToNode
      } else {
        costOfLabelLookup + 3 * sizeHint
      }

    foreachProfile.rows() shouldBe sizeHint
    foreachProfile.dbHits() shouldBe expectedDbHits
  }

  test("should profile rows and dbhits of foreach + remove dynamic label correctly") {
    // given
    givenGraph {
      bipartiteGraph(sizeHint, "A", "B", "R")
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .foreach("i", "[1, 2, 3]", Seq(removeDynamicLabel("x", "'A'")))
      .nodeByLabelScan("x", "A")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    val foreachProfile = queryProfile.operatorProfile(1)

    val expectedDbHits =
      if (useWritesWithProfiling & canFuseOverPipelines) {
        val tokenLookup = 1 * sizeHint
        val setLabelToNode = 3 * sizeHint
        tokenLookup + setLabelToNode
      } else {
        costOfLabelLookup + 3 * sizeHint
      }

    foreachProfile.rows() shouldBe sizeHint
    foreachProfile.dbHits() shouldBe expectedDbHits
  }

  test("should profile rows and dbhits of foreach + delete correctly") {
    // given
    givenGraph {
      bipartiteGraph(sizeHint, "A", "B", "R")
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .foreach("n", "[x]", Seq(delete("n")))
      .nodeByLabelScan("x", "A")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    val foreachProfile = queryProfile.operatorProfile(1)

    foreachProfile.rows() shouldBe sizeHint
    foreachProfile.dbHits() shouldBe sizeHint
  }

  test("should profile db hits on set labels") {
    // given
    val emptyNodes = 2
    val nodesWithLabel = sizeHint - emptyNodes
    val label = "ExistingLabel"
    val newLabels = Seq("Label", "OtherLabel")
    val labelsSet = label +: newLabels

    givenGraph {
      nodeGraph(emptyNodes)
      nodeGraph(nodesWithLabel, label)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("l")
      .projection("labels(n) AS l")
      .setLabels("n", labelsSet.toSeq: _*)
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)
    val setLabelsProfile = runtimeResult.runtimeResult.queryProfile().operatorProfile(2)

    val setLabelsToNodeDbHits =
      if (useWritesWithProfiling) {
        emptyNodes * 3 + nodesWithLabel * 2
      } else {
        sizeHint
      }

    val labelTokenLookup = labelsSet.size

    setLabelsProfile.dbHits() shouldBe setLabelsToNodeDbHits + labelTokenLookup
  }

  test("should profile db hits on remove labels") {
    // given
    val nodeCount = sizeHint
    givenGraph {
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
    val removeLabelsProfile = runtimeResult.runtimeResult.queryProfile().operatorProfile(1)

    val expectedLabelLookups = 2

    val labelsRemovedFromNodeDbHits =
      if (useWritesWithProfiling) {
        (nodeCount - 3) * Seq("Label", "OtherLabel").size + 1 * Seq("Label").size + 1 * Seq("OtherLabel").size
      } else {
        nodeCount
      }

    removeLabelsProfile.dbHits() shouldBe labelsRemovedFromNodeDbHits + expectedLabelLookups
  }

  test("should profile db hits on set dynamic labels") {
    // given
    val emptyNodes = 2
    val nodesWithLabel = sizeHint - emptyNodes
    val label = "ExistingLabel"
    val newLabels = Seq("Label", "OtherLabel")
    val labelsSet = label +: newLabels

    givenGraph {
      nodeGraph(emptyNodes)
      nodeGraph(nodesWithLabel, label)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("l")
      .projection("labels(n) AS l")
      .setDynamicLabels("n", labelsSet.map(l => s"'$l''"): _*)
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)
    val setLabelsProfile = runtimeResult.runtimeResult.queryProfile().operatorProfile(2)

    val setLabelsToNodeDbHits =
      if (useWritesWithProfiling) {
        emptyNodes * 3 + nodesWithLabel * 2
      } else {
        sizeHint
      }

    val labelTokenLookup = labelsSet.size

    setLabelsProfile.dbHits() shouldBe setLabelsToNodeDbHits + labelTokenLookup
  }

  test("should profile db hits on remove dynamic labels") {
    // given
    val nodeCount = sizeHint
    givenGraph {
      nodeGraph(nodeCount - 3, "Label", "OtherLabel", "ThirdLabel")
      nodeGraph(1, "Label")
      nodeGraph(1, "OtherLabel")
      nodeGraph(1, "ThirdLabel")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .removeDynamicLabels("n", "`Label`", "`OtherLabel`")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)
    val removeLabelsProfile = runtimeResult.runtimeResult.queryProfile().operatorProfile(1)

    val expectedLabelLookups = 2

    val labelsRemovedFromNodeDbHits =
      if (useWritesWithProfiling) {
        (nodeCount - 3) * Seq("Label", "OtherLabel").size + 1 * Seq("Label").size + 1 * Seq("OtherLabel").size
      } else {
        nodeCount
      }

    removeLabelsProfile.dbHits() shouldBe labelsRemovedFromNodeDbHits + expectedLabelLookups
  }

  test("should profile db hits on set node properties from map") {
    // given
    givenGraph {
      nodeGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .setPropertiesFromMap("n", "{prop: 42, foo: 1}", removeOtherProps = false)
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)
    val setPropertiesFromMapProfile = runtimeResult.runtimeResult.queryProfile().operatorProfile(1)

    val expectedDbHits =
      if (useWritesWithProfiling & canFuseOverPipelines) {
        val setPropertyDbHits = sizeHint
        val tokenDbHits = sizeHint * 2
        setPropertyDbHits + tokenDbHits
      } else if (useWritesWithProfiling) {
        val setPropertyDbHits = sizeHint * 2
        val tokenDbHits = sizeHint * 2 * costOfPropertyToken
        setPropertyDbHits + tokenDbHits
      } else {
        val setPropertyDbHits = sizeHint
        val tokenDbHits = sizeHint * 2 * costOfPropertyToken
        setPropertyDbHits + tokenDbHits
      }

    setPropertiesFromMapProfile.dbHits() shouldBe expectedDbHits
  }

  test("should profile db hits on set node properties") {
    // given
    givenGraph {
      nodeGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .setNodeProperties("n", ("prop", "42"), ("foo", "1"))
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)
    val setPropertiesProfile = runtimeResult.runtimeResult.queryProfile().operatorProfile(1)

    val expectedDbHits =
      if (useWritesWithProfiling & canFuseOverPipelines) {
        val setPropertyDbHits = sizeHint
        val tokenDbHits = sizeHint * 2
        setPropertyDbHits + tokenDbHits
      } else if (useWritesWithProfiling) {
        // In non-fused pipeline we only count the number of set properties
        sizeHint * 2
      } else {
        // In interpreted runtimes we use lazy property types which will amount
        // to the following count:
        // - first time we will try to read the two props, 2 db hits
        // - since the props are not there we will create the two, +2 db hits
        // - second time we will try reading them again, +2 db hits
        // - now the ints are cached and subsequent calls will not be counted.
        sizeHint + 6
      }

    setPropertiesProfile.dbHits() shouldBe expectedDbHits
  }

  test("should profile db hits on set relationship properties from map") {
    // given
    val relationships = givenGraph {
      val (_, rels) = circleGraph(sizeHint)
      rels
    }
    val relationshipCount = relationships.size

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .setRelationshipPropertiesFromMap("r", "{prop: 42, foo: 1}", removeOtherProps = false)
      .expandAll("(n)-[r]->()")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)
    val setPropertiesFromMapProfile = runtimeResult.runtimeResult.queryProfile().operatorProfile(1)

    val expectedDbHits =
      if (useWritesWithProfiling & canFuseOverPipelines) {
        val setPropertyDbHits = relationshipCount
        val tokenDbHits = relationshipCount * 2
        setPropertyDbHits + tokenDbHits
      } else if (useWritesWithProfiling) {
        val setPropertyDbHits = relationshipCount * 2
        val tokenDbHits = relationshipCount * 2 * costOfPropertyToken
        setPropertyDbHits + tokenDbHits
      } else {
        val setPropertyDbHits = relationshipCount
        val tokenDbHits = relationshipCount * 2 * costOfPropertyToken
        setPropertyDbHits + tokenDbHits
      }

    setPropertiesFromMapProfile.dbHits() shouldBe expectedDbHits
  }

  test("should profile db hits on set relationship properties") {
    // given
    val relationships = givenGraph {
      val (_, rels) = circleGraph(sizeHint)
      rels
    }
    val relationshipCount = relationships.size

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .setRelationshipProperties("r", ("prop", "42"), ("foo", "1"))
      .expandAll("(n)-[r]->()")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)
    val setPropertiesProfile = runtimeResult.runtimeResult.queryProfile().operatorProfile(1)

    val expectedDbHits =
      if (useWritesWithProfiling & canFuseOverPipelines) {
        val setPropertyDbHits = relationshipCount
        val tokenDbHits = relationshipCount * 2
        setPropertyDbHits + tokenDbHits
      } else if (useWritesWithProfiling) {
        // In non-fused pipeline we only count the number of set properties
        relationshipCount * 2
      } else {
        // In interpreted runtimes we use lazy property types which will amount
        // to the following count:
        // - first time we will try to read the two props, 2 db hits
        // - since the props are not there we will create the two, +2 db hits
        // - second time we will try reading them again, +2 db hits
        // - now the ints are cached and subsequent calls will not be counted.
        relationshipCount + 6
      }

    setPropertiesProfile.dbHits() shouldBe expectedDbHits
  }

  protected def propertiesString(properties: Map[String, Any]): String = {
    properties.map { case (key, value) => s"$key: $value" }.mkString("{", ",", "}")
  }
}

trait NonFusedWriteOperatorsDbHitsTestBase[CONTEXT <: RuntimeContext] extends WriteOperatorsDbHitsTestBase[CONTEXT] {
  self: ProfileDbHitsTestBase[CONTEXT] =>

  test("should profile db hits on create nodes and relationships (non-fused)") {
    val createNodeCount = 7
    val createNodes = Range(0, createNodeCount).map(i => createNode(s"n$i"))
    val properties = Map("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4, "e" -> 5)
    val createRelationships = createNodes
      .sliding(2)
      .zipWithIndex
      .map {
        case (Seq(a, b), i) =>
          createRelationship(
            s"r$i",
            a.variable.name,
            "REL",
            b.variable.name,
            properties = Some(propertiesString(properties))
          )
      }
      .toSeq
    // given empty db

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults()
      .emptyResult()
      .create(createNodes ++ createRelationships: _*)
      .argument()
      .build(readOnly = false)

    // then
    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    val queryProfile = runtimeResult.runtimeResult.queryProfile()

    val createProfile = queryProfile.operatorProfile(2)
    val propertyHits = createRelationships.size
    val propertyTokenHits = createRelationships.size * properties.size
    val relationshipHits = /* create relationships */
      createRelationships.size + /* create relationship type */ createRelationships.size
    val nodeHits = createNodeCount
    createProfile.dbHits() shouldBe (nodeHits + relationshipHits + propertyHits + propertyTokenHits)
  }
}
