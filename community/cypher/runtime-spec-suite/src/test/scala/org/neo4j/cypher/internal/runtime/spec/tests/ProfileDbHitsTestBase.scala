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
import org.neo4j.cypher.result.QueryProfile

abstract class ProfileDbHitsTestBase[CONTEXT <: RuntimeContext](
                                  edition: Edition[CONTEXT],
                                  runtime: CypherRuntime[CONTEXT],
                                  sizeHint: Int,
                                  costOfProperty: Int, // the reported dbHits for a single property lookup
                                  costOfLabelLookup: Int, // the reported dbHits for finding the id of a label
                                  costOfExpand: Int, // the reported dbHits for expanding a one-relationship node
                                  costOfRelationshipTypeLookup: Int // the reported dbHits for finding the id of a relationship type
                                                         ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should profile dbHits of all nodes scan") {
    // given
    nodeGraph(sizeHint)

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
    // given
    nodeGraph(3, "Dud")
    nodeGraph(sizeHint, "It")
    nodeGraph(3, "Decoy")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeByLabelScan("x", "It")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(1).dbHits() should be (sizeHint + 1 + costOfLabelLookup) // label scan
  }

  test("should profile dbHits of node index seek") {
    // given
    nodePropertyGraph(sizeHint, {
      case i if i % 10 == 0 => Map("difficulty" -> i)
    },"Language")

    index("Language", "difficulty")

    // when
    val seekProfile = profileIndexSeek(s"x:Language(difficulty >= ${sizeHint / 2})")
    // then
    seekProfile.operatorProfile(1).dbHits() should be (sizeHint / 10 / 2 + 1) // node index seek
  }

  test("should profile dbHits of node index scan") {
    // given
    nodePropertyGraph(sizeHint, {
      case i if i % 10 == 0 => Map("difficulty" -> i)
    },"Language")

    index("Language", "difficulty")

    // when
    val scanProfile = profileIndexSeek("x:Language(difficulty)")
    // then
    scanProfile.operatorProfile(1).dbHits() should be (sizeHint / 10 + 1) // node index scan
  }

  test("should profile dbHits of node index contains") {
    // given
    nodePropertyGraph(sizeHint, {
      case i => Map("difficulty" -> s"x${i%2}")
    },"Language")

    index("Language", "difficulty")

    // when
    val seekProfile = profileIndexSeek(s"x:Language(difficulty CONTAINS '1')")
    // then
    seekProfile.operatorProfile(1).dbHits() should be (sizeHint / 2 + 1) // node index contains
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
    val nodes = nodeGraph(17)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeByIdSeek("x", nodes(7).getId, nodes(11).getId, nodes(13).getId)
      .build()

    val result = profile(logicalQuery, runtime)
    consume(result)

    // then
    result.runtimeResult.queryProfile().operatorProfile(1).dbHits() shouldBe 3 // node by id
  }

  test("should profile dbHits of directed relationship by id") {
    // given
    val (_, rels) = circleGraph(17)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .directedRelationshipByIdSeek("r", "x", "y", rels(13).getId)
      .build()

    val result = profile(logicalQuery, runtime)
    consume(result)

    // then
    result.runtimeResult.queryProfile().operatorProfile(1).dbHits() shouldBe 1
  }

  test("should profile dbHits of NodeCountFromCountStore") {
    // given
    nodeGraph(10, "LabelA")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeCountFromCountStore("x", List(None))
      .build()

    val result = profile(logicalQuery, runtime)
    consume(result)

    // then
    result.runtimeResult.queryProfile().operatorProfile(1).dbHits() shouldBe 1 // node count from count store
  }

  test("should profile dbHits of RelationshipFromCountStore") {
    // given
    bipartiteGraph(10, "LabelA", "LabelB", "RelType")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .relationshipCountFromCountStore("x", None, List("RelType"), Some("LabelB"))
      .build()

    val result = profile(logicalQuery, runtime)
    consume(result)

    // then
    result.runtimeResult.queryProfile().operatorProfile(1).dbHits() shouldBe 1 + costOfRelationshipTypeLookup // relationship count from count store
  }

  test("should profile dbHits of input + produce results") {
    // given
    val nodes = nodeGraph(sizeHint)
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
    queryProfile.operatorProfile(0).dbHits() shouldBe 0 // produce results
    queryProfile.operatorProfile(1).dbHits() shouldBe 0 // input
  }

  test("should profile dbHits of sort + filter") {
    // given
    nodePropertyGraph(sizeHint,{
      case i => Map("prop" -> i)
    })

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
    queryProfile.operatorProfile(1).dbHits() shouldBe sizeHint * costOfProperty // filter
    queryProfile.operatorProfile(2).dbHits() shouldBe 0 // sort
  }

  test("should profile dbHits of limit") {
    // given
    nodeGraph(sizeHint)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .limit(10)
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(1).dbHits() shouldBe 0 // limit
    queryProfile.operatorProfile(2).dbHits() should be >= (1+10L) // all node scan
  }

  test("should profile dbHits with limit + expand") {
    // given
    val nodes = nodeGraph(sizeHint * 10)
    connect(nodes, Seq((1, 2, "REL")))

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
    queryProfile.operatorProfile(1).dbHits() shouldBe (sizeHint * 2L + costOfExpand) // expand
    queryProfile.operatorProfile(2).dbHits() shouldBe 0 // limit
    queryProfile.operatorProfile(3).dbHits() should be >= (sizeHint * 2L) // all node scan
  }

  test("should profile dbHits with var-expand") {
    // given
    val SIZE = sizeHint / 4
    val (nodes, relationships) = circleGraph(SIZE)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .expand("(x)-[*1..3]->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val matchesPerVarExpand = 3
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(1).dbHits() shouldBe (SIZE * (1 + costOfExpand) * matchesPerVarExpand) // expand
    queryProfile.operatorProfile(2).dbHits() should be >= SIZE.toLong // all node scan
  }

  test("should profile dbHits with node hash join") {
    // given
    nodePropertyGraph(sizeHint,{
      case i => Map("prop" -> i)
    })

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
    queryProfile.operatorProfile(1).dbHits() shouldBe 0 // node hash join
    queryProfile.operatorProfile(2).dbHits() shouldBe sizeHint * costOfProperty // filter
    queryProfile.operatorProfile(3).dbHits() should (be (sizeHint) or be (sizeHint + 1)) // all node scan
    queryProfile.operatorProfile(4).dbHits() shouldBe sizeHint * costOfProperty // filter
    queryProfile.operatorProfile(5).dbHits() should (be (sizeHint) or be (sizeHint + 1)) // all node scan
  }

  test("should profile dbHits with apply") {
    // given
    val size = sizeHint / 10
    nodePropertyGraph(size,{
      case i => Map("prop" -> i)
    })

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
    queryProfile.operatorProfile(1).dbHits() shouldBe (size / 2 * size) * costOfProperty // filter
    queryProfile.operatorProfile(2).dbHits() shouldBe 0 // apply
    queryProfile.operatorProfile(3).dbHits() shouldBe size * size * costOfProperty // filter
    queryProfile.operatorProfile(4).dbHits() should (be (size * size) or be (size * (1+size))) // all node scan
    queryProfile.operatorProfile(5).dbHits() should (be (size) or be (size + 1)) // all node scan
  }

  test("should profile dbHits of expression reads") {
    // given
    nodePropertyGraph(sizeHint, { case i => Map("list" -> Array(i, i+1), "prop" -> i)})

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("exists")
      .projection("[p IN x.list | x.prop = p] AS exists")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    // assertions on property dbHits are tricky because in morsel more dbHits are reported for
    // properties late in the chain, while in interpreted/slotted all property reads cost only 1 dbHit
    queryProfile.operatorProfile(1).dbHits() should (be (sizeHint * costOfProperty * (1+2)) or  // 1 x.list + 2 x.prop
                                                     (be (sizeHint * costOfProperty * (2+2)) or // late x.list in morsel
                                                      be (sizeHint * costOfProperty * (1+4))))  // late x.prop in morsel
  }

  test("should profile dbHits of aggregation") {
    // given
    nodePropertyGraph(sizeHint, { case i => Map("prop" -> i)})

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("avg")
      .aggregation(Seq.empty, Seq("avg(x.prop) AS avg"))
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(1).dbHits() shouldBe sizeHint * costOfProperty // late x.prop in morsel
  }

  // TODO fix for Morsel. test passes in Slotted but failed in (non)fused Morsel with wrong count
  ignore("should profile dbHits of aggregation with grouping") {
    // given
    val aggregationGroups = sizeHint / 2

    nodePropertyGraph(sizeHint, { case i => Map("group" -> i % aggregationGroups, "prop" -> i)})

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("group", "avg")
      .aggregation(Seq("x.group AS group"), Seq("avg(x.prop) AS avg"))
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(1).dbHits() shouldBe sizeHint * (costOfProperty * 2) // late x.prop in morsel
  }
}
