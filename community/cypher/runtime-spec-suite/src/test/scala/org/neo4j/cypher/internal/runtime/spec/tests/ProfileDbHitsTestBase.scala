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

abstract class ProfileDbHitsTestBase[CONTEXT <: RuntimeContext](edition: Edition[CONTEXT],
                                                                runtime: CypherRuntime[CONTEXT],
                                                                sizeHint: Int,
                                                                costOfProperty: Int,
                                                                costOfLabelScan: Int,
                                                                costOfExpand: Int
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
    queryProfile.operatorProfile(1).dbHits() should be (sizeHint + costOfLabelScan) // label scan
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
    // assertions on property dbHits is tricky because in morsel it's more expensive to
    // traverse properties late in the chain, which in interpreted/slotted all property reads
    // cost 1 dbHit
    queryProfile.operatorProfile(1).dbHits() should (be (sizeHint * costOfProperty * (1+2)) or  // 1 x.list + 2 x.prop
                                                     (be (sizeHint * costOfProperty * (2+2)) or // late x.list in morsel
                                                      be (sizeHint * costOfProperty * (1+4))))  // late x.prop in morsel
  }
}
