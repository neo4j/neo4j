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
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.internal.index.label.RelationshipTypeScanStoreSettings.enable_relationship_type_scan_store

abstract class RelationshipTypeScanTestBase[CONTEXT <: RuntimeContext](
                                                               edition: Edition[CONTEXT],
                                                               runtime: CypherRuntime[CONTEXT],
                                                               sizeHint: Int
                                                             ) extends RuntimeTestSuite[CONTEXT](edition.copyWith(enable_relationship_type_scan_store -> java.lang.Boolean.TRUE), runtime) {

  test("should support directed relationship scan") {
    // given
    val (_, _, relationships, _) = given {
      bidirectionalBipartiteGraph(sizeHint, "A", "B", "R", "S")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .relationshipTypeScan("(x)-[r:R]->(y)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r", "x", "y").withRows(relationships.map(r => Array(r, r.getStartNode, r.getEndNode)))
  }

  test("should combine directed type scan and filter") {
    // given
    val (_, _, relationships, _) = given {
      bidirectionalBipartiteGraph(sizeHint, "A", "B", "R", "S")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .filter("x:NOT_THERE")
      .relationshipTypeScan("(x)-[r:R]->(y)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r", "x", "y").withNoRows()
  }

  test("should handle multiple directed scans") {
    // given
    val (_, relationships) = given { circleGraph(10, "L") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r1", "r2", "r3")
      .apply()
      .|.relationshipTypeScan("()-[r3:R]->()")
      .apply()
      .|.relationshipTypeScan("()-[r2:R]->()")
      .relationshipTypeScan("()-[r1:R]->()")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {r1 <- relationships; r2 <- relationships; r3 <- relationships} yield Array(r1, r2, r3)
    runtimeResult should beColumns("r1", "r2", "r3").withRows(expected)
  }

  test("should support undirected relationship scan") {
    // given
    val (_, _, relationships, _) = given {
      bidirectionalBipartiteGraph(sizeHint, "A", "B", "R", "S")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .relationshipTypeScan("(x)-[r:R]-(y)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r", "x", "y").withRows(relationships.flatMap(r => Seq(Array(r, r.getStartNode, r.getEndNode), Array(r, r.getEndNode, r.getStartNode))))
  }

  test("should combine undirected type scan and filter") {
    // given
    val (_, _, relationships, _) = given {
      bidirectionalBipartiteGraph(sizeHint, "A", "B", "R", "S")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .filter("x:NOT_THERE")
      .relationshipTypeScan("(x)-[r:R]-(y)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r", "x", "y").withNoRows()
  }

  test("should handle multiple undirected scans") {
    // given
    val (_, relationships) = given { circleGraph(10, "L") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r1", "r2", "r3")
      .apply()
      .|.relationshipTypeScan("()-[r3:R]-()")
      .apply()
      .|.relationshipTypeScan("()-[r2:R]-()")
      .relationshipTypeScan("()-[r1:R]-()")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {r1 <- relationships; r2 <- relationships; r3 <- relationships} yield Seq.fill(8)(Array(r1, r2, r3))
    runtimeResult should beColumns("r1", "r2", "r3").withRows(expected.flatten)
  }

  //TODO: These tests should live in ProfileRowsTestBase but lives here instead because they rely on typescans being enabled
  test("should profile rows with directed relationship type scan") {
    // given
    val nodesPerLabel = 20
    val (_, _, rs, _) = given {
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
    val (_, _, rs, _) = given {
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

  //TODO: These tests should live in ProfileRowsTestBase but lives here instead because they rely on typescans being enabled
  test("should profile dbHits of directed relationship type scan") {
    // given
    given { circleGraph(sizeHint) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .relationshipTypeScan("(x)-[r:R]->(y)")
      .build()

    val result = profile(logicalQuery, runtime)
    consume(result)

    // then
    //TODO: Interpreted and slotted doesn't count relationshipById as a dbhit
    //      is this a bug?
    result.runtimeResult.queryProfile().operatorProfile(1).dbHits() should
      (be (sizeHint + 1 + 1 /*costOfRelationshipTypeLookup*/) or be (2 * sizeHint + 1 + 0/*costOfRelationshipTypeLookup*/))
  }

  test("should profile dbHits of undirected relationship type scan") {
    // given
    given { circleGraph(sizeHint) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .relationshipTypeScan("(x)-[r:R]-(y)")
      .build()

    val result = profile(logicalQuery, runtime)
    consume(result)

    // then
    //TODO: Interpreted and slotted doesn't count relationshipById as a dbhit
    //      is this a bug?
    result.runtimeResult.queryProfile().operatorProfile(1).dbHits() should
      (be (sizeHint + 1 + 1 /*costOfRelationshipTypeLookup*/) or be (2 * sizeHint + 1 + 0/*costOfRelationshipTypeLookup*/))
  }


}
