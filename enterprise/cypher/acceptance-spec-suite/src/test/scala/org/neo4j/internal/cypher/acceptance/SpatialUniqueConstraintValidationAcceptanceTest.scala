/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.compiler.v3_4.helpers.ListSupport
import org.neo4j.cypher.{CypherExecutionException, ExecutionEngineFunSuite, QueryStatisticsTestSupport}

class SpatialUniqueConstraintValidationAcceptanceTest
  extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with ListSupport {

  test("should be able to create uniqueness constraint after nodes") {
    execute("CREATE ( :Label { location: point({x:1, y:2}) } )")
    execute("CREATE ( :Label { location: point({x:1, y:3}) } )")

    execute("CREATE CONSTRAINT ON (node:Label) ASSERT node.location IS UNIQUE")
  }

  test("should enforce uniqueness constraint on create node with label and property") {
    // GIVEN
    execute("CREATE CONSTRAINT ON (node:Label1) ASSERT node.key1 IS UNIQUE")
    execute("CREATE ( node:Label1 { key1: point({x:1, y:2}) } )")

    // WHEN
    val exception = intercept[CypherExecutionException] {
      execute("CREATE ( node:Label1 { key1: point({x:1, y:2}) } )")
    }
    exception.getMessage should include("`key1` = {geometry: {type: \"Point\", coordinates: [1.0, 2.0]")
  }

  test("should enforce uniqueness constraint on set property") {
    // GIVEN
    execute("CREATE CONSTRAINT ON (node:Label1) ASSERT node.key1 IS UNIQUE")
    execute("CREATE ( node1:Label1 { seq: 1, key1: point({x:1, y:2}) } ), ( node2:Label1 { seq: 2 } )")

    // WHEN
    val exception = intercept[CypherExecutionException] {
      execute("MATCH (node2:Label1) WHERE node2.seq = 2 SET node2.key1 = point({x:1, y:2})")
    }
    exception.getMessage should include("`key1` = {geometry: {type: \"Point\", coordinates: [1.0, 2.0]")
  }

  test("should enforce uniqueness constraint on add label") {
    // GIVEN
    execute("CREATE CONSTRAINT ON (node:Label1) ASSERT node.key1 IS UNIQUE")
    execute("CREATE ( node1:Label1 { seq: 1, key1: point({x:1, y:2}) } ), ( node2 { seq: 2, key1: point({x:1, y:2}) } )")

    // WHEN
    val exception = intercept[CypherExecutionException] {
      execute("MATCH (node2) WHERE node2.seq = 2 SET node2:Label1")
    }
    exception.getMessage should include("`key1` = {geometry: {type: \"Point\", coordinates: [1.0, 2.0]")
  }

  test("should enforce uniqueness constraint on conflicting data in same statement") {
    // GIVEN
    execute("CREATE CONSTRAINT ON (node:Label1) ASSERT node.key1 IS UNIQUE")

    // WHEN
    val exception = intercept[CypherExecutionException] {
      execute("CREATE ( node1:Label1 { key1: point({x:1, y:2}) } ), ( node2:Label1 { key1: point({x:1, y:2}) } )")
    }
    exception.getMessage should include("`key1` = {geometry: {type: \"Point\", coordinates: [1.0, 2.0]")
  }

  test("should allow remove and add conflicting data in one statement") {
    // GIVEN
    execute("CREATE CONSTRAINT ON (node:Label1) ASSERT node.key1 IS UNIQUE")
    execute("CREATE ( node:Label1 { seq:1, key1: point({x:1, y:2}) } )")

    var seq = 2
    for (resolve <- List("DELETE toRemove", "REMOVE toRemove.key1", "REMOVE toRemove:Label1", "SET toRemove.key1 = point({x:3, y:4})")) {
      // WHEN
      execute(s"MATCH (toRemove:Label1 {key1: point({x:1, y:2})}) $resolve CREATE ( toAdd:Label1 { seq: {seq}, key1: point({x:1, y:2}) } )", "seq" -> seq)

      // THEN
      val result = execute("MATCH (n:Label1) WHERE n.key1 =  point({x:1, y:2}) RETURN n.seq AS seq")
      result.columnAs[Int]("seq").toList should equal(List(seq))
      seq += 1
    }
  }

  test("should allow creation of non conflicting data") {
    // GIVEN
    execute("CREATE CONSTRAINT ON (node:Label1) ASSERT node.key1 IS UNIQUE")
    execute("CREATE ( node:Label1 { key1: point({x:1, y:2}) } )")
    execute("CREATE ( node:Label1 { key1: point({x:1, y:1000000}) } )")
    execute("CREATE ( node:Label1 { key1: point({x:1, y:2, crs: 'wgs-84'}) } )")
    execute("CREATE ( node:Label1 { key1: point({x:1, y:1000000, crs: 'wgs-84'}) } )")

    // WHEN
    Set("cartesian", "wgs-84").foreach { crs =>
      execute(s"CREATE ( node { key1: point({x:1, y:2, crs: '$crs'}) } )")
      execute(s"CREATE ( node:Label2 { key1: point({x:1, y:2, crs: '$crs'}) } )")
      execute(s"CREATE ( node:Label1 { key1: point({x:3, y:4, crs: '$crs'}) } )")
      execute(s"CREATE ( node:Label1 { key2: point({x:1, y:2, crs: '$crs'}) } )")
      execute(s"CREATE ( node:Label1 { key1: point({x:1, y:1000001, crs: '$crs'}) } )")
      execute(s"CREATE ( node:Label1 { key1: point({x:1, y:2000000, crs: '$crs'}) } )")
    }

    // THEN
    val result = execute("MATCH (n) RETURN count(*) AS nodeCount")
    result.columnAs[Int]("nodeCount").toList should equal(List(16))
  }
}
