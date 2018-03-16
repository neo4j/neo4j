/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.compiler.v3_4.helpers.ListSupport
import org.neo4j.cypher.{CypherExecutionException, ExecutionEngineFunSuite, QueryStatisticsTestSupport}

class SpatialUniqueConstraintValidationAcceptanceTest
  extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with ListSupport {

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
