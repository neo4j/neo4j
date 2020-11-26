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

abstract class RelationshipTypeScanTest[CONTEXT <: RuntimeContext](
                                                               edition: Edition[CONTEXT],
                                                               runtime: CypherRuntime[CONTEXT],
                                                               sizeHint: Int
                                                             ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should find relationship") {
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

  test("should work with filter") {
    // given
    val (_, _, relationships, _) = given {
      bidirectionalBipartiteGraph(sizeHint, "A", "B", "R", "S")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .filter("x:NOT_THERE")
      .relationshipTypeScan("(x)-[:R]->(y)")
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
}
