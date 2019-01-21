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
package org.neo4j.cypher.internal.runtime.spec.interpreted

import org.neo4j.cypher.internal.compatibility._
import org.neo4j.cypher.internal.runtime.spec._

abstract class AllNodeScanTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT]
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should scan all nodes") {
    // given
    val nodes = nodeGraph(10, "Honey") ++
      nodeGraph(10, "Butter") ++
      nodeGraph(10, "Almond")

    // when
    val logicalQuery = new LogicalQueryBuilder()
      .produceResults("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withSingleValueRows(nodes)
  }

  test("should scan empty graph") {
    // when
    val logicalQuery = new LogicalQueryBuilder()
      .produceResults("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withNoRows()
  }

  test("should handle multiple scans") {
    // given
    val nodes = nodeGraph(10, "Honey")

    // when
    val logicalQuery = new LogicalQueryBuilder()
      .produceResults("y", "z", "x")
      .apply()
      .|.allNodeScan("z")
      .apply()
      .|.allNodeScan("y")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {x <- nodes; y <- nodes; z <- nodes} yield Array(y, z, x)
    runtimeResult should beColumns("y", "z", "x").withRows(expected)
  }
}

class InterpretedAllNodeScanTest extends AllNodeScanTestBase(COMMUNITY_EDITION, InterpretedRuntime)
