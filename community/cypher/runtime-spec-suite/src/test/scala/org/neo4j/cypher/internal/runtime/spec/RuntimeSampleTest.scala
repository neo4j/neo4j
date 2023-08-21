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
package org.neo4j.cypher.internal.runtime.spec

import org.neo4j.cypher.internal.InterpretedRuntime

/**
 * Sample tests to demonstrate the runtime acceptance test framework. Remove eventually?
 */
class RuntimeSampleTest extends RuntimeTestSuite(COMMUNITY.EDITION, InterpretedRuntime) {

  test("sample test I - simple all nodes scan") {
    // given
    val nodes = nodeGraph(10)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(nodes))
  }

  test("sample test II - logical plan with branches") {
    // given
    val nodes = nodeGraph(2)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z", "q")
      .apply()
      .|.apply()
      .|.|.allNodeScan("q")
      .|.allNodeScan("z")
      .apply()
      .|.allNodeScan("y")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      x <- nodes
      y <- nodes
      z <- nodes
      q <- nodes
    } yield Array(x, y, z, q)
    runtimeResult should beColumns("x", "y", "z", "q").withRows(expected)
  }

}
