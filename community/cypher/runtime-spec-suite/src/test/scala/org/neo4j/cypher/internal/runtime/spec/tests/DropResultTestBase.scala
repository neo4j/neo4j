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
import org.neo4j.cypher.internal.runtime.TestSubscriber
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite

abstract class DropResultTestBase[CONTEXT <: RuntimeContext](
                                                              edition: Edition[CONTEXT],
                                                              runtime: CypherRuntime[CONTEXT],
                                                              sizeHint: Int
                                                            ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should not produce any rows") {
    given {
      nodeGraph(sizeHint)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .dropResult()
      .allNodeScan("n")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n").withNoRows()
  }

  test("should not produce any rows on RHS of apply") {
    given {
      nodeGraph(sizeHint)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .apply()
      .|.dropResult()
      .|.argument("n")
      .allNodeScan("n")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n").withNoRows()
  }

  test("should work with aggregation") {
    given {
      nodeGraph(sizeHint)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("count(n) as c"))
      .dropResult()
      .allNodeScan("n")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("c").withSingleRow(0)
  }

  test("should let through all LHS rows of antiSemiApply") {
    val nodes = given {
      nodeGraph(sizeHint)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .antiSemiApply()
      .|.dropResult()
      .|.argument("n")
      .allNodeScan("n")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n").withRows(nodes.map(Array[Any](_)))
  }

  test("should not exhaust input") {
    val inputStream = inputColumns(nBatches = sizeHint / 10, batchSize = 10, identity).stream()

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .dropResult()
      .input(variables = Seq("x"))
      .build()

    val result = execute(logicalQuery, runtime, inputStream, TestSubscriber.concurrent)

    result.request(Long.MaxValue)
    result.await() shouldBe false

    inputStream.hasMore shouldBe true
  }
}
