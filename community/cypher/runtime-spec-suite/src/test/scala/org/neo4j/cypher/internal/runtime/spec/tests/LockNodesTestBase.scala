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

import java.util.concurrent.atomic.AtomicBoolean

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.values.storable.Values

abstract class LockNodesTestBase[CONTEXT <: RuntimeContext](
                                                             edition: Edition[CONTEXT],
                                                             runtime: CypherRuntime[CONTEXT],
                                                             sizeHint: Int
                                                           ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  /*
   Should not be flaky. However, it could give false successful result if the sleep time
    is too low (and we don't come to the point of executing logicalQuery2)
   */
  test("should lock nodes") {
    val nodes = given {
      nodeGraph(sizeHint)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("x.prop as prop")
      .lockNodes("x")
      .allNodeScan("x")
      .build()

    // request one row and lock the node
    val runtimeResult = execute(logicalQuery, runtime)
    request(1, runtimeResult)

    // In another thread: set property to 2 (needs to wait until lock has been released)
    val thread2HasBeenExecuted = new AtomicBoolean(false)
    val thread2ReturnedCorrectResults = new AtomicBoolean(false)
    val thread = new Thread(() => {
      val logicalQuery2 = new LogicalQueryBuilder(this)
        .produceResults("prop")
        .projection("x.prop as prop")
        .setProperty("x", "prop", "2")
        .allNodeScan("x")
        .build()

      val expected = singleColumn(nodes.map(_ => Values.longValue(2L)))
      try {
        val result2 = executeAndConsumeTransactionally(logicalQuery2, runtime) //shouldn't run until lock has been released
        expected.matchesRaw(Array("prop"), result2) // verify correct result
        thread2ReturnedCorrectResults.set(true)
      } catch {
        case e: MatchError => thread2ReturnedCorrectResults.set(false)
      }
      thread2HasBeenExecuted.set(true)
    })

    // Sleep for 1 second and verify that the query in the other thread is waiting for lock
    thread.start()
    Thread.sleep(1000)
    thread2HasBeenExecuted.get() shouldBe false

    // consume result ()
    consume(runtimeResult)
    restartTx()
    thread.join(100000)

    // verify correct result
    thread2ReturnedCorrectResults.get() shouldBe true
  }
}
