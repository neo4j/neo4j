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

abstract class ProfileTimeTestBase[CONTEXT <: RuntimeContext](edition: Edition[CONTEXT],
                                                              runtime: CypherRuntime[CONTEXT],
                                                              sizeHint: Int
                                                             ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  // time is profiled in nano-seconds, but we can only assert > 0, because the operators take
  // different time on different tested systems.

  test("should profile time of all nodes scan + produce results") {
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
    queryProfile.operatorProfile(0).time() should be > 0L // produce results
    queryProfile.operatorProfile(1).time() should be > 0L // all nodes scan
  }

  test("should profile time with apply") {
    // given
    val size = sizeHint / 10
    nodePropertyGraph(size, {
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
    queryProfile.operatorProfile(0).time() should be > 0L // produce results
    queryProfile.operatorProfile(1).time() should be > 0L // filter
    queryProfile.operatorProfile(2).time() shouldBe 0L // apply
    queryProfile.operatorProfile(3).time() should be > 0L // filter
    queryProfile.operatorProfile(4).time() should be > 0L // all node scan
    queryProfile.operatorProfile(5).time() should be > 0L // all node scan
  }

  test("should profile time of sort") {
    // given
    nodeGraph(sizeHint)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .sort(Seq(Ascending("x")))
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).time() should be > 0L // produce results
    queryProfile.operatorProfile(1).time() should be > 0L // sort
    queryProfile.operatorProfile(2).time() should be > 0L // all node scan
  }
}
