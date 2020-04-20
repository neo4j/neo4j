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

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.result.OperatorProfile

abstract class ProfileMemoryTrackingDisabledTestBase[CONTEXT <: RuntimeContext](edition: Edition[CONTEXT],
                                                                                runtime: CypherRuntime[CONTEXT],
                                                                                sizeHint: Int
                                                               ) extends RuntimeTestSuite[CONTEXT](
  edition.copyWith(GraphDatabaseSettings.track_query_allocation -> java.lang.Boolean.FALSE),
  runtime) {

  test("should profile memory even if tracking disabled") {
    given { nodeGraph(sizeHint) }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .distinct("x AS x")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).maxAllocatedMemory() should (be(OperatorProfile.NO_DATA) or be(0L)) // produce results
    queryProfile.operatorProfile(1).maxAllocatedMemory() should be > 0L  // distinct
    queryProfile.operatorProfile(2).maxAllocatedMemory() should (be(OperatorProfile.NO_DATA) or be > 0L) // all node scan
    queryProfile.maxAllocatedMemory()should be > 0L
  }
}
