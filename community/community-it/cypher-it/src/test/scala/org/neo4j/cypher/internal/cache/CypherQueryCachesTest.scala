/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.cache

import org.neo4j.cypher.ExecutionEngineTestSupport
import org.neo4j.cypher.GraphDatabaseTestSupport
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class CypherQueryCachesTest extends CypherFunSuite with GraphDatabaseTestSupport with ExecutionEngineTestSupport {

  test("can get up to date statistics on cache entries") {

    val stats = eengine.queryCaches.statistics()

    execute("RETURN 1 AS i")
    execute("RETURN 2 AS i")
    execute("RETURN 3 AS i")
    execute("RETURN 3 AS i")
    execute("CYPHER 4.3 RETURN 1 AS i")
    execute("CYPHER runtime=slotted RETURN 1 AS i")
    execute("CYPHER planner=dp RETURN 1 AS i")
    execute("CYPHER planner=dp RETURN 1 AS i")
    execute("CYPHER expressionEngine=interpreted RETURN 1 AS i")

    stats.preParserCacheEntries().shouldEqual(7)
    stats.astCacheEntries().shouldEqual(7)
    stats.logicalPlanCacheEntries().shouldEqual(4)
    stats.executionPlanCacheEntries().shouldEqual(5)
    stats.executableQueryCacheEntries().shouldEqual(7)

    execute("RETURN 1 AS i")
    execute("RETURN 4 AS i")
    execute("CYPHER planner=dp RETURN 1 AS i")
    execute("CYPHER expressionEngine=interpreted RETURN 1 AS i")

    stats.preParserCacheEntries().shouldEqual(8)
    stats.astCacheEntries().shouldEqual(8)
    stats.logicalPlanCacheEntries().shouldEqual(4)
    stats.executionPlanCacheEntries().shouldEqual(5)
    stats.executableQueryCacheEntries().shouldEqual(8)
  }

  test("clearing caches is reflected in statistics") {

    val stats = eengine.queryCaches.statistics()

    execute("RETURN 1 AS i")
    execute("RETURN 2 AS i")
    execute("RETURN 3 AS i")
    execute("CYPHER 4.3 RETURN 1 AS i")
    execute("CYPHER runtime=slotted RETURN 1 AS i")
    execute("CYPHER planner=dp RETURN 1 AS i")

    val counts0 = Map(
      "preparser" -> 6,
      "ast" -> 6,
      "logical_plan" -> 4,
      "execution_plan" -> 4,
      "executable_query" -> 6
    )

    stats.preParserCacheEntries().shouldEqual(6)
    stats.astCacheEntries().shouldEqual(6)
    stats.logicalPlanCacheEntries().shouldEqual(4)
    stats.executionPlanCacheEntries().shouldEqual(4)
    stats.executableQueryCacheEntries().shouldEqual(6)

    eengine.queryCaches.preParserCache.clear()

    stats.preParserCacheEntries().shouldEqual(0)
    stats.astCacheEntries().shouldEqual(6)
    stats.logicalPlanCacheEntries().shouldEqual(4)
    stats.executionPlanCacheEntries().shouldEqual(4)
    stats.executableQueryCacheEntries().shouldEqual(6)

    eengine.queryCaches.executionPlanCache.clear()

    stats.preParserCacheEntries().shouldEqual(0)
    stats.astCacheEntries().shouldEqual(6)
    stats.logicalPlanCacheEntries().shouldEqual(4)
    stats.executionPlanCacheEntries().shouldEqual(0)
    stats.executableQueryCacheEntries().shouldEqual(6)

    eengine.queryCaches.executableQueryCache.clear()

    stats.preParserCacheEntries().shouldEqual(0)
    stats.astCacheEntries().shouldEqual(6)
    stats.logicalPlanCacheEntries().shouldEqual(4)
    stats.executionPlanCacheEntries().shouldEqual(0)
    stats.executableQueryCacheEntries().shouldEqual(0)

    eengine.compilerLibrary.clearCaches()

    stats.preParserCacheEntries().shouldEqual(0)
    stats.astCacheEntries().shouldEqual(0)
    stats.logicalPlanCacheEntries().shouldEqual(0)
    stats.executionPlanCacheEntries().shouldEqual(0)
    stats.executableQueryCacheEntries().shouldEqual(0)
  }

  test("clearAll clears all caches") {

    val stats = eengine.queryCaches.statistics()

    execute("RETURN 1 AS i")
    execute("RETURN 2 AS i")
    execute("RETURN 3 AS i")
    execute("CYPHER 4.3 RETURN 1 AS i")
    execute("CYPHER runtime=slotted RETURN 1 AS i")
    execute("CYPHER planner=dp RETURN 1 AS i")

    stats.preParserCacheEntries().shouldEqual(6)
    stats.astCacheEntries().shouldEqual(6)
    stats.logicalPlanCacheEntries().shouldEqual(4)
    stats.executionPlanCacheEntries().shouldEqual(4)
    stats.executableQueryCacheEntries().shouldEqual(6)

    eengine.queryCaches.clearAll()

    stats.preParserCacheEntries().shouldEqual(0)
    stats.astCacheEntries().shouldEqual(0)
    stats.logicalPlanCacheEntries().shouldEqual(0)
    stats.executionPlanCacheEntries().shouldEqual(0)
    stats.executableQueryCacheEntries().shouldEqual(0)
  }
}
