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
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.options.CypherConnectComponentsPlannerOption
import org.neo4j.cypher.internal.options.CypherDebugOption
import org.neo4j.cypher.internal.options.CypherDebugOptions
import org.neo4j.cypher.internal.options.CypherExecutionMode
import org.neo4j.cypher.internal.options.CypherExpressionEngineOption
import org.neo4j.cypher.internal.options.CypherInterpretedPipesFallbackOption
import org.neo4j.cypher.internal.options.CypherOperatorEngineOption
import org.neo4j.cypher.internal.options.CypherPlannerOption
import org.neo4j.cypher.internal.options.CypherQueryOptions
import org.neo4j.cypher.internal.options.CypherReplanOption
import org.neo4j.cypher.internal.options.CypherRuntimeOption
import org.neo4j.cypher.internal.options.CypherUpdateStrategy
import org.neo4j.cypher.internal.options.CypherVersion
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class CacheKeyTest extends CypherFunSuite {

  test("For default options, only version is part of cache key") {
    val options = CypherQueryOptions.default

    options.cacheKey
           .shouldEqual(CypherVersion.default.name)
  }

  test("EXPLAIN does not appear in cache key") {
    val options = CypherQueryOptions.default.copy(executionMode = CypherExecutionMode.explain)

    options.cacheKey
           .shouldEqual(CypherVersion.default.name)
  }

  test("All non-default options should be part of cache key, except replan") {
    val options = CypherQueryOptions(
      executionMode = CypherExecutionMode.profile,
      version = CypherVersion.v3_5,
      planner = CypherPlannerOption.dp,
      runtime = CypherRuntimeOption.pipelined,
      updateStrategy = CypherUpdateStrategy.eager,
      expressionEngine = CypherExpressionEngineOption.interpreted,
      operatorEngine = CypherOperatorEngineOption.interpreted,
      interpretedPipesFallback = CypherInterpretedPipesFallbackOption.allPossiblePlans,
      replan = CypherReplanOption.force,
      connectComponentsPlanner = CypherConnectComponentsPlannerOption.idp,
      debugOptions = CypherDebugOptions(Set(CypherDebugOption.queryGraph, CypherDebugOption.tostring))
    )

    options.cacheKey
      .shouldEqual("3.5 PROFILE planner=dp runtime=pipelined updateStrategy=eager expressionEngine=interpreted operatorEngine=interpreted interpretedPipesFallback=all connectComponentsPlanner=idp debug=querygraph debug=tostring")
  }

  test("Only certain non-default options should be part of logical plan cache key") {
    val options = CypherQueryOptions(
      executionMode = CypherExecutionMode.profile,
      version = CypherVersion.v3_5,
      planner = CypherPlannerOption.dp,
      runtime = CypherRuntimeOption.pipelined,
      updateStrategy = CypherUpdateStrategy.eager,
      expressionEngine = CypherExpressionEngineOption.interpreted,
      operatorEngine = CypherOperatorEngineOption.interpreted,
      interpretedPipesFallback = CypherInterpretedPipesFallbackOption.allPossiblePlans,
      replan = CypherReplanOption.force,
      connectComponentsPlanner = CypherConnectComponentsPlannerOption.idp,
      debugOptions = CypherDebugOptions(Set(CypherDebugOption.queryGraph, CypherDebugOption.tostring))
    )

    options.logicalPlanCacheKey
      .shouldEqual(
        """updateStrategy=eager connectComponentsPlanner=idp"""
      )
  }
}
