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
package org.neo4j.cypher.internal

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.CypherCurrentCompiler.CypherExecutableQuery
import org.neo4j.cypher.internal.frontend.PlannerName
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes
import org.neo4j.cypher.internal.runtime.READ_ONLY
import org.neo4j.cypher.internal.util.CartesianProductNotification
import org.neo4j.cypher.internal.util.DeprecatedPropertyReferenceInMerge
import org.neo4j.cypher.internal.util.DeprecatedRelTypeSeparatorNotification
import org.neo4j.cypher.internal.util.DeprecatedRuntimeNotification
import org.neo4j.cypher.internal.util.DeprecatedTextIndexProvider
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.internal.util.UnboundedShortestPathNotification
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.kernel.api.query.CompilerInfo
import org.neo4j.kernel.api.query.QueryObfuscator
import org.neo4j.monitoring.Monitors
import org.neo4j.values.virtual.MapValue

class CypherExecutableQueryTest extends CypherFunSuite {

  test("should report both planning and execution plan deprecation notifications") {

    val planningNotifications: Vector[InternalNotification] = Vector(
      DeprecatedRuntimeNotification("msg", "old", "new"),
      CartesianProductNotification(InputPosition.NONE, Set("x", "y"), "")
    )

    val executionPlanNotifications: Set[InternalNotification] = Set(
      DeprecatedRelTypeSeparatorNotification(InputPosition.NONE, "old", "rewritten"),
      DeprecatedTextIndexProvider(InputPosition.NONE),
      DeprecatedPropertyReferenceInMerge(InputPosition.NONE, "prop"),
      UnboundedShortestPathNotification(InputPosition.NONE, "")
    )

    val executionPlan = mock[ExecutionPlan]
    when(executionPlan.notifications).thenReturn(executionPlanNotifications)
    when(executionPlan.rewrittenPlan).thenReturn(None)

    val query = new CypherExecutableQuery(
      // relevant fields
      executionPlan = executionPlan,
      planningNotifications = planningNotifications,

      // the rest
      logicalPlan = mock[LogicalPlan],
      readOnly = true,
      effectiveCardinalities = mock[PlanningAttributes.EffectiveCardinalities],
      rawCardinalitiesInPlanDescription = false,
      distinctnessInPlanDescription = false,
      providedOrders = mock[PlanningAttributes.ProvidedOrders],
      reusabilityState = FineToReuse,
      paramNames = Array.empty,
      extractedParams = MapValue.EMPTY,
      compilerInfo = mock[CompilerInfo],
      plannerName = mock[PlannerName],
      internalQueryType = READ_ONLY,
      shouldBeCached = true,
      enableMonitors = false,
      queryObfuscator = QueryObfuscator.PASSTHROUGH,
      renderPlanDescription = false,
      kernelMonitors = mock[Monitors]
    )

    val provider = query.deprecationNotificationsProvider(InputPosition.NONE)

    val builder = Seq.newBuilder[String]
    provider.forEachDeprecation((name, _) => builder += name)

    builder.result() should contain theSameElementsAs Seq(
      "DeprecatedRuntimeNotification",
      "DeprecatedRelTypeSeparatorNotification",
      "DeprecatedTextIndexProvider",
      "DeprecatedPropertyReferenceInMerge"
    )
  }
}
