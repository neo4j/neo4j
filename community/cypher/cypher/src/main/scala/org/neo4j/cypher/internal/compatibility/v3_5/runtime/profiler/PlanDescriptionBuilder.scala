/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.compatibility.v3_5.runtime.profiler

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.RuntimeName
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription.Arguments
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription.Arguments.{Runtime, RuntimeImpl}
import org.neo4j.cypher.internal.runtime.planDescription.{InternalPlanDescription, LogicalPlan2PlanDescription}
import org.neo4j.cypher.internal.v3_5.logical.plans.LogicalPlan
import org.neo4j.cypher.result.QueryProfile
import org.opencypher.v9_0.frontend.PlannerName

class PlanDescriptionBuilder(logicalPlan: LogicalPlan,
                             plannerName: PlannerName,
                             readOnly: Boolean,
                             cardinalities: Cardinalities,
                             runtimeName: RuntimeName) {

  def explain(): InternalPlanDescription =
    LogicalPlan2PlanDescription(logicalPlan, plannerName, readOnly, cardinalities)
      .addArgument(Runtime(runtimeName.toTextOutput))
      .addArgument(RuntimeImpl(runtimeName.name))

  def profile(queryProfile: QueryProfile): InternalPlanDescription = {

    val planDescription = explain()

    planDescription map {
      input: InternalPlanDescription =>
        val data = queryProfile.operatorProfile(input.id.x)

        input
          .addArgument(Arguments.Rows(data.rows))
          .addArgument(Arguments.DbHits(data.dbHits))
          .addArgument(Arguments.PageCacheHits(data.pageCacheHits))
          .addArgument(Arguments.PageCacheMisses(data.pageCacheMisses))
          .addArgument(Arguments.PageCacheHitRatio(data.pageCacheHitRatio()))
          .addArgument(Arguments.Time(data.time()))
    }
  }
}
