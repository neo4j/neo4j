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
package org.neo4j.cypher.internal.plandescription

import org.neo4j.cypher.CypherVersion
import org.neo4j.cypher.internal.RuntimeName
import org.neo4j.cypher.internal.plandescription.Arguments.{Runtime, RuntimeImpl}
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.{Cardinalities, ProvidedOrders}
import org.neo4j.cypher.internal.v4_0.frontend.PlannerName
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.result.{OperatorProfile, QueryProfile}

class PlanDescriptionBuilder(logicalPlan: LogicalPlan,
                             plannerName: PlannerName,
                             cypherVersion: CypherVersion,
                             readOnly: Boolean,
                             cardinalities: Cardinalities,
                             providedOrders: ProvidedOrders,
                             runtimeName: RuntimeName,
                             metadata: Seq[Argument]) {

  def explain(): InternalPlanDescription = {
    val description =
      LogicalPlan2PlanDescription(logicalPlan, plannerName, cypherVersion, readOnly, cardinalities, providedOrders)
        .addArgument(Runtime(runtimeName.toTextOutput))
        .addArgument(RuntimeImpl(runtimeName.name))

    metadata.foldLeft(description)((plan, metadata) => plan.addArgument(metadata))
  }

  def profile(queryProfile: QueryProfile): InternalPlanDescription = {

    val planDescription = explain()

    planDescription map {
      input: InternalPlanDescription =>
        val data = queryProfile.operatorProfile(input.id.x)

        BuildPlanDescription(input)
          .addArgument(Arguments.Rows, data.rows)
          .addArgument(Arguments.DbHits, data.dbHits)
          .addArgument(Arguments.PageCacheHits, data.pageCacheHits)
          .addArgument(Arguments.PageCacheMisses, data.pageCacheMisses)
          .addArgument(Arguments.PageCacheHitRatio, data.pageCacheHitRatio())
          .addArgument(Arguments.Time, data.time())
        .plan
    }
  }

  case class BuildPlanDescription(plan: InternalPlanDescription) {

    def addArgument[T](argument: T => Argument,
                       value: T): BuildPlanDescription =
      if (value == OperatorProfile.NO_DATA) this
      else BuildPlanDescription(plan.addArgument(argument(value)))
  }
}
