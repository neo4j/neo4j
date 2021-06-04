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
package org.neo4j.cypher.internal.plandescription

import org.neo4j.cypher.CypherVersion
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.RuntimeName
import org.neo4j.cypher.internal.frontend.PlannerName
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.plandescription.Arguments.Runtime
import org.neo4j.cypher.internal.plandescription.Arguments.RuntimeImpl
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.result.OperatorProfile
import org.neo4j.cypher.result.QueryProfile

object PlanDescriptionBuilder {
  def apply(logicalPlan: LogicalPlan,
            plannerName: PlannerName,
            cypherVersion: CypherVersion,
            readOnly: Boolean,
            cardinalities: Cardinalities,
            providedOrders: ProvidedOrders,
            executionPlan: ExecutionPlan): PlanDescriptionBuilder = {
    // NOTE: We should not keep a reference to the ExecutionPlan in the PlanDescriptionBuilder since it can end up in long-lived caches, e.g. RecentQueryBuffer
    val runtimeName = executionPlan.runtimeName
    val runtimeMetadata = executionPlan.metadata
    val runtimeOperatorMetadata = executionPlan.operatorMetadata

    new PlanDescriptionBuilder(logicalPlan: LogicalPlan,
                               plannerName: PlannerName,
                               cypherVersion: CypherVersion,
                               readOnly: Boolean,
                               cardinalities: Cardinalities,
                               providedOrders: ProvidedOrders,
                               runtimeName,
                               runtimeMetadata,
                               runtimeOperatorMetadata)
  }
}

class PlanDescriptionBuilder(logicalPlan: LogicalPlan,
                             plannerName: PlannerName,
                             cypherVersion: CypherVersion,
                             readOnly: Boolean,
                             cardinalities: Cardinalities,
                             providedOrders: ProvidedOrders,
                             runtimeName: RuntimeName,
                             runtimeMetadata: Seq[Argument],
                             runtimeOperatorMetadata: Id => Seq[Argument]) {

  def explain(): InternalPlanDescription = {
    val description =
      LogicalPlan2PlanDescription
        .create(logicalPlan, plannerName, cypherVersion, readOnly, cardinalities, providedOrders, runtimeOperatorMetadata)
        .addArgument(Runtime(runtimeName.toTextOutput))
        .addArgument(RuntimeImpl(runtimeName.name))

    runtimeMetadata.foldLeft(description)((plan, metadata) => plan.addArgument(metadata))
  }

  def profile(queryProfile: QueryProfile): InternalPlanDescription = {

    val planDescription = BuildPlanDescription(explain())
        .addArgument(Arguments.GlobalMemory, queryProfile.maxAllocatedMemory())
        .plan

    planDescription map {
      input: InternalPlanDescription =>
        val data = queryProfile.operatorProfile(input.id.x)

        BuildPlanDescription(input)
          .addArgument(Arguments.Rows, data.rows)
          .addArgument(Arguments.DbHits, data.dbHits)
          .addArgument(Arguments.PageCacheHits, data.pageCacheHits)
          .addArgument(Arguments.PageCacheMisses, data.pageCacheMisses)
          .addArgument(Arguments.Time, data.time())
          .addArgument(Arguments.Memory, data.maxAllocatedMemory())
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
