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
package org.neo4j.cypher.internal.compatibility

import java.time.Clock

import org.neo4j.cypher.internal.QueryCache.ParameterTypeMap
import org.neo4j.cypher.internal._
import org.neo4j.cypher.internal.compiler.v3_5.StatsDivergenceCalculator
import org.neo4j.cypher.internal.compiler.v3_5.phases.LogicalPlanState
import org.neo4j.helpers.collection.Pair
import org.neo4j.cypher.internal.v3_5.util.InternalNotification

/**
  * Cache which stores logical plans indexed by an AST statement.
  *
  * @param maximumSize Maximum size of this cache
  * @param tracer Traces cache activity
  * @param clock Clock used to compute logical plan staleness
  * @param divergence Statistics divergence calculator used to compute logical plan staleness
  * @param lastCommittedTxIdProvider Transation id provider used to compute logical plan staleness
  * @tparam STATEMENT Type of AST statement used as key
  */
class AstLogicalPlanCache[STATEMENT <: AnyRef](override val maximumSize: Int,
                                               override val tracer: CacheTracer[Pair[STATEMENT, ParameterTypeMap]],
                                               clock: Clock,
                                               divergence: StatsDivergenceCalculator,
                                               lastCommittedTxIdProvider: () => Long
) extends QueryCache[STATEMENT,Pair[STATEMENT,ParameterTypeMap], CacheableLogicalPlan](maximumSize,
                                                  AstLogicalPlanCache.stalenessCaller(clock, divergence, lastCommittedTxIdProvider),
                                                  tracer)
object AstLogicalPlanCache {
  def stalenessCaller(clock: Clock,
                      divergence: StatsDivergenceCalculator,
                      txIdProvider: () => Long): PlanStalenessCaller[CacheableLogicalPlan] = {
    new PlanStalenessCaller[CacheableLogicalPlan](clock, divergence, txIdProvider, (state, _) => state.reusability)
  }
}

case class CacheableLogicalPlan(logicalPlanState: LogicalPlanState,
                                reusability: ReusabilityState, notifications: Set[InternalNotification])
