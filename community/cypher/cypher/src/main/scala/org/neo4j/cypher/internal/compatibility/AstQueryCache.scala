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
package org.neo4j.cypher.internal.compatibility

import java.time.Clock

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.executionplan.ExecutionPlan
import org.neo4j.cypher.internal.compiler.v3_5.StatsDivergenceCalculator
import org.neo4j.cypher.internal.{CacheTracer, PlanStalenessCaller, QueryCache}

class AstQueryCache[STATEMENT <: AnyRef](override val maximumSize: Int,
                                         override val tracer: CacheTracer[STATEMENT],
                                         clock: Clock,
                                         divergence: StatsDivergenceCalculator,
                                         lastCommittedTxIdProvider: () => Long
) extends QueryCache[STATEMENT, ExecutionPlan](maximumSize,
                                               AstQueryCache.stalenessCaller(clock, divergence, lastCommittedTxIdProvider),
                                               tracer)

object AstQueryCache {
  def stalenessCaller(clock: Clock,
                      divergence: StatsDivergenceCalculator,
                      txIdProvider: () => Long): PlanStalenessCaller[ExecutionPlan] = {
    new PlanStalenessCaller[ExecutionPlan](clock, divergence, txIdProvider, (plan, _) => plan.reusability)
  }
}
