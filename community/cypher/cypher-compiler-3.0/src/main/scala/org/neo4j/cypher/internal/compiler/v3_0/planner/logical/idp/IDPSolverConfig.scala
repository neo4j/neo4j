/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v3_0.planner.logical.idp

import org.neo4j.cypher.internal.compiler.v3_0.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans.{PatternRelationship, LogicalPlan}

/**
 * The IDP inner loop can be optimized and tweaked in several ways, and this trait encapsulates those settings
 */
trait IDPSolverConfig {
  def maxTableSize: Int = 128
  def iterationDurationLimit: Long = 1000
  def solvers(queryGraph: QueryGraph): Seq[QueryGraph => IDPSolverStep[PatternRelationship, LogicalPlan, LogicalPlanningContext]]
}

/* The Dynamic Programming (DP) approach is IDP with no optimizations */
case object DPSolverConfig extends IDPSolverConfig {
  override def maxTableSize = Integer.MAX_VALUE
  override def iterationDurationLimit = Long.MaxValue
  override def solvers(queryGraph: QueryGraph) = Seq(joinSolverStep(_), expandSolverStep(_))
}

/* The default settings for IDP uses a maxTableSize and a inner loop duration threshold
   to improve planning performance with minimal impact of plan quality */
case object DefaultIDPSolverConfig extends IDPSolverConfig {
  override def solvers(queryGraph: QueryGraph) = Seq(joinSolverStep(_), expandSolverStep(_))
}

/* The default settings for IDP uses a maxTableSize and a inner loop duration threshold
   to improve planning performance with minimal impact of plan quality */
case class ConfigurableIDPSolverConfig(override val maxTableSize: Int, iterationDuration: Long) extends IDPSolverConfig {
  override def iterationDurationLimit = iterationDuration
  override def solvers(queryGraph: QueryGraph) = Seq(joinSolverStep(_), expandSolverStep(_))
}

/* For testing IDP we sometimes limit the solver to expands only */
case object ExpandOnlyIDPSolverConfig extends IDPSolverConfig {
  override def solvers(queryGraph: QueryGraph) = Seq(expandSolverStep(_))
}

/* For testing IDP we sometimes limit the solver to joins only */
case object JoinOnlyIDPSolverConfig extends IDPSolverConfig {
  override def solvers(queryGraph: QueryGraph) = Seq(joinSolverStep(_))
}

/* One approach to optimizing planning of very long patterns is to only allow expands.
   This making planning very fast, but can lead to non-optimal plans where joins make more sense */
case object ExpandOnlyWhenPatternIsLong extends IDPSolverConfig {
  override def solvers(queryGraph: QueryGraph) =
    if(queryGraph.patternRelationships.size > 10) Seq(expandSolverStep(_))
    else Seq(joinSolverStep(_), expandSolverStep(_))
}

/* One more advanced approach is to allow the inner loop to automatically switch from
   expands-only to expands and joins when the problem size becomes smaller.
   This is a good compromise between performance and quality, however it is not clear
   where best to draw the line, and it is probable that the joins might be planned in
   sub-optimal positions. We should consider this for a future default once we have the
   time to develop more confidence in the approach. */
case class AdaptiveChainPatternConfig(patternLengthThreshold: Int) extends IDPSolverConfig {
  override def solvers(queryGraph: QueryGraph) =
    Seq(AdaptiveSolverStep(_, patternLengthThreshold))
}
