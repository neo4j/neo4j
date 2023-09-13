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
package org.neo4j.cypher.internal.compiler.planner.logical.idp

import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.ir.NodeConnection
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

/**
 * The IDP inner loop can be optimized and tweaked in several ways, and this trait encapsulates those settings
 */
trait IDPSolverConfig {
  def maxTableSize: Int = 128
  def iterationDurationLimit: Long = 1000
}

trait SingleComponentIDPSolverConfig extends IDPSolverConfig {

  def solvers(qppInnerPlanner: QPPInnerPlanner)
    : Seq[QueryGraph => IDPSolverStep[NodeConnection, LogicalPlan, LogicalPlanningContext]]
}

/* The default settings for IDP uses a maxTableSize and a inner loop duration threshold
   to improve planning performance with minimal impact of plan quality */
case object DefaultIDPSolverConfig extends SingleComponentIDPSolverConfig {

  override def solvers(qppInnerPlanner: QPPInnerPlanner)
    : Seq[QueryGraph => IDPSolverStep[NodeConnection, LogicalPlan, LogicalPlanningContext]] =
    Seq(joinSolverStep(_), expandSolverStep(_, qppInnerPlanner))
}

/* The Dynamic Programming (DP) approach is IDP with no optimizations */
case object DPSolverConfig extends SingleComponentIDPSolverConfig {
  override def maxTableSize: Int = Integer.MAX_VALUE
  override def iterationDurationLimit: Long = Long.MaxValue

  override def solvers(qppInnerPlanner: QPPInnerPlanner)
    : Seq[QueryGraph => IDPSolverStep[NodeConnection, LogicalPlan, LogicalPlanningContext]] =
    Seq(joinSolverStep(_), expandSolverStep(_, qppInnerPlanner))
}

/* The default settings for IDP uses a maxTableSize and a inner loop duration threshold
   to improve planning performance with minimal impact of plan quality */
class ConfigurableIDPSolverConfig(override val maxTableSize: Int, override val iterationDurationLimit: Long)
    extends SingleComponentIDPSolverConfig {

  override def solvers(qppInnerPlanner: QPPInnerPlanner)
    : Seq[QueryGraph => IDPSolverStep[NodeConnection, LogicalPlan, LogicalPlanningContext]] =
    Seq(joinSolverStep(_), expandSolverStep(_, qppInnerPlanner))

  override def toString: String =
    s"${this.getClass.getSimpleName}(maxTableSize = $maxTableSize, iterationDurationLimit = $iterationDurationLimit})"
}

/* For testing IDP we sometimes limit the solver to expands only */
case object ExpandOnlyIDPSolverConfig extends ConfigurableIDPSolverConfig(256, Long.MaxValue) {

  override def solvers(qppInnerPlanner: QPPInnerPlanner)
    : Seq[QueryGraph => IDPSolverStep[NodeConnection, LogicalPlan, LogicalPlanningContext]] =
    Seq(expandSolverStep(_, qppInnerPlanner))
}

/* For testing IDP we sometimes limit the solver to joins only */
case object JoinOnlyIDPSolverConfig extends ConfigurableIDPSolverConfig(256, Long.MaxValue) {

  override def solvers(qppInnerPlanner: QPPInnerPlanner)
    : Seq[QueryGraph => IDPSolverStep[NodeConnection, LogicalPlan, LogicalPlanningContext]] = Seq(joinSolverStep(_))
}
