/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.steps.solveOptionalMatches.OptionalSolver
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.frontend.v3_4.ast.UsingJoinHint
import org.neo4j.cypher.internal.ir.v3_4.QueryGraph
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.{Cardinalities, Solveds}
import org.neo4j.cypher.internal.v3_4.logical.plans.LogicalPlan

object solveOptionalMatches {
  type OptionalSolver = ((QueryGraph, LogicalPlan, LogicalPlanningContext, Solveds, Cardinalities) => Option[LogicalPlan])
}

case object applyOptional extends OptionalSolver {
  override def apply(optionalQg: QueryGraph, lhs: LogicalPlan, context: LogicalPlanningContext, solveds: Solveds, cardinalities: Cardinalities) = {
    val innerContext: LogicalPlanningContext = context.withUpdatedCardinalityInformation(lhs, solveds, cardinalities)
    val inner = context.strategy.plan(optionalQg, innerContext, solveds, cardinalities)
    val rhs = context.logicalPlanProducer.planOptional(inner, lhs.availableSymbols, innerContext)
    Some(context.logicalPlanProducer.planApply(lhs, rhs, context))
  }
}

abstract class outerHashJoin extends OptionalSolver {
  override def apply(optionalQg: QueryGraph, side1: LogicalPlan, context: LogicalPlanningContext, solveds: Solveds, cardinalities: Cardinalities) = {
    val joinNodes = optionalQg.argumentIds
    val hintsToIgnore = optionalQg.hints.filter { hint =>
      val hintDependencies = joinNodes -- hint.variables.map(_.name).toSet
      hintDependencies.isEmpty
    }
    val side2 = context.strategy.plan(optionalQg.withoutArguments().withoutHints(hintsToIgnore), context, solveds, cardinalities)

    if (joinNodes.nonEmpty &&
      joinNodes.forall(side1.availableSymbols) &&
      joinNodes.forall(optionalQg.patternNodes)) {
      val solvedHints = optionalQg.joinHints.filter { hint =>
        joinNodes == hint.variables.map(_.name).toSet
      }

      Some(produceJoin(context, joinNodes, side1, side2, solvedHints))
    } else {
      None
    }
  }

  def produceJoin(context: LogicalPlanningContext, joinNodes: Set[String], side1: LogicalPlan, side2: LogicalPlan, solvedHints: Set[UsingJoinHint]): LogicalPlan
}

case object leftOuterHashJoin extends outerHashJoin {
  override def produceJoin(context: LogicalPlanningContext, joinNodes: Set[String], lhs: LogicalPlan, rhs: LogicalPlan, solvedHints: Set[UsingJoinHint]) = {
    context.logicalPlanProducer.planLeftOuterHashJoin(joinNodes, lhs, rhs, solvedHints, context)
  }
}

case object rightOuterHashJoin extends outerHashJoin {
  override def produceJoin(context: LogicalPlanningContext, joinNodes: Set[String], rhs: LogicalPlan, lhs: LogicalPlan, solvedHints: Set[UsingJoinHint]) = {
    context.logicalPlanProducer.planRightOuterHashJoin(joinNodes, lhs, rhs, solvedHints, context)
  }
}
