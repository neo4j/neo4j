/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical.idp


import org.neo4j.cypher.internal.compiler.v3_4.helpers.IteratorSupport._
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.LogicalPlanningSupport._
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical._
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.idp.SingleComponentPlanner.planSinglePattern
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.idp.expandSolverStep.{planSinglePatternSide, planSingleProjectEndpoints}
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.steps.leafPlanOptions
import org.neo4j.cypher.internal.frontend.v3_4.ast.RelationshipStartItem
import org.neo4j.cypher.internal.ir.v3_4.{PatternRelationship, QueryGraph}
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.{Cardinalities, Solveds}
import org.neo4j.cypher.internal.util.v3_4.InternalException
import org.neo4j.cypher.internal.v3_4.logical.plans.LogicalPlan

/**
  * This class contains the main IDP loop in the cost planner.
  * This planner is based on the paper
  *
  * "Iterative Dynamic Programming: A New Class of Query Optimization Algorithms"
  *
  * written by Donald Kossmann and Konrad Stocker
  */
case class SingleComponentPlanner(monitor: IDPQueryGraphSolverMonitor,
                                  solverConfig: IDPSolverConfig = DefaultIDPSolverConfig,
                                  leafPlanFinder: LeafPlanFinder = leafPlanOptions) extends SingleComponentPlannerTrait {
  def planComponent(qg: QueryGraph, context: LogicalPlanningContext, solveds: Solveds, cardinalities: Cardinalities, kit: QueryPlannerKit): LogicalPlan = {
    val leaves = leafPlanFinder(context.config, qg, context, solveds, cardinalities)

    val bestPlan =
      if (qg.patternRelationships.nonEmpty) {

        val generators = solverConfig.solvers(qg).map(_ (qg))
        val selectingGenerators = generators.map(_.map(plan => kit.select(plan, qg)))
        val generator = selectingGenerators.foldLeft(IDPSolverStep.empty[PatternRelationship, LogicalPlan, LogicalPlanningContext])(_ ++ _)

        val solver = new IDPSolver[PatternRelationship, LogicalPlan, LogicalPlanningContext](
          generator = generator,
          projectingSelector = kit.pickBest,
          maxTableSize = solverConfig.maxTableSize,
          iterationDurationLimit = solverConfig.iterationDurationLimit,
          monitor = monitor
        )

        monitor.initTableFor(qg)
        val seed = initTable(qg, kit, leaves, context, solveds)
        monitor.startIDPIterationFor(qg)
        val solutions = solver(seed, qg.patternRelationships, context, solveds)
        val (_, result) = solutions.toSingleOption.getOrElse(throw new AssertionError("Expected a single plan to be left in the plan table"))
        monitor.endIDPIterationFor(qg, result)

        result
      } else {
        val solutionPlans = if (qg.shortestPathPatterns.isEmpty)
          leaves collect {
            case plan if planFullyCoversQG(qg, plan) => kit.select(plan, qg)
          }
        else leaves map (kit.select(_, qg)) filter (planFullyCoversQG(qg, _))
        val result = kit.pickBest(solutionPlans).getOrElse(throw new InternalException("Found no leaf plan for connected component. This must not happen. QG: " + qg))
        monitor.noIDPIterationFor(qg, result)
        result
      }

    if (IDPQueryGraphSolver.VERBOSE)
      println(s"Result (picked best plan):\n\tPlan #${bestPlan.debugId}\n\t${bestPlan.toString}\n\n")

    bestPlan
  }

  private def planFullyCoversQG(qg: QueryGraph, plan: LogicalPlan) =
    (qg.idsWithoutOptionalMatchesOrUpdates -- plan.availableSymbols -- qg.argumentIds).isEmpty

  private def initTable(qg: QueryGraph, kit: QueryPlannerKit, leaves: Set[LogicalPlan], context: LogicalPlanningContext, solveds: Solveds) = {
    for (pattern <- qg.patternRelationships)
      yield {
        val plans = planSinglePattern(qg, pattern, leaves, context, solveds).map(plan => kit.select(plan, qg))
        val bestAccessor = kit.pickBest(plans).getOrElse(
          throw new InternalException("Found no access plan for a pattern relationship in a connected component. This must not happen."))
        Set(pattern) -> bestAccessor
      }
  }
}

trait SingleComponentPlannerTrait {
  def planComponent(qg: QueryGraph, context: LogicalPlanningContext, solveds: Solveds, cardinalities: Cardinalities, kit: QueryPlannerKit): LogicalPlan
}


object SingleComponentPlanner {
  def DEFAULT_SOLVERS: Seq[(QueryGraph) => IDPSolverStep[PatternRelationship, LogicalPlan, LogicalPlanningContext]] =
    Seq(joinSolverStep(_), expandSolverStep(_))

  def planSinglePattern(qg: QueryGraph, pattern: PatternRelationship, leaves: Set[LogicalPlan], context: LogicalPlanningContext, solveds: Solveds): Iterable[LogicalPlan] = {
    leaves.flatMap {
      case plan if solveds.get(plan.id).lastQueryGraph.patternRelationships.contains(pattern) =>
        Set(plan)
      case plan if solveds.get(plan.id).lastQueryGraph.allCoveredIds.contains(pattern.name) =>
        Set(planSingleProjectEndpoints(pattern, plan, context))
      case plan if solveds.get(plan.id).lastQueryGraph.patternNodes.isEmpty && solveds.get(plan.id).lastQueryGraph.hints.exists(_.isInstanceOf[RelationshipStartItem]) =>
        Set(context.logicalPlanProducer.planEndpointProjection(plan, pattern.nodes._1, startInScope = false, pattern.nodes._2, endInScope = false, pattern, context))
      case plan =>
        val (start, end) = pattern.nodes
        val leftExpand = planSinglePatternSide(qg, pattern, plan, start, context)
        val rightExpand = planSinglePatternSide(qg, pattern, plan, end, context)

        val startJoinNodes = Set(start)
        val endJoinNodes = Set(end)
        val maybeStartPlan = leaves.find(_.availableSymbols == startJoinNodes)
        val maybeEndPlan = leaves.find(_.availableSymbols == endJoinNodes)
        val cartesianProduct = planSinglePatternCartesian(qg, pattern, start, maybeStartPlan, maybeEndPlan, context)
        val joins = planSinglePatternJoins(qg, leftExpand, rightExpand, startJoinNodes, endJoinNodes, maybeStartPlan, maybeEndPlan, context)
        leftExpand ++ rightExpand ++ cartesianProduct ++ joins
    }
  }

  def planSinglePatternCartesian(qg: QueryGraph,
                                 pattern: PatternRelationship,
                                 start: String,
                                 maybeStartPlan: Option[LogicalPlan],
                                 maybeEndPlan: Option[LogicalPlan],
                                 context: LogicalPlanningContext): Option[LogicalPlan] = (maybeStartPlan, maybeEndPlan) match {
    case (Some(startPlan), Some(endPlan)) =>
      planSinglePatternSide(qg, pattern, context.logicalPlanProducer.planCartesianProduct(startPlan, endPlan, context), start, context)
    case _ => None
  }

  /*
  If there are hints and the query graph is small, joins have to be constructed as an alternative here, otherwise the hints might not be able to be fulfilled.
  Creating joins if the query graph is larger will lead to too many joins.
   */
  def planSinglePatternJoins(qg: QueryGraph, leftExpand: Option[LogicalPlan], rightExpand: Option[LogicalPlan], startJoinNodes: Set[String],
                             endJoinNodes: Set[String], maybeStartPlan: Option[LogicalPlan], maybeEndPlan: Option[LogicalPlan], context: LogicalPlanningContext): Iterable[LogicalPlan] = (maybeStartPlan, maybeEndPlan) match {
    case (Some(startPlan), Some(endPlan)) if qg.hints.nonEmpty && qg.size == 1 =>
      val startJoinHints = qg.joinHints.filter(_.coveredBy(startJoinNodes))
      val endJoinHints = qg.joinHints.filter(_.coveredBy(endJoinNodes))
      val join1a = leftExpand.map(expand => context.logicalPlanProducer.planNodeHashJoin(endJoinNodes, expand, endPlan, endJoinHints, context))
      val join1b = leftExpand.map(expand => context.logicalPlanProducer.planNodeHashJoin(endJoinNodes, endPlan, expand, endJoinHints, context))
      val join2a = rightExpand.map(expand => context.logicalPlanProducer.planNodeHashJoin(startJoinNodes, startPlan, expand, startJoinHints, context))
      val join2b = rightExpand.map(expand => context.logicalPlanProducer.planNodeHashJoin(startJoinNodes, expand, startPlan, startJoinHints, context))
      join1a ++ join1b ++ join2a ++ join2b
    case _ => None
  }
}
