/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_2.planner.logical.idp


import org.neo4j.cypher.internal.compiler.v3_2.helpers.IteratorSupport._
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical._
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.idp.expandSolverStep.{planSinglePatternSide, planSingleProjectEndpoints}
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.steps.solveOptionalMatches.OptionalSolver
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.steps.{applyOptional, leafPlanOptions, outerHashJoin}
import org.neo4j.cypher.internal.frontend.v3_2.InternalException
import org.neo4j.cypher.internal.frontend.v3_2.ast.RelationshipStartItem
import org.neo4j.cypher.internal.ir.v3_2.{PatternRelationship, QueryGraph}

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
                                  leafPlanFinder: LeafPlanFinder = leafPlanOptions,
                                  optionalSolvers: Seq[OptionalSolver] = Seq(applyOptional, outerHashJoin)) extends SingleComponentPlannerTrait {
  def planComponent(qg: QueryGraph)(implicit context: LogicalPlanningContext, kit: QueryPlannerKit): LogicalPlan = {
    val leaves = leafPlanFinder(context.config, qg)

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
        val seed = initTable(qg, kit, leaves)
        monitor.startIDPIterationFor(qg)
        val solutions = solver(seed, qg.patternRelationships)
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
    (qg.coveredIds -- plan.availableSymbols -- qg.argumentIds).isEmpty

  private def initTable(qg: QueryGraph, kit: QueryPlannerKit, leaves: Set[LogicalPlan])(implicit context: LogicalPlanningContext) = {
    for (pattern <- qg.patternRelationships)
      yield {
        val accessPlans = planSinglePattern(qg, pattern, leaves).map(plan => kit.select(plan, qg))
        val bestAccessor = kit.pickBest(accessPlans).getOrElse(throw new InternalException("Found no access plan for a pattern relationship in a connected component. This must not happen."))
        Set(pattern) -> bestAccessor
      }
  }

  private def planSinglePattern(qg: QueryGraph, pattern: PatternRelationship, leaves: Set[LogicalPlan])
                               (implicit context: LogicalPlanningContext): Iterable[LogicalPlan] = {
    leaves.flatMap {
      case plan if plan.solved.lastQueryGraph.patternRelationships.contains(pattern) =>
        Set(plan)
      case plan if plan.solved.lastQueryGraph.allCoveredIds.contains(pattern.name) =>
        Set(planSingleProjectEndpoints(pattern, plan))
      case plan if plan.solved.lastQueryGraph.patternNodes.isEmpty && plan.solved.lastQueryGraph.hints.exists(_.isInstanceOf[RelationshipStartItem]) =>
        Set(context.logicalPlanProducer.planEndpointProjection(plan, pattern.nodes._1, startInScope = false, pattern.nodes._2, endInScope = false, pattern))
      case plan =>
        val (start, end) = pattern.nodes
        val leftExpand = planSinglePatternSide(qg, pattern, plan, start)
        val rightExpand = planSinglePatternSide(qg, pattern, plan, end)
        leftExpand.toSet ++ rightExpand.toSet
    }
  }
}

trait SingleComponentPlannerTrait {
  def planComponent(qg: QueryGraph)(implicit context: LogicalPlanningContext, kit: QueryPlannerKit): LogicalPlan
}


object SingleComponentPlanner {
  def DEFAULT_SOLVERS: Seq[(QueryGraph) => IDPSolverStep[PatternRelationship, LogicalPlan, LogicalPlanningContext]] =
    Seq(joinSolverStep(_), expandSolverStep(_))
}
