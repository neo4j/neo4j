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
package org.neo4j.cypher.internal.compiler.planner.logical.idp


import org.neo4j.cypher.internal.compiler.helpers.IteratorSupport._
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningSupport._
import org.neo4j.cypher.internal.compiler.planner.logical._
import org.neo4j.cypher.internal.compiler.planner.logical.idp.SingleComponentPlanner.planSinglePattern
import org.neo4j.cypher.internal.compiler.planner.logical.idp.expandSolverStep.{planSinglePatternSide, planSingleProjectEndpoints}
import org.neo4j.cypher.internal.compiler.planner.logical.steps.leafPlanOptions
import org.neo4j.cypher.internal.ir.InterestingOrder.FullSatisfaction
import org.neo4j.cypher.internal.ir.{InterestingOrder, PatternRelationship, QueryGraph}
import org.neo4j.cypher.internal.logical.plans.{Argument, LogicalPlan}
import org.neo4j.cypher.internal.v4_0.ast.RelationshipHint
import org.neo4j.exceptions.InternalException

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
  override def planComponent(qg: QueryGraph, context: LogicalPlanningContext, kit: QueryPlannerKit, interestingOrder: InterestingOrder): LogicalPlan = {
    val leavesWithoutSorting = leafPlanFinder(context.config, qg, interestingOrder, context)
    val leaves = if (interestingOrder.requiredOrderCandidate.nonEmpty)
      leavesWithoutSorting ++ leavesWithoutSorting.flatMap(plan => SortPlanner.maybeSortedPlan(plan, interestingOrder, context))
    else leavesWithoutSorting

    val bestPlan =
      if (qg.patternRelationships.nonEmpty) {

        val orderRequirement = new ExtraRequirement[InterestingOrder, LogicalPlan]() {
          override def none: InterestingOrder = InterestingOrder.empty

          override def forResult(plan: LogicalPlan): InterestingOrder = {
            SortPlanner.orderSatisfaction(interestingOrder, context, plan) match {
              case FullSatisfaction() => interestingOrder
              case _ => InterestingOrder.empty
            }
          }

          override def is(requirement: InterestingOrder): Boolean = requirement == interestingOrder
        }

        val generators = solverConfig.solvers(qg).map(_ (qg))
        val selectingGenerators = generators.map(_.map(plan => kit.select(plan, qg)))
        val combinedGenerators = if (interestingOrder.isEmpty) selectingGenerators else {
          val sortingGenerators = selectingGenerators.map(_.flatMap(plan => SortPlanner.maybeSortedPlan(plan, interestingOrder, context)))
          selectingGenerators ++ sortingGenerators
        }
        val generator = combinedGenerators.foldLeft(IDPSolverStep.empty[PatternRelationship, InterestingOrder, LogicalPlan, LogicalPlanningContext])(_ ++ _)

        val solver = new IDPSolver[PatternRelationship, InterestingOrder, LogicalPlan, LogicalPlanningContext](
          generator = generator,
          projectingSelector = kit.pickBest,
          maxTableSize = solverConfig.maxTableSize,
          iterationDurationLimit = solverConfig.iterationDurationLimit,
          extraRequirement = orderRequirement,
          monitor = monitor
        )

        monitor.initTableFor(qg)
        val seed = initTable(qg, kit, leaves, context, interestingOrder)
        monitor.startIDPIterationFor(qg)
        val solutions = solver(seed, qg.patternRelationships, context)
        val (_, result) = solutions.toSingleOption.getOrElse(throw new AssertionError("Expected a single plan to be left in the plan table"))
        monitor.endIDPIterationFor(qg, result)

        result
      } else {
        val solutionPlans = if (qg.shortestPathPatterns.isEmpty)
          leaves collect {
            case plan if planFullyCoversQG(qg, plan, interestingOrder, context) => kit.select(plan, qg)
          }
        else leaves map (kit.select(_, qg)) filter (planFullyCoversQG(qg, _, interestingOrder, context))
        val result = kit.pickBest(solutionPlans).getOrElse(throw new InternalException("Found no leaf plan for connected component. This must not happen. QG: " + qg))
        monitor.noIDPIterationFor(qg, result)
        result
      }

    if (IDPQueryGraphSolver.VERBOSE)
      println(s"Result (picked best plan):\n\tPlan #${bestPlan.debugId}\n\t${bestPlan.toString}\n\n")

    bestPlan
  }

  private def planFullyCoversQG(qg: QueryGraph, plan: LogicalPlan, interestingOrder: InterestingOrder, context: LogicalPlanningContext) =
    (qg.idsWithoutOptionalMatchesOrUpdates -- plan.availableSymbols -- qg.argumentIds).isEmpty

  private def initTable(qg: QueryGraph, kit: QueryPlannerKit, leaves: Set[LogicalPlan], context: LogicalPlanningContext, interestingOrder: InterestingOrder): Set[((Set[PatternRelationship], InterestingOrder), LogicalPlan)] = {
    for (pattern <- qg.patternRelationships)
      yield {
        val plans = planSinglePattern(qg, pattern, leaves, interestingOrder, context).map(plan => kit.select(plan, qg))
        val best = kit.pickBest(plans).map(p => ((Set(pattern), InterestingOrder.empty), p))

        val result: Iterable[((Set[PatternRelationship], InterestingOrder), LogicalPlan)] =
          if (interestingOrder.isEmpty) {
            best
          } else {
            val orderedPlans = plans.flatMap(plan => SortPlanner.maybeSortedPlan(plan, interestingOrder, context))
            val ordered = (plans ++ orderedPlans).filter{ plan =>
              SortPlanner.orderSatisfaction(interestingOrder, context, plan) match {
                case FullSatisfaction() => true
                case _ => false
              }
            }
            val bestWithSort = kit.pickBest(ordered).map(p => ((Set(pattern), interestingOrder), p))
            best ++ bestWithSort
          }

        if (result.isEmpty)
          throw new InternalException("Found no access plan for a pattern relationship in a connected component. This must not happen.")

        result
      }
  }.flatten
}

trait SingleComponentPlannerTrait {
  def planComponent(qg: QueryGraph, context: LogicalPlanningContext, kit: QueryPlannerKit, interestingOrder: InterestingOrder): LogicalPlan
}


object SingleComponentPlanner {
  def DEFAULT_SOLVERS: Seq[QueryGraph => IDPSolverStep[PatternRelationship, InterestingOrder, LogicalPlan, LogicalPlanningContext]] =
    Seq(joinSolverStep(_), expandSolverStep(_))

  def planSinglePattern(qg: QueryGraph,
                        pattern: PatternRelationship,
                        leaves: Set[LogicalPlan],
                        interestingOrder: InterestingOrder,
                        context: LogicalPlanningContext): Iterable[LogicalPlan] = {
    val solveds = context.planningAttributes.solveds
    leaves.flatMap {
      case plan if solveds.get(plan.id).asSinglePlannerQuery.lastQueryGraph.patternRelationships.contains(pattern) =>
        Set(plan)
      case plan if solveds.get(plan.id).asSinglePlannerQuery.lastQueryGraph.allCoveredIds.contains(pattern.name) =>
        Set(planSingleProjectEndpoints(pattern, plan, context))
      case plan if solveds.get(plan.id).asSinglePlannerQuery.lastQueryGraph.patternNodes.isEmpty && solveds.get(plan.id).asSinglePlannerQuery.lastQueryGraph.hints.exists(_.isInstanceOf[RelationshipHint]) =>
        Set(context.logicalPlanProducer.planEndpointProjection(plan, pattern.nodes._1, startInScope = false, pattern.nodes._2, endInScope = false, pattern, context))
      case plan =>
        val (start, end) = pattern.nodes
        val leftExpand = planSinglePatternSide(qg, pattern, plan, start, interestingOrder, context)
        val rightExpand = planSinglePatternSide(qg, pattern, plan, end, interestingOrder, context)

        val startJoinNodes = Set(start)
        val endJoinNodes = Set(end)
        val maybeStartPlan = leaves.find(leaf => solveds(leaf.id).asSinglePlannerQuery.queryGraph.patternNodes == startJoinNodes && !leaf.isInstanceOf[Argument])
        val maybeEndPlan = leaves.find(leaf => solveds(leaf.id).asSinglePlannerQuery.queryGraph.patternNodes == endJoinNodes && !leaf.isInstanceOf[Argument])
        val cartesianProduct = planSinglePatternCartesian(qg, pattern, start, maybeStartPlan, maybeEndPlan, interestingOrder, context)
        val joins = planSinglePatternJoins(qg, leftExpand, rightExpand, startJoinNodes, endJoinNodes, maybeStartPlan, maybeEndPlan, context)
        leftExpand ++ rightExpand ++ cartesianProduct ++ joins
    }
  }

  def planSinglePatternCartesian(qg: QueryGraph,
                                 pattern: PatternRelationship,
                                 start: String,
                                 maybeStartPlan: Option[LogicalPlan],
                                 maybeEndPlan: Option[LogicalPlan],
                                 interestingOrder: InterestingOrder,
                                 context: LogicalPlanningContext): Option[LogicalPlan] = (maybeStartPlan, maybeEndPlan) match {
    case (Some(startPlan), Some(endPlan)) =>
      planSinglePatternSide(qg, pattern, context.logicalPlanProducer.planCartesianProduct(startPlan, endPlan, context), start, interestingOrder, context)
    case _ => None
  }

  /*
  If there are hints and the query graph is small, joins have to be constructed as an alternative here, otherwise the hints might not be able to be fulfilled.
  Creating joins if the query graph is larger will lead to too many joins.
   */
  def planSinglePatternJoins(qg: QueryGraph,
                             leftExpand: Option[LogicalPlan],
                             rightExpand: Option[LogicalPlan],
                             startJoinNodes: Set[String],
                             endJoinNodes: Set[String],
                             maybeStartPlan: Option[LogicalPlan],
                             maybeEndPlan: Option[LogicalPlan],
                             context: LogicalPlanningContext): Iterable[LogicalPlan] = (maybeStartPlan, maybeEndPlan) match {
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
