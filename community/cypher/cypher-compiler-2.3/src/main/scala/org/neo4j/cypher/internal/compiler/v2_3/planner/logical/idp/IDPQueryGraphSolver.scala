/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.idp

import org.neo4j.cypher.internal.compiler.v2_3.InternalException
import org.neo4j.cypher.internal.compiler.v2_3.helpers.IteratorSupport._
import org.neo4j.cypher.internal.compiler.v2_3.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.idp.expandSolverStep.{planSinglePatternSide, planSingleProjectEndpoints}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.steps.solveOptionalMatches.OptionalSolver
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.steps.{applyOptional, outerHashJoin}

import scala.annotation.tailrec

/**
 * This planner is based on the paper
 *
 *   "Iterative Dynamic Programming: A New Class of Query Optimization Algorithms"
 *
 * written by Donald Kossmann and Konrad Stocker
 */
case class IDPQueryGraphSolver(maxTableSize: Int = 256,
                               leafPlanFinder: LogicalLeafPlan.Finder = leafPlanOptions,
                               config: QueryPlannerConfiguration = QueryPlannerConfiguration.default,
                               solvers: Seq[QueryGraph => IDPSolverStep[PatternRelationship, LogicalPlan]] = Seq(joinSolverStep(_), expandSolverStep(_)),
                               optionalSolvers: Seq[OptionalSolver] = Seq(applyOptional, outerHashJoin))
  extends QueryGraphSolver with PatternExpressionSolving {

  def plan(queryGraph: QueryGraph)(implicit context: LogicalPlanningContext, leafPlan: Option[LogicalPlan]): LogicalPlan = {
    implicit val kitFactory = (qg: QueryGraph) => kitWithShortestPathSupport(config.toKit(qg))
    val components = queryGraph.connectedComponents
    val plans = if (components.isEmpty) planEmptyComponent(queryGraph) else planComponents(components)

    implicit val kit = kitFactory.apply(queryGraph)
    val plansWithRemainingOptionalMatches = plans.map { (plan: LogicalPlan) => (plan, queryGraph.optionalMatches) }
    val result = connectComponents(plansWithRemainingOptionalMatches)
    result
  }

  private def kitWithShortestPathSupport(kit: QueryPlannerKit)(implicit context: LogicalPlanningContext) =
    kit.copy(select = selectShortestPath(kit, _))

  private def selectShortestPath(kit: QueryPlannerKit, initialPlan: LogicalPlan)(implicit context: LogicalPlanningContext): LogicalPlan =
    kit.qg.shortestPathPatterns.foldLeft(kit.select(initialPlan)) {
      case (plan, sp) if sp.isFindableFrom(plan.availableSymbols) => kit.select(context.logicalPlanProducer.planShortestPaths(plan, sp))
      case (plan, _) => plan
    }

  private def planComponents(components: Seq[QueryGraph])(implicit context: LogicalPlanningContext, leafPlan: Option[LogicalPlan], kitFactory: (QueryGraph) => QueryPlannerKit): Seq[LogicalPlan] =
    components.map { qg =>
      implicit val kit = kitFactory(qg)
      planComponent(qg)
    }

  private def planEmptyComponent(queryGraph: QueryGraph)(implicit context: LogicalPlanningContext, leafPlan: Option[LogicalPlan], kitFactory: (QueryGraph) => QueryPlannerKit): Seq[LogicalPlan] = {
    val plan = if (queryGraph.argumentIds.isEmpty)
      context.logicalPlanProducer.planSingleRow()
    else
      context.logicalPlanProducer.planQueryArgumentRow(queryGraph)
    Seq(kitFactory(queryGraph).select(plan))
  }

  private def planComponent(qg: QueryGraph)(implicit context: LogicalPlanningContext, kit: QueryPlannerKit, leafPlanWeHopeToGetAwayWithIgnoring: Option[LogicalPlan]): LogicalPlan = {
    // TODO: Investigate dropping leafPlanWeHopeToGetAwayWithIgnoring argument
    val leaves = leafPlanFinder(config, qg)

    if (qg.patternRelationships.size > 0) {

      val generators = solvers.map(_(qg))
      val selectingGenerators = generators.map(_.map(kit.select))
      val generator = selectingGenerators.foldLeft(IDPSolverStep.empty[PatternRelationship, LogicalPlan])(_ ++ _)

      val solver = new IDPSolver[PatternRelationship, LogicalPlan](
        generator = generator,
        projectingSelector = kit.pickBest,
        maxTableSize = maxTableSize
      )

      val seed = initTable(qg, kit, leaves)
      val solutions = solver(seed, qg.patternRelationships)
      val (_, result) = solutions.toSingleOption.getOrElse(throw new AssertionError("Expected a single plan to be left in the plan table"))
      result
    } else {
      val solutionPlans = leaves.filter(plan => (qg.coveredIds -- plan.availableSymbols).isEmpty)
      kit.pickBest(solutionPlans).getOrElse(throw new InternalException("Found no leaf plan for connected component.  This must not happen."))
    }
  }

  private def initTable(qg: QueryGraph, kit: QueryPlannerKit, leaves: Set[LogicalPlan])(implicit context: LogicalPlanningContext) = {
    for (pattern <- qg.patternRelationships)
    yield {
      val accessPlans = planSinglePattern(qg, pattern, leaves).map(kit.select)
      val bestAccessor = kit.pickBest(accessPlans).getOrElse(throw new InternalException("Found no access plan for a pattern relationship in a connected component. This must not happen."))
      Set(pattern) -> bestAccessor
    }
  }

  private def planSinglePattern(qg: QueryGraph, pattern: PatternRelationship, leaves: Iterable[LogicalPlan])
                               (implicit context: LogicalPlanningContext): Iterable[LogicalPlan] = {

    leaves.collect {
      case plan if plan.solved.lastQueryGraph.patternRelationships.contains(pattern) =>
        Set(plan)
      case plan if plan.solved.lastQueryGraph.allCoveredIds.contains(pattern.name) =>
        Set(planSingleProjectEndpoints(pattern, plan))
      case plan =>
        val (start, end) = pattern.nodes
        val leftPlan = planSinglePatternSide(qg, pattern, plan, start)
        val rightPlan = planSinglePatternSide(qg, pattern, plan, end)
        leftPlan.toSet ++ rightPlan.toSet
    }.flatten
  }

  // TODO: Consider replacing with IDP loop
  private def connectComponents(plans: Seq[(LogicalPlan, Seq[QueryGraph])])(implicit context: LogicalPlanningContext, kit: QueryPlannerKit): LogicalPlan = {
    @tailrec
    def recurse(plans: Seq[(LogicalPlan, Seq[QueryGraph])]): LogicalPlan = {
      if (plans.size == 1) {
        val (resultPlan, leftOvers) = applyApplicableOptionalMatches(plans.head)
        if (leftOvers.nonEmpty)
          throw new InternalException(s"Failed to plan all optional matches:\n$leftOvers")
        resultPlan
      } else {
        val candidates = plans.map(applyApplicableOptionalMatches)
        val cartesianProducts: Map[LogicalPlan, (Set[(LogicalPlan, Seq[QueryGraph])], Seq[QueryGraph])] = (for (
          lhs @ (left, leftRemaining) <- candidates.iterator;
          rhs @ (right, rightRemaining) <- candidates.iterator if left ne right;
          remaining = if (leftRemaining.size < rightRemaining.size) leftRemaining else rightRemaining;
          oldPlans = Set(lhs, rhs);
          newPlan = kit.select(context.logicalPlanProducer.planCartesianProduct(left, right))
        )
        yield newPlan ->(oldPlans, remaining)).toMap
        val bestCartesian = kit.pickBest(cartesianProducts.keys).get
        val (oldPlans, remaining) = cartesianProducts(bestCartesian)
        val newPlans = plans.filterNot(oldPlans.contains) :+ (bestCartesian -> remaining)
        recurse(newPlans)
      }
    }

    @tailrec
    def applyApplicableOptionalMatches(todo: (LogicalPlan, Seq[QueryGraph])): (/* new plan*/ LogicalPlan, /* remaining */ Seq[QueryGraph]) = {
      todo match {
        case (plan, allRemaining @ Seq(nextOptional, nextRemaining@_*)) => withOptionalMatch(plan, nextOptional) match {
          case Some(newPlan) => applyApplicableOptionalMatches(newPlan, nextRemaining)
          case None          => (plan, allRemaining)
        }

        case done =>
          done
      }
    }

    def withOptionalMatch(plan: LogicalPlan, optionalMatch: QueryGraph): Option[LogicalPlan] =
      if ((optionalMatch.argumentIds -- plan.availableSymbols).isEmpty) {
        val candidates = config.optionalSolvers.flatMap { solver => solver(optionalMatch, plan) }
        val best = kit.pickBest(candidates)
        best
      } else {
        None
      }

    recurse(plans)
  }
}
