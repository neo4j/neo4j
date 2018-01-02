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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.idp

import org.neo4j.cypher.internal.compiler.v2_3.helpers.IteratorSupport._
import org.neo4j.cypher.internal.compiler.v2_3.planner.{Predicate, QueryGraph}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.idp.expandSolverStep.{planSinglePatternSide, planSingleProjectEndpoints}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.steps.solveOptionalMatches.OptionalSolver
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.steps.{applyOptional, outerHashJoin}
import org.neo4j.cypher.internal.frontend.v2_3.InternalException
import org.neo4j.cypher.internal.frontend.v2_3.ast._
import scala.annotation.tailrec

trait IDPQueryGraphSolverMonitor extends IDPSolverMonitor {
  def noIDPIterationFor(graph: QueryGraph, component: Int, result: LogicalPlan): Unit
  def initTableFor(graph: QueryGraph, component: Int): Unit
  def startIDPIterationFor(graph: QueryGraph, component: Int): Unit
  def endIDPIterationFor(graph: QueryGraph, i: Int, result: LogicalPlan): Unit
  def emptyComponentPlanned(graph: QueryGraph, plan: LogicalPlan): Unit
  def startConnectingComponents(graph: QueryGraph): Unit
  def endConnectingComponents(graph: QueryGraph, result: LogicalPlan): Unit
}

object IDPQueryGraphSolver {
  val VERBOSE = java.lang.Boolean.getBoolean("pickBestPlan.VERBOSE")
}

/**
 * This planner is based on the paper
 *
 *   "Iterative Dynamic Programming: A New Class of Query Optimization Algorithms"
 *
 * written by Donald Kossmann and Konrad Stocker
 */
case class IDPQueryGraphSolver(monitor: IDPQueryGraphSolverMonitor,
                               solverConfig: IDPSolverConfig = DefaultIDPSolverConfig,
                               leafPlanFinder: LogicalLeafPlan.Finder = leafPlanOptions,
                               config: QueryPlannerConfiguration = QueryPlannerConfiguration.default,
                               optionalSolvers: Seq[OptionalSolver] = Seq(applyOptional, outerHashJoin))
  extends QueryGraphSolver with PatternExpressionSolving {

  def plan(queryGraph: QueryGraph)(implicit context: LogicalPlanningContext, leafPlan: Option[LogicalPlan]): LogicalPlan = {
    implicit val kit = kitWithShortestPathSupport(config.toKit())
    val components = queryGraph.connectedComponents
    val plans = if (components.isEmpty) planEmptyComponent(queryGraph) else planComponents(components)

    monitor.startConnectingComponents(queryGraph)
    val result = connectComponentsAndSolveOptionalMatch(plans.toSet, queryGraph)
    monitor.endConnectingComponents(queryGraph, result)
    result
  }

  private def kitWithShortestPathSupport(kit: QueryPlannerKit)(implicit context: LogicalPlanningContext) =
    kit.copy(select = selectShortestPath(kit, _, _))

  private def selectShortestPath(kit: QueryPlannerKit, initialPlan: LogicalPlan, qg: QueryGraph)
                                (implicit context: LogicalPlanningContext): LogicalPlan =
    qg.shortestPathPatterns.foldLeft(kit.select(initialPlan, qg)) {
      case (plan, sp) if sp.isFindableFrom(plan.availableSymbols) =>
        val pathIdentifiers = Set(sp.name, Some(sp.rel.name)).flatten
        val pathPredicates = qg.selections.predicates.collect {
          case Predicate(dependencies, expr: Expression) if (dependencies intersect pathIdentifiers).nonEmpty => expr
        }.toSeq
        val shortestPath = context.logicalPlanProducer.planShortestPaths(plan, sp, pathPredicates)
        kit.select(shortestPath, qg)
      case (plan, _) => plan
    }

  private def planComponents(components: Seq[QueryGraph])(implicit context: LogicalPlanningContext, leafPlan: Option[LogicalPlan], kit: QueryPlannerKit): Seq[LogicalPlan] =
    components.zipWithIndex.map { case (qg, component) =>
      planComponent(qg, component)
    }

  private def planEmptyComponent(queryGraph: QueryGraph)(implicit context: LogicalPlanningContext, leafPlan: Option[LogicalPlan], kit: QueryPlannerKit): Seq[LogicalPlan] = {
    val plan = if (queryGraph.argumentIds.isEmpty)
      context.logicalPlanProducer.planSingleRow()
    else
      context.logicalPlanProducer.planQueryArgumentRow(queryGraph)
    val result: LogicalPlan = kit.select(plan, queryGraph)
    monitor.emptyComponentPlanned(queryGraph, result)
    Seq(result)
  }

  private def planComponent(qg: QueryGraph, component: Int)
                           (implicit context: LogicalPlanningContext, kit: QueryPlannerKit, leafPlanWeHopeToGetAwayWithIgnoring: Option[LogicalPlan]): LogicalPlan = {
    // TODO: Investigate dropping leafPlanWeHopeToGetAwayWithIgnoring argument
    val leaves = leafPlanFinder(config, qg)

    val bestPlan =
      if (qg.patternRelationships.nonEmpty) {

        val generators = solverConfig.solvers(qg).map(_(qg))
        val selectingGenerators = generators.map(_.map(plan => kit.select(plan, qg)))
        val generator = selectingGenerators.foldLeft(IDPSolverStep.empty[PatternRelationship, LogicalPlan, LogicalPlanningContext])(_ ++ _)

        val solver = new IDPSolver[PatternRelationship, LogicalPlan, LogicalPlanningContext](
          generator = generator,
          projectingSelector = kit.pickBest,
          maxTableSize = solverConfig.maxTableSize,
          iterationDurationLimit = solverConfig.iterationDurationLimit,
          monitor = monitor
        )

        monitor.initTableFor(qg, component)
        val seed = initTable(qg, kit, leaves)
        monitor.startIDPIterationFor(qg, component)
        val solutions = solver(seed, qg.patternRelationships)
        val (_, result) = solutions.toSingleOption.getOrElse(throw new AssertionError("Expected a single plan to be left in the plan table"))
        monitor.endIDPIterationFor(qg, component, result)

        result
      } else {
        val solutionPlans = leaves collect {
          case plan if (qg.coveredIds -- plan.availableSymbols).isEmpty => kit.select(plan, qg)
        }
        val result = kit.pickBest(solutionPlans).getOrElse(throw new InternalException("Found no leaf plan for connected component.  This must not happen."))
        monitor.noIDPIterationFor(qg, component, result)
        result
      }

    if (IDPQueryGraphSolver.VERBOSE)
      println(s"Result (picked best plan):\n\tPlan #${bestPlan.debugId}\n\t${bestPlan.toString}\n\n")

    bestPlan
  }

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
      case plan =>
        val (start, end) = pattern.nodes
        val leftExpand = planSinglePatternSide(qg, pattern, plan, start)
        val rightExpand = planSinglePatternSide(qg, pattern, plan, end)
        leftExpand.toSet ++ rightExpand.toSet
    }
  }

  private def connectComponentsAndSolveOptionalMatch(plans: Set[LogicalPlan], qg: QueryGraph)
    (implicit context: LogicalPlanningContext, kit: QueryPlannerKit): LogicalPlan = {

    def findBestCartesianProduct(plans: Set[LogicalPlan]): Set[LogicalPlan] = {

      assert(plans.size > 1, "Can't build cartesian product with less than two input plans")

      val allCrossProducts = (for (p1 <- plans; p2 <- plans if p1 != p2) yield {
        val crossProduct = kit.select(context.logicalPlanProducer.planCartesianProduct(p1, p2), qg)
        (crossProduct, (p1, p2))
      }).toMap
      val bestCartesian = kit.pickBest(allCrossProducts.keySet).get
      val (p1, p2) = allCrossProducts(bestCartesian)

      plans - p1 - p2 + bestCartesian
    }

    @tailrec
    def recurse(plans: Set[LogicalPlan], optionalMatches: Seq[QueryGraph]): (Set[LogicalPlan], Seq[QueryGraph]) = {
      if (optionalMatches.nonEmpty) {
        // If we have optional matches left to solve - start with that
        val firstOptionalMatch = optionalMatches.head
        val applicablePlan = plans.find(p => firstOptionalMatch.argumentIds subsetOf p.availableSymbols)

        applicablePlan match {
          case Some(p) =>
            val candidates = config.optionalSolvers.flatMap(solver => solver(firstOptionalMatch, p))
            val best = kit.pickBest(candidates).get
            recurse(plans - p + best, optionalMatches.tail)

          case None =>
            // If we couldn't find any optional match we can take on, produce the best cartesian product possible
            recurse(findBestCartesianProduct(plans), optionalMatches)
        }
      } else if (plans.size > 1) {

        recurse(findBestCartesianProduct(plans), optionalMatches)
      } else (plans, optionalMatches)
    }

    val (resultingPlans, optionalMatches) = recurse(plans, qg.optionalMatches)
    assert(resultingPlans.size == 1)
    assert(optionalMatches.isEmpty)
    resultingPlans.head
  }
}
