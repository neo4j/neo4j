/*
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
package org.neo4j.cypher.internal.compiler.v3_0.planner.logical.idp

import org.neo4j.cypher.internal.compiler.v3_0.helpers.IteratorSupport._
import org.neo4j.cypher.internal.compiler.v3_0.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical._
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.idp.expandSolverStep.{planSinglePatternSide, planSingleProjectEndpoints}
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.steps.solveOptionalMatches.OptionalSolver
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.steps.{applyOptional, outerHashJoin, planShortestPaths}
import org.neo4j.cypher.internal.frontend.v3_0.{ast, InternalException}
import IDPQueryGraphSolver.RelOrJoin
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

  // A type alias to make it easy to change in the future
  type RelOrJoin = Either[PatternRelationship, ast.Equals]
}

/**
 * This planner is based on the paper
 *
 *   "Iterative Dynamic Programming: A New Class of Query Optimization Algorithms"
 *
 * written by Donald Kossmann and Konrad Stocker
 */
case class IDPQueryGraphSolver(monitor: IDPQueryGraphSolverMonitor,
                               maxTableSize: Int = 256,
                               leafPlanFinder: LogicalLeafPlan.Finder = leafPlanOptions,
                               solvers: Seq[QueryGraph => IDPSolverStep[RelOrJoin, LogicalPlan, LogicalPlanningContext]] = Seq(nodeJoinSolverStep(_), expandSolverStep(_)),
                               optionalSolvers: Seq[OptionalSolver] = Seq(applyOptional, outerHashJoin))
  extends QueryGraphSolver with PatternExpressionSolving {

  def plan(queryGraph: QueryGraph)(implicit context: LogicalPlanningContext, leafPlan: Option[LogicalPlan]): LogicalPlan = {
    implicit val kit = kitWithShortestPathSupport(context.config.toKit())
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
        val shortestPath = planShortestPaths(plan, qg, sp)
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
    val leaves = leafPlanFinder(context.config, qg)

    val bestPlan =
      if (qg.patternRelationships.nonEmpty) {

        val generators = solvers.map(_ (qg))
        val selectingGenerators = generators.map(_.map(plan => kit.select(plan, qg)))
        val generator = selectingGenerators.foldLeft(IDPSolverStep.empty[RelOrJoin, LogicalPlan, LogicalPlanningContext])(_ ++ _)

        val solver = new IDPSolver[RelOrJoin, LogicalPlan, LogicalPlanningContext](
          generator = generator,
          projectingSelector = kit.pickBest,
          maxTableSize = maxTableSize,
          monitor = monitor
        )

        monitor.initTableFor(qg, component)
        val seed = tableInitialiser(qg, kit, leaves)
        monitor.startIDPIterationFor(qg, component)
        val solutions = solver(seed, qg.patternRelationships.map(Left.apply))
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

  private def connectComponentsAndSolveOptionalMatch(plans: Set[LogicalPlan], qg: QueryGraph)
                                                    (implicit context: LogicalPlanningContext, kit: QueryPlannerKit): LogicalPlan = {


    @tailrec
    def recurse(plans: Set[LogicalPlan], optionalMatches: Seq[QueryGraph]): (Set[LogicalPlan], Seq[QueryGraph]) = {
      if (optionalMatches.nonEmpty) {
        // If we have optional matches left to solve - start with that
        val firstOptionalMatch = optionalMatches.head
        val applicablePlan = plans.find(p => firstOptionalMatch.argumentIds subsetOf p.availableSymbols)

        applicablePlan match {
          case Some(p) =>
            val candidates = context.config.optionalSolvers.flatMap(solver => solver(firstOptionalMatch, p))
            val best = kit.pickBest(candidates).get
            recurse(plans - p + best, optionalMatches.tail)

          case None =>
            // If we couldn't find any optional match we can take on, produce the best cartesian product possible
            recurse(cartesianProductsOrValueJoins(plans, qg), optionalMatches)
        }
      } else if (plans.size > 1) {

        recurse(cartesianProductsOrValueJoins(plans, qg), optionalMatches)
      } else (plans, optionalMatches)
    }

    val (resultingPlans, optionalMatches) = recurse(plans, qg.optionalMatches)
    assert(resultingPlans.size == 1)
    assert(optionalMatches.isEmpty)
    resultingPlans.head
  }
}
