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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical

import org.neo4j.cypher.internal.compiler.v2_2.InternalException
import org.neo4j.cypher.internal.compiler.v2_2.ast.{AllIterablePredicate, FilterScope, Identifier}
import org.neo4j.cypher.internal.compiler.v2_2.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.ExhaustiveQueryGraphSolver.MAX_SEARCH_DEPTH
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.LogicalPlanProducer._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.solveOptionalMatches.OptionalSolver
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.{applyOptional, outerHashJoin, pickBestPlan}

import scala.annotation.tailrec

object ExhaustiveQueryGraphSolver {
  val MAX_SEARCH_DEPTH = 5

  // TODO: Make sure this is tested by extracting tests from greedy expand step
  def planSinglePatternSide(qg: QueryGraph, patternRel: PatternRelationship, plan: LogicalPlan, nodeId: IdName): Set[LogicalPlan] = {
    val availableSymbols = plan.availableSymbols
    if (availableSymbols(nodeId)) {
      val dir = patternRel.directionRelativeTo(nodeId)
      val otherSide = patternRel.otherSide(nodeId)
      val overlapping = availableSymbols.contains(otherSide)
      val mode = if (overlapping) ExpandInto else ExpandAll

      patternRel.length match {
        case SimplePatternLength =>
          Set(planSimpleExpand(plan, nodeId, dir, otherSide, patternRel, mode))

        case length: VarPatternLength =>
          // TODO: Move selections out here (?)
          val availablePredicates = qg.selections.predicatesGiven(availableSymbols + patternRel.name)
          val (predicates, allPredicates) = availablePredicates.collect {
            case all@AllIterablePredicate(FilterScope(identifier, Some(innerPredicate)), relId@Identifier(patternRel.name.name))
              if identifier == relId || !innerPredicate.dependencies(relId) =>
              (identifier, innerPredicate) -> all
          }.unzip
          Set(planVarExpand(plan, nodeId, dir, otherSide, patternRel, predicates, allPredicates, mode))
      }
    } else {
      Set.empty
    }
  }
}

case class ExhaustiveQueryGraphSolver(leafPlanFinder: LogicalLeafPlan.Finder = leafPlanOptions,
                                      bestPlanFinder: CandidateSelector = pickBestPlan,
                                      config: PlanningStrategyConfiguration = PlanningStrategyConfiguration.default,
                                      solvers: Seq[ExhaustiveTableSolver] = Seq(joinTableSolver, expandTableSolver),
                                      optionalSolvers: Seq[OptionalSolver] = Seq(applyOptional, outerHashJoin))
  extends QueryGraphSolver with PatternExpressionSolving {

  import ExhaustiveQueryGraphSolver.planSinglePatternSide

  // TODO: For selection, for now
  override def emptyPlanTable: PlanTable = GreedyPlanTable.empty

  def plan(queryGraph: QueryGraph)(implicit context: LogicalPlanningContext, leafPlan: Option[LogicalPlan]): LogicalPlan = {
    val kitFactory = config.kit
    val plans = queryGraph.connectedComponents.map { qg => planComponent(qg, kitFactory(qg)) }

    val kit = kitFactory(queryGraph)
    plans.reduceOption( (l, r) => kit.select(planCartesianProduct(l, r)) ).getOrElse {
      val plan = if (queryGraph.argumentIds.isEmpty) planSingleRow() else planQueryArgumentRow(queryGraph)
      kit.select(plan)
    }
  }

  private def planComponent(qg: QueryGraph, kit: PlanningStrategyKit)(implicit context: LogicalPlanningContext, leafPlanWeHopeToGetAwayWithIgnoring: Option[LogicalPlan]): LogicalPlan = {
    // TODO: Investigate dropping leafPlanWeHopeToGetAwayWithIgnoring argument
    val leaves = leafPlanFinder(config, qg).toSeq // TODO: Get rid off toSeq
    if (qg.patternRelationships.size > 0) {

      // line 1-4
      val table = initTable(qg, kit, leaves)

      val initialToDo = Solvables(qg)
      iterate(qg, initialToDo, kit, table)

      table.head
    } else {
      bestPlanFinder(leaves).getOrElse(throw new InternalException("Found no leaf plan for connected component.  This must not happen."))
    }
  }

  @tailrec
  private def iterate(fullQG: QueryGraph, toDo: Set[Solvable], kit: PlanningStrategyKit, table: ExhaustivePlanTable)(implicit context: LogicalPlanningContext): Unit = {
    val size = toDo.size
    if (size > 1) {
      // line 7-16
      val k = Math.min(size, MAX_SEARCH_DEPTH)
      for (i <- 2 to k;
           goal <- toDo.subsets(i);
           // TODO: Make sure this is executed lazily to avoid mem blowup
           candidates = solvers.flatMap(solver => solver(fullQG, goal, table)).map(kit.select);
           best <- bestPlanFinder(candidates)) {
        table.put(goal, best)
      }

      // TODO: Get rid of map
      // line 17
      val blockCandidates = toDo.subsets(k).flatMap( set => table(set).map(_ -> set) ).toMap
      val bestBlock = bestPlanFinder(blockCandidates.keys.toSeq).getOrElse(throw new InternalException("Did not find a single solution for a block"))
      val bestSolvables = blockCandidates(bestBlock)

      // TODO: Test this
      // line 18 - 21
      val blockSolved = SolvableBlock(bestSolvables)
      table.put(Set(blockSolved), bestBlock)
      val newToDo = toDo -- bestSolvables + blockSolved
      bestSolvables.subsets.foreach(table.remove)
      iterate(fullQG, newToDo, kit, table)
    }
  }

  def initTable(qg: QueryGraph, kit: PlanningStrategyKit, leaves: Seq[LogicalPlan])(implicit context: LogicalPlanningContext): ExhaustivePlanTable = {
    val table = new ExhaustivePlanTable
    qg.patternRelationships.foreach { pattern =>
      val accessPlans = planSinglePattern(qg, pattern, leaves).map(kit.select)
      val bestAccessor = bestPlanFinder(accessPlans).getOrElse(throw new InternalException("Found no access plan for a pattern relationship in a connected component.  This must not happen."))
      table.put(Set(SolvableRelationship(pattern)), bestAccessor)
    }
    table
  }

  private def planSinglePattern(qg: QueryGraph, pattern: PatternRelationship, leaves: Seq[LogicalPlan]): Seq[LogicalPlan] =
    // TODO: Filter plans that already solve pattern
    leaves.flatMap { plan =>
      val (start, end) = pattern.nodes
      planSinglePatternSide(qg, pattern, plan, start) ++ planSinglePatternSide(qg, pattern, plan, end)
    }
}

