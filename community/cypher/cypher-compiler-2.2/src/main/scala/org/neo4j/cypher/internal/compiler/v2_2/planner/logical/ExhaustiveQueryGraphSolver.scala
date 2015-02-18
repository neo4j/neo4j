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

import org.neo4j.cypher.internal.compiler.v2_2.ast.Hint
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.ExhaustiveQueryGraphSolver._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.{LogicalPlan, _}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.LogicalPlanProducer._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.solveOptionalMatches.OptionalSolver
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.{applyOptional, outerHashJoin, pickBestPlan}
import org.neo4j.cypher.internal.compiler.v2_2.planner.{CantHandleQueryException, QueryGraph, Selections}

import scala.annotation.tailrec
import scala.collection.mutable

case class ExhaustiveQueryGraphSolver(leafPlanTableGenerator: PlanTableGenerator,
                                      planProducers: Seq[PlanProducer],
                                      bestPlanFinder: CandidateSelector,
                                      config: PlanningStrategyConfiguration,
                                      optionalSolvers: Seq[OptionalSolver])
  extends QueryGraphSolver with PatternExpressionSolving {

  def emptyPlanTable: PlanTable = ExhaustivePlanTable.empty

  def plan(queryGraph: QueryGraph)(implicit context: LogicalPlanningContext, leafPlan: Option[LogicalPlan]): LogicalPlan = {
    val cache = initiateCacheWithLeafPlans(queryGraph, leafPlan)
    val plans = queryGraph.connectedComponents.map { qg =>
      (1 to qg.size) foreach { x =>
        qg.combinations(x).foreach {
          subQG =>
            val plans = planProducers.flatMap(_(subQG, cache)).map(config.applySelections(_, subQG))
            val bestPlan = bestPlanFinder(plans)
            bestPlan.foreach(p => cache + p)
        }
      }

      cache.getOrElse(qg, throw new CantHandleQueryException)
    }

    val resultPlan = plans.reduceRightOption[LogicalPlan] { case (p, acc) =>
      val result = config.applySelections(planCartesianProduct(p, acc), queryGraph)
      cache + result
      result
    }

    val plan = resultPlan.getOrElse(cache.getOrElse(queryGraph,
      if (queryGraph.argumentIds.isEmpty)
        planSingleRow()
      else
        planQueryArgumentRow(queryGraph)
    ))

    val optionalQGs: Seq[QueryGraph] = findQGsToSolve(plan, cache, queryGraph.optionalMatches)
    val result = optionalQGs.foldLeft(plan) {
      case (lhs: LogicalPlan, optionalQg: QueryGraph) =>
        val plans = optionalSolvers.flatMap(_.apply(optionalQg, lhs))
        assert(plans.map(_.solved).distinct.size == 1) // All plans are solving the same query
        bestPlanFinder(plans).get
    }
    result
  }

  private def initiateCacheWithLeafPlans(queryGraph: QueryGraph, leafPlan: Option[LogicalPlan])
                                        (implicit context: LogicalPlanningContext) =
    leafPlanTableGenerator.apply(queryGraph, leafPlan).plans.foldLeft(context.strategy.emptyPlanTable)(_ + _)

  private def findQGsToSolve(plan: LogicalPlan, table: PlanTable, graphs: Seq[QueryGraph]): Seq[QueryGraph] = {
    @tailrec
    def inner(in: Seq[QueryGraph], out: Seq[QueryGraph]): Seq[QueryGraph] = in match {
      case hd :: tl if isSolved(table, hd)  => inner(tl, out)
      case hd :: tl if applicable(plan, hd) => inner(tl, out :+ hd)
      case _                                => out
    }

    inner(graphs, Seq.empty)
  }

  private def isSolved(table: PlanTable, optionalQG: QueryGraph) =
    table.plans.exists(_.solved.lastQueryGraph.optionalMatches.contains(optionalQG))

  private def applicable(outerPlan: LogicalPlan, optionalQG: QueryGraph) =
    optionalQG.argumentIds.subsetOf(outerPlan.availableSymbols)
}

object ExhaustiveQueryGraphSolver {

  def withDefaults(leafPlanTableGenerator: PlanTableGenerator = LeafPlanTableGenerator(PlanningStrategyConfiguration.default),
                   planProducers: Seq[PlanProducer] = Seq(expandOptions, joinOptions),
                   bestPlanFinder: CandidateSelector = pickBestPlan,
                   config: PlanningStrategyConfiguration = PlanningStrategyConfiguration.default,
                   optionalSolvers: Seq[OptionalSolver] = Seq(applyOptional, outerHashJoin)) =
    new ExhaustiveQueryGraphSolver(leafPlanTableGenerator, planProducers, bestPlanFinder, config, optionalSolvers)

  type PlanProducer = ((QueryGraph, PlanTable) => Seq[LogicalPlan])

  implicit class RichQueryGraph(inner: QueryGraph) {
    /**
     * Returns the connected patterns of this query graph where each connected pattern is represented by a QG.
     * Does not include optional matches, shortest paths or predicates that have dependencies across multiple of the
     * connected query graphs.
     */
    def connectedComponents: Seq[QueryGraph] = {
      val visited = mutable.Set.empty[IdName]
      inner.patternNodes.toSeq.collect {
        case patternNode if !visited(patternNode) =>
          val qg = connectedComponentFor(patternNode, visited)
          val coveredIds = qg.coveredIds
          val predicates = inner.selections.predicates.filter(_.dependencies.subsetOf(coveredIds))
          val arguments = inner.argumentIds.filter(coveredIds)
          val hints = inner.hints.filter(h => coveredIds.contains(IdName(h.identifier.name)))
          val shortestPaths = inner.shortestPathPatterns.filter {
            p => coveredIds.contains(p.rel.nodes._1) && coveredIds.contains(p.rel.nodes._2)
          }
          qg.
            withSelections(Selections(predicates)).
            withArgumentIds(arguments).
            addHints(hints).
            addShortestPaths(shortestPaths.toSeq: _*)
      }
    }

    def --(other: QueryGraph): QueryGraph = {
      val remainingRels: Set[PatternRelationship] = inner.patternRelationships -- other.patternRelationships
      val hints = inner.hints -- other.hints
      createSubQueryWithRels(remainingRels, hints)
    }

    def combinations(size: Int): Seq[QueryGraph] = if (size < 0 || size > inner.patternRelationships.size )
      throw new IndexOutOfBoundsException(s"Expected $size to be in [0,${inner.patternRelationships.size}[")
     else if (size == 0) {
      val nonArgs = inner.
        patternNodes.filterNot(inner.argumentIds).
        map(createSubQueryWithNode(_, inner.argumentIds, inner.hints)).toSeq

      val args: Option[QueryGraph] = if ((inner.argumentIds intersect inner.patternNodes).isEmpty)
        None
      else {
        val filteredHints = inner.hints.filter(h => inner.argumentIds.contains(IdName(h.identifier.name)))

        Some(QueryGraph(
          patternNodes = inner.patternNodes.filter(inner.argumentIds),
          argumentIds = inner.argumentIds,
          selections = Selections.from(inner.selections.predicatesGiven(inner.argumentIds): _*),
          hints = filteredHints
        ))
      }

      nonArgs ++ args
    } else {
      inner.
        patternRelationships.toSeq.combinations(size).
        map(r => createSubQueryWithRels(r.toSet, inner.hints)).toSeq
    }

    private def connectedComponentFor(startNode: IdName, visited: mutable.Set[IdName]): QueryGraph = {
      val queue = mutable.Queue(startNode)
      var qg = QueryGraph.empty.withArgumentIds(inner.argumentIds)
      while (queue.nonEmpty) {
        val node = queue.dequeue()
        qg = if (visited(node)) {
          qg
        } else {
          visited += node

          val patternRelationships = inner.patternRelationships.filter { rel =>
            rel.coveredIds.contains(node) && !qg.patternRelationships.contains(rel)
          }

          queue.enqueue(patternRelationships.toSeq.map(_.otherSide(node)): _*)

          qg
            .addPatternNodes(node)
            .addPatternRelationships(patternRelationships.toSeq)
        }
      }
      qg
    }

    private def createSubQueryWithRels(rels: Set[PatternRelationship], hints: Set[Hint]) = {
      val nodes = rels.map(r => Seq(r.nodes._1, r.nodes._2)).flatten.toSet
      val filteredHints = hints.filter( h => nodes(IdName(h.identifier.name)))

      val qg = QueryGraph(
        patternNodes = nodes,
        argumentIds = inner.argumentIds,
        patternRelationships = rels,
        hints = filteredHints
      )

      qg.
        withSelections(selections = Selections.from(inner.selections.predicatesGiven(qg.coveredIds): _*))
    }

    private def createSubQueryWithNode(id: IdName, argumentIds: Set[IdName], hints: Set[Hint]) = {
      val filteredHints = hints.filter(id.name == _.identifier.name)

      QueryGraph(
        patternNodes = Set(id),
        argumentIds = argumentIds,
        selections = Selections.from(inner.selections.predicatesGiven(Set(id)): _*),
        hints = filteredHints
      )
    }
  }
}
