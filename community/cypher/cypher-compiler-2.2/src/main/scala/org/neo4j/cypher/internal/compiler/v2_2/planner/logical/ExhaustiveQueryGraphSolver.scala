/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.pickBestPlan
import org.neo4j.cypher.internal.compiler.v2_2.planner.{QueryGraph, Selections}


case class ExhaustiveQueryGraphSolver(leafPlanTableGenerator: PlanTableGenerator,
                                      planProducers: Seq[PlanProducer],
                                      bestPlanFinder: CandidateSelector,
                                      config: PlanningStrategyConfiguration)
  extends TentativeQueryGraphSolver {


  def emptyPlanTable: PlanTable = ExhaustivePlanTable.empty

  def tryPlan(queryGraph: QueryGraph)(implicit context: LogicalPlanningContext, leafPlan: Option[LogicalPlan]): Option[LogicalPlan] = {

    val cache = initiateCacheWithLeafPlans(queryGraph, leafPlan)

    (1 to queryGraph.size) foreach { x =>
      queryGraph.combinations(x).foreach {
          subQG =>
            val plans = planProducers.flatMap(_(subQG, cache)).map(config.applySelections(_, subQG))
            val bestPlan = bestPlanFinder(plans)
            bestPlan.foreach(p => cache + p)
        }
    }

    cache.get(queryGraph)
  }

  private def initiateCacheWithLeafPlans(queryGraph: QueryGraph, leafPlan: Option[LogicalPlan])
                                        (implicit context: LogicalPlanningContext) =
    leafPlanTableGenerator.apply(queryGraph, leafPlan).plans.foldLeft(context.strategy.emptyPlanTable)(_ + _)
  }

object ExhaustiveQueryGraphSolver {

  def withDefaults(leafPlanTableGenerator: PlanTableGenerator = LeafPlanTableGenerator(PlanningStrategyConfiguration.default),
             planProducers: Seq[PlanProducer] = Seq(expandOptions, joinOptions),
             bestPlanFinder: CandidateSelector = pickBestPlan,
             config: PlanningStrategyConfiguration = PlanningStrategyConfiguration.default) =
    new ExhaustiveQueryGraphSolver(leafPlanTableGenerator, planProducers, bestPlanFinder, config)

  type PlanProducer = ((QueryGraph, PlanTable) => Seq[LogicalPlan])

  implicit class RichQueryGraph(inner: QueryGraph) {

    def --(other: QueryGraph): QueryGraph = {
      val remainingRels: Set[PatternRelationship] = inner.patternRelationships -- other.patternRelationships
      val argumentIds = inner.argumentIds -- other.argumentIds
      val hints = inner.hints -- other.hints
      createSubQueryWithRels(remainingRels, argumentIds, hints)
    }

    def combinations(size: Int): Seq[QueryGraph] = if (size < 0 || size > inner.patternRelationships.size )
      throw new IndexOutOfBoundsException(s"Expected $size to be in [0,${inner.patternRelationships.size}[")
     else if (size == 0)
      inner.
        patternNodes.
        map(createSubQueryWithNode(_, inner.argumentIds, inner.hints)).toSeq
    else {
      inner.
        patternRelationships.toSeq.combinations(size).
        map(r => createSubQueryWithRels(r.toSet, inner.argumentIds, inner.hints)).toSeq
    }

    private def createSubQueryWithRels(rels: Set[PatternRelationship], argumentIds: Set[IdName], hints: Set[Hint]) = {
      val nodes = rels.map(r => Seq(r.nodes._1, r.nodes._2)).flatten.toSet
      val filteredHints = hints.filter( h => nodes(IdName(h.identifier.name)))
      val availableIds = rels.map(_.name) ++ nodes

      QueryGraph(
        patternNodes = nodes,
        argumentIds = argumentIds,
        patternRelationships = rels,
        selections = Selections.from(inner.selections.predicatesGiven(availableIds): _*),
        hints = filteredHints
      )
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
