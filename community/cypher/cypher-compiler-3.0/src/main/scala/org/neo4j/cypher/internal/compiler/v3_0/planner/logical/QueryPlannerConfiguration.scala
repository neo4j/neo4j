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
package org.neo4j.cypher.internal.compiler.v3_0.planner.logical

import org.neo4j.cypher.internal.compiler.v3_0.{defaultUpdateStrategy, UpdateStrategy}
import org.neo4j.cypher.internal.compiler.v3_0.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.greedy.projectEndpoints
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.steps._
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.steps.solveOptionalMatches.OptionalSolver

object QueryPlannerConfiguration {
  private val defaultLeafPlanners = LeafPlannerList(
    argumentLeafPlanner,

    // MATCH (n) WHERE id(n) IN ... RETURN n
    idSeekLeafPlanner,

    // MATCH (n) WHERE has(n.prop) RETURN n
    indexScanLeafPlanner,

    // MATCH (n:Person) RETURN n
    labelScanLeafPlanner,

    // MATCH (n) RETURN n
    allNodesLeafPlanner,

    // Legacy indices
    legacyHintLeafPlanner
  )

  private val baseConfiguration: QueryPlannerConfiguration = QueryPlannerConfiguration(
    pickBestCandidate = pickBestPlanUsingHintsAndCost,
    applySelections = Selector(pickBestPlanUsingHintsAndCost,
      selectPatternPredicates,
      triadicSelectionFinder,
      selectCovered,
      selectHasLabelWithJoin
    ),
    projectAllEndpoints = projectEndpoints.all,
    optionalSolvers = Seq(
      applyOptional,
      outerHashJoin
    ),
    leafPlanners = defaultLeafPlanners,
    updateStrategy = defaultUpdateStrategy
  )

  val default = baseConfiguration.withLeafPlanners(defaultLeafPlanners +
    nonUniqueindexSeekLeafPlanner + uniqueIndexSeekLeafPlanner)

  //read-only queries doesn't need to treat unique indexes and normal indexes
  //any differently
  val readOnly = baseConfiguration.withLeafPlanners(defaultLeafPlanners + allIndexSeekLeafPlanner)
}

case class QueryPlannerConfiguration(leafPlanners: LeafPlannerIterable,
                                     applySelections: PlanTransformer[QueryGraph],
                                     projectAllEndpoints: PlanTransformer[QueryGraph],
                                     optionalSolvers: Seq[OptionalSolver],
                                     pickBestCandidate: LogicalPlanningFunction0[CandidateSelector],
                                     updateStrategy: UpdateStrategy) {

  def toKit()(implicit context: LogicalPlanningContext): QueryPlannerKit =
    QueryPlannerKit(
      select = (plan: LogicalPlan, qg: QueryGraph) => applySelections(plan, qg),
      projectAllEndpoints = (plan: LogicalPlan, qg: QueryGraph) => projectAllEndpoints(plan, qg),
      pickBest = pickBestCandidate(context)
    )

  def withLeafPlanners(leafPlanners: LeafPlannerIterable) = copy(leafPlanners = leafPlanners)

  def withUpdateStrategy(updateStrategy: UpdateStrategy) = copy(updateStrategy = updateStrategy)
}

case class QueryPlannerKit(select: (LogicalPlan, QueryGraph) => LogicalPlan,
                           projectAllEndpoints: (LogicalPlan, QueryGraph) => LogicalPlan,
                           pickBest: CandidateSelector) {
  def select(plans: Iterable[Seq[LogicalPlan]], qg: QueryGraph): Iterable[Seq[LogicalPlan]] =
    plans.map(_.map(plan => select(plan, qg)))
}
