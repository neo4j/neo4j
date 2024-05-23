/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.CandidateSelectorFactory
import org.neo4j.cypher.internal.compiler.planner.logical.steps.OptionalSolver
import org.neo4j.cypher.internal.compiler.planner.logical.steps.OrLeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.SelectPatternPredicates
import org.neo4j.cypher.internal.compiler.planner.logical.steps.SelectSubQueryPredicates
import org.neo4j.cypher.internal.compiler.planner.logical.steps.allNodesLeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.allRelationshipsScanLeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.applyOptional
import org.neo4j.cypher.internal.compiler.planner.logical.steps.argumentLeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.idSeekLeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.NodeIndexLeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.RelationshipIndexLeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.RelationshipIndexScanPlanProvider
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.RelationshipIndexSeekPlanProvider
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.RelationshipIndexStringSearchScanPlanProvider
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.nodeIndexScanPlanProvider
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.nodeIndexSeekPlanProvider
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.nodeIndexStringSearchScanPlanProvider
import org.neo4j.cypher.internal.compiler.planner.logical.steps.intersectionLabelScanLeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.labelScanLeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.outerHashJoin
import org.neo4j.cypher.internal.compiler.planner.logical.steps.pickBestPlanUsingHintsAndCost
import org.neo4j.cypher.internal.compiler.planner.logical.steps.relationshipTypeScanLeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.resolveImplicitlySolvedPredicates
import org.neo4j.cypher.internal.compiler.planner.logical.steps.selectCovered
import org.neo4j.cypher.internal.compiler.planner.logical.steps.selectHasLabelWithJoin
import org.neo4j.cypher.internal.compiler.planner.logical.steps.subtractionLabelScanLeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.triadicSelectionFinder
import org.neo4j.cypher.internal.compiler.planner.logical.steps.unionLabelScanLeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.unionRelationshipTypeScanLeafPlanner
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

object QueryPlannerConfiguration {

  private def leafPlannersUsedInOrLeafPlanner(restrictions: LeafPlanRestrictions): IndexedSeq[LeafPlanner] = IndexedSeq(
    // MATCH (n) WHERE id(n) IN ... RETURN n
    idSeekLeafPlanner(restrictions.symbolsThatShouldOnlyUseIndexSeekLeafPlanners),
    NodeIndexLeafPlanner(
      Seq(
        // MATCH (n) WHERE n.prop IN ... RETURN n
        nodeIndexSeekPlanProvider,
        // MATCH (n:Person) WHERE n.prop CONTAINS ...
        // MATCH (n:Person) WHERE n.prop ENDS WITH ...
        nodeIndexStringSearchScanPlanProvider,
        // MATCH (n) WHERE has(n.prop) RETURN n
        nodeIndexScanPlanProvider
      ),
      restrictions
    ),
    RelationshipIndexLeafPlanner(
      Seq(
        RelationshipIndexScanPlanProvider,
        RelationshipIndexSeekPlanProvider,
        RelationshipIndexStringSearchScanPlanProvider
      ),
      restrictions
    ),

    // MATCH (n:Person) RETURN n
    labelScanLeafPlanner(restrictions.symbolsThatShouldOnlyUseIndexSeekLeafPlanners),

    // MATCH (n:Person&Artist) RETURN n
    intersectionLabelScanLeafPlanner(restrictions.symbolsThatShouldOnlyUseIndexSeekLeafPlanners),

    // MATCH (n:Person&!Artist) RETURN n
    subtractionLabelScanLeafPlanner(restrictions.symbolsThatShouldOnlyUseIndexSeekLeafPlanners),

    // MATCH (n:Person|Bird) RETURN n
    unionLabelScanLeafPlanner(restrictions.symbolsThatShouldOnlyUseIndexSeekLeafPlanners),

    // MATCH ()-[r:R]->()
    relationshipTypeScanLeafPlanner(restrictions.symbolsThatShouldOnlyUseIndexSeekLeafPlanners),

    // MATCH ()-[r:R|S]->()
    unionRelationshipTypeScanLeafPlanner(restrictions.symbolsThatShouldOnlyUseIndexSeekLeafPlanners)
  )

  private def allLeafPlanners(restrictions: LeafPlanRestrictions): IndexedSeq[LeafPlanner] = {
    val innerOrLeafPlanners = leafPlannersUsedInOrLeafPlanner(restrictions)
    innerOrLeafPlanners ++ IndexedSeq(
      argumentLeafPlanner(restrictions.symbolsThatShouldOnlyUseIndexSeekLeafPlanners),

      // MATCH ()-[r]->()
      allRelationshipsScanLeafPlanner(restrictions.symbolsThatShouldOnlyUseIndexSeekLeafPlanners),

      // MATCH (n) RETURN n
      allNodesLeafPlanner(restrictions.symbolsThatShouldOnlyUseIndexSeekLeafPlanners),

      // Handles OR between other leaf planners
      OrLeafPlanner(innerOrLeafPlanners)
    )
  }

  /**
   * When doing nested index joins, we have certain variables for which we only want to allow certain index plans.
   * This method returns leaf planners that will not produce any other plans for these variables.
   */
  def leafPlannersForNestedIndexJoins(restrictions: LeafPlanRestrictions): LeafPlannerIterable = {
    LeafPlannerList(allLeafPlanners(restrictions))
  }

  val default: QueryPlannerConfiguration = {
    val predicateSelector = steps.Selector(
      pickBestPlanUsingHintsAndCost,
      SelectPatternPredicates,
      triadicSelectionFinder,
      SelectSubQueryPredicates,
      resolveImplicitlySolvedPredicates,
      selectCovered,
      selectHasLabelWithJoin
    )

    QueryPlannerConfiguration(
      pickBestCandidate = pickBestPlanUsingHintsAndCost,
      applySelections = predicateSelector,
      optionalSolvers = Seq(
        applyOptional,
        outerHashJoin
      ),
      leafPlanners = LeafPlannerList(allLeafPlanners(LeafPlanRestrictions.NoRestrictions))
    )

  }
}

case class QueryPlannerConfiguration(
  leafPlanners: LeafPlannerIterable,
  applySelections: PlanSelector,
  optionalSolvers: Seq[OptionalSolver],
  pickBestCandidate: CandidateSelectorFactory
) {

  def toKit(interestingOrderConfig: InterestingOrderConfig, context: LogicalPlanningContext): QueryPlannerKit =
    QueryPlannerKit(
      select = (plan: LogicalPlan, qg: QueryGraph) => applySelections(plan, qg, interestingOrderConfig, context),
      pickBest = pickBestCandidate(context)
    )

  def withLeafPlanners(leafPlanners: LeafPlannerIterable): QueryPlannerConfiguration = copy(leafPlanners = leafPlanners)
}

case class QueryPlannerKit(select: (LogicalPlan, QueryGraph) => LogicalPlan, pickBest: CandidateSelector) {

  def select(plans: Iterable[LogicalPlan], qg: QueryGraph): Iterable[LogicalPlan] =
    plans.map(plan => select(plan, qg))
}
