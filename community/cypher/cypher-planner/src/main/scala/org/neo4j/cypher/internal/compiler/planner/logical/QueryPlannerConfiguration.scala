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
import org.neo4j.cypher.internal.compiler.planner.logical.steps.ApplyOptionalSolverFactory
import org.neo4j.cypher.internal.compiler.planner.logical.steps.CandidateSelectorFactory
import org.neo4j.cypher.internal.compiler.planner.logical.steps.DynamicLabelLookupLeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.DynamicRelationshipTypeLookupLeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.OptionalSolverFactory
import org.neo4j.cypher.internal.compiler.planner.logical.steps.OrLeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.OuterHashJoinSolverFactory
import org.neo4j.cypher.internal.compiler.planner.logical.steps.SelectPatternPredicates
import org.neo4j.cypher.internal.compiler.planner.logical.steps.SelectSubQueryPredicates
import org.neo4j.cypher.internal.compiler.planner.logical.steps.VectorSearchLeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.allNodesLeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.allRelationshipsScanLeafPlanner
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

  private def leafPlannersUsedInOrLeafPlanner: IndexedSeq[LeafPlanner] = IndexedSeq(
    // MATCH (n) WHERE id(n) IN ... RETURN n
    idSeekLeafPlanner,
    NodeIndexLeafPlanner(
      Seq(
        // MATCH (n) WHERE n.prop IN ... RETURN n
        nodeIndexSeekPlanProvider,
        // MATCH (n:Person) WHERE n.prop CONTAINS ...
        // MATCH (n:Person) WHERE n.prop ENDS WITH ...
        nodeIndexStringSearchScanPlanProvider,
        // MATCH (n) WHERE has(n.prop) RETURN n
        nodeIndexScanPlanProvider
      )
    ),
    RelationshipIndexLeafPlanner(
      Seq(
        RelationshipIndexScanPlanProvider,
        RelationshipIndexSeekPlanProvider,
        RelationshipIndexStringSearchScanPlanProvider
      )
    ),

    // MATCH (n:Person) RETURN n
    labelScanLeafPlanner,

    // MATCH (n:Person&Artist) RETURN n
    intersectionLabelScanLeafPlanner,

    // MATCH (n:Person&!Artist) RETURN n
    subtractionLabelScanLeafPlanner,

    // MATCH (n:Person|Bird) RETURN n
    unionLabelScanLeafPlanner,

    // MATCH (n:$(['Person', 'Artist'])) RETURN n
    DynamicLabelLookupLeafPlanner,

    // MATCH ()-[r:R]->()
    relationshipTypeScanLeafPlanner,

    // MATCH ()-[r:R|S]->()
    unionRelationshipTypeScanLeafPlanner,

    // MATCH ()-[r:$any(['R', 'S'])]->()
    DynamicRelationshipTypeLookupLeafPlanner
  )

  private def allLeafPlanners: IndexedSeq[LeafPlanner] = {
    val innerOrLeafPlanners = leafPlannersUsedInOrLeafPlanner
    innerOrLeafPlanners ++ IndexedSeq(
      argumentLeafPlanner,

      // MATCH ()-[r]->()
      allRelationshipsScanLeafPlanner,

      // MATCH (n) RETURN n
      allNodesLeafPlanner,

      // Handles OR between other leaf planners
      OrLeafPlanner(innerOrLeafPlanners)
    ) ++ searchClauseLeafPlanner
  }

  private def searchClauseLeafPlanner: IndexedSeq[LeafPlanner] = IndexedSeq(
    // MATCH … SEARCH
    VectorSearchLeafPlanner
  )

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
        ApplyOptionalSolverFactory,
        OuterHashJoinSolverFactory
      ),
      leafPlanners = PrioritizeSearchLeafPlannerFeature {
        PriorityLeafPlannerList(
          // TODO We may want to permit other leaf plans.
          //  See PLAN-3087
          LeafPlannerList(searchClauseLeafPlanner),
          LeafPlannerList(allLeafPlanners)
        )
      }
    )

  }
}

case class QueryPlannerConfiguration(
  leafPlanners: LeafPlannerIterable,
  applySelections: PlanSelector,
  optionalSolvers: Seq[OptionalSolverFactory],
  pickBestCandidate: CandidateSelectorFactory
) {

  def toKit(interestingOrderConfig: InterestingOrderConfig, context: LogicalPlanningContext): QueryPlannerKit =
    QueryPlannerKit(
      select = SelectionPlanner.PlanSelections(applySelections, interestingOrderConfig, context),
      pickBest = pickBestCandidate(context)
    )

  def withLeafPlanners(leafPlanners: LeafPlannerIterable): QueryPlannerConfiguration = copy(leafPlanners = leafPlanners)
}

case class QueryPlannerKit(select: SelectionPlanner, pickBest: CandidateSelector) {

  def select(plans: Set[LogicalPlan], qg: QueryGraph): Set[LogicalPlan] =
    plans.map(plan => select(plan, qg))
}

object QueryPlannerKit {

  def withShortestPathSupportIfNeeded(
    kit: QueryPlannerKit,
    queryGraph: QueryGraph,
    context: LogicalPlanningContext
  ): QueryPlannerKit = {
    if (queryGraph.shortestRelationshipPatterns.isEmpty) {
      kit
    } else {
      kit.copy(select = SelectionPlanner.ShortestPathDecorator(kit.select, context))
    }
  }

  def withSearchSupportIfNeeded(
    kit: QueryPlannerKit,
    queryGraph: QueryGraph,
    context: LogicalPlanningContext
  ): QueryPlannerKit = {
    if (queryGraph.searchClause.isEmpty) {
      kit
    } else {
      kit.copy(select = SelectionPlanner.SearchDecorator(kit.select, context))
    }
  }
}
