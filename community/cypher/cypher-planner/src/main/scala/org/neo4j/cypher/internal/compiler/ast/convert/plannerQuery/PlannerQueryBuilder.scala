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
package org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery

import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsParameters
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery.PlannerQueryBuilder.finalizeQuery
import org.neo4j.cypher.internal.compiler.helpers.SeqSupport.RichSeq
import org.neo4j.cypher.internal.expressions.AssertIsNode
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.CallSubqueryHorizon
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.QueryHorizon
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.collection.immutable.ListSet

case class PlannerQueryBuilder(q: SinglePlannerQuery, semanticTable: SemanticTable) {

  def amendQueryGraph(f: QueryGraph => QueryGraph): PlannerQueryBuilder =
    copy(q = q.updateTailOrSelf(_.amendQueryGraph(f)))

  def withHorizon(horizon: QueryHorizon): PlannerQueryBuilder =
    copy(q = q.updateTailOrSelf(_.withHorizon(horizon)))

  def withInitialArguments(arguments: Set[LogicalVariable]): PlannerQueryBuilder =
    copy(q.amendQueryGraph(_.withArgumentIds(arguments)))

  def withCallSubquery(
    subquery: PlannerQuery,
    correlated: Boolean,
    yielding: Boolean,
    inTransactionsParameters: Option[InTransactionsParameters],
    optional: Boolean
  ): PlannerQueryBuilder = {
    withHorizon(CallSubqueryHorizon(subquery, correlated, yielding, inTransactionsParameters, optional)).withTail(
      SinglePlannerQuery.empty
    )
  }

  def withTail(newTail: SinglePlannerQuery): PlannerQueryBuilder = {
    copy(q =
      q.updateTailOrSelf(
        _.withTail(newTail.amendQueryGraph(_.addArgumentIds(currentlyExposedSymbols.toIndexedSeq)))
      )
    )
  }

  def withQueryInput(inputVariables: Seq[Variable]): PlannerQueryBuilder = {
    copy(q = q.withInput(inputVariables))
  }

  def withInterestingOrder(interestingOrder: InterestingOrder): PlannerQueryBuilder = {
    val existingIO = q.last.interestingOrder
    val newIO = InterestingOrder(
      interestingOrder.requiredOrderCandidate,
      existingIO.interestingOrderCandidates ++ interestingOrder.interestingOrderCandidates
    )
    copy(q = q.updateTailOrSelf(_.withInterestingOrder(newIO)))
  }

  def withPropagatedTailInterestingOrder(): PlannerQueryBuilder = {
    copy(q = q.withTailInterestingOrder(q.last.interestingOrder))
  }

  private def currentlyExposedSymbols: Set[LogicalVariable] = {
    q.lastQueryHorizon.exposedSymbols(q.lastQueryGraph.allCoveredIds)
  }

  def currentlyAvailableVariables: Set[LogicalVariable] = {
    val allPlannerQueries = q.allPlannerQueries
    val previousAvailableSymbols =
      if (allPlannerQueries.length > 1) {
        val current = allPlannerQueries(allPlannerQueries.length - 2)
        current.horizon.exposedSymbols(current.queryGraph.allCoveredIds)
      } else Set.empty

    // for the last planner query we should not consider the return projection
    previousAvailableSymbols ++ q.lastQueryGraph.allCoveredIds
  }

  def currentQueryGraph: QueryGraph = q.lastQueryGraph

  def lastQGNodesAndArguments: Set[LogicalVariable] = {
    val qg = q.lastQueryGraph
    qg.allPatternNodes ++ qg.argumentIds
  }

  def readOnly: Boolean = q.queryGraph.readOnly

  def build(): SinglePlannerQuery = {
    finalizeQuery(q)
  }
}

object PlannerQueryBuilder {

  def apply(semanticTable: SemanticTable): PlannerQueryBuilder =
    PlannerQueryBuilder(SinglePlannerQuery.empty, semanticTable)

  def apply(semanticTable: SemanticTable, argumentIds: Set[LogicalVariable]): PlannerQueryBuilder =
    PlannerQueryBuilder(RegularSinglePlannerQuery(queryGraph = QueryGraph(argumentIds = argumentIds)), semanticTable)

  def finalizeQuery(q: SinglePlannerQuery): SinglePlannerQuery = {

    def fixArgumentIds(plannerQuery: SinglePlannerQuery): SinglePlannerQuery = plannerQuery.foldMap {
      case (head, tail) =>
        val symbols = head.horizon.exposedSymbols(head.queryGraph.allCoveredIds)
        val newTailGraph = tail.queryGraph.withArgumentIds(symbols)
        tail.withQueryGraph(newTailGraph)
    }

    def fixArgumentIdsOnOptionalMatch(plannerQuery: SinglePlannerQuery): SinglePlannerQuery = {
      val optionalMatches = plannerQuery.queryGraph.optionalMatches
      val (_, newOptionalMatches) =
        optionalMatches.foldMap(plannerQuery.queryGraph.idsWithoutOptionalMatchesOrUpdates) {
          case (args, qg) =>
            (args ++ qg.allCoveredIds, qg.withArgumentIds(args intersect qg.dependencies))
        }
      plannerQuery
        .amendQueryGraph(_.withOptionalMatches(newOptionalMatches.toIndexedSeq))
        .updateTail(fixArgumentIdsOnOptionalMatch)
    }

    def groupInequalities(plannerQuery: SinglePlannerQuery): SinglePlannerQuery = {

      plannerQuery
        .amendQueryGraph(_.mapSelections {
          case Selections(predicates) =>
            val newPredicates = groupInequalityPredicates(ListSet.from(predicates))
            Selections(newPredicates)
        })
        .updateTail(groupInequalities)
    }

    def fixStandaloneArgumentPatternNodes(part: SinglePlannerQuery): SinglePlannerQuery = {

      def addPredicates(qg: QueryGraph): QueryGraph = {
        val preds = qg.standaloneArgumentPatternNodes.map { n =>
          AssertIsNode(n)(InputPosition.NONE)
        }
        qg.addPredicates(preds.toSeq: _*)
      }

      val newOptionalMatches = part.queryGraph.optionalMatches.map(addPredicates)
      part
        .amendQueryGraph(qg => addPredicates(qg).withOptionalMatches(newOptionalMatches))
        .updateTail(fixStandaloneArgumentPatternNodes)
    }

    Function.chain[SinglePlannerQuery](List(
      fixArgumentIds,
      fixArgumentIdsOnOptionalMatch,
      groupInequalities,
      fixStandaloneArgumentPatternNodes
    )).apply(q)
  }
}
