/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery

import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsParameters
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery.PlannerQueryBuilder.inlineRelationshipTypePredicates
import org.neo4j.cypher.internal.compiler.helpers.ListSupport
import org.neo4j.cypher.internal.expressions.HasTypes
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.CallSubqueryHorizon
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.PlannerQueryPart
import org.neo4j.cypher.internal.ir.Predicate
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.QueryHorizon
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder

case class PlannerQueryBuilder(private val q: SinglePlannerQuery, semanticTable: SemanticTable)
  extends ListSupport {

  def amendQueryGraph(f: QueryGraph => QueryGraph): PlannerQueryBuilder =
    copy(q = q.updateTailOrSelf(_.amendQueryGraph(f)))

  def withHorizon(horizon: QueryHorizon): PlannerQueryBuilder =
    copy(q = q.updateTailOrSelf(_.withHorizon(horizon)))

  def withCallSubquery(subquery: PlannerQueryPart,
                       correlated: Boolean,
                       yielding: Boolean,
                       inTransactionsParameters: Option[InTransactionsParameters]): PlannerQueryBuilder = {
    withHorizon(CallSubqueryHorizon(subquery, correlated, yielding, inTransactionsParameters)).withTail(SinglePlannerQuery.empty)
  }

  def withTail(newTail: SinglePlannerQuery): PlannerQueryBuilder = {
    copy(q = q.updateTailOrSelf(_.withTail(newTail.amendQueryGraph(_.addArgumentIds(currentlyExposedSymbols.toIndexedSeq)))))
  }

  def withQueryInput(inputVariables : Seq[String]): PlannerQueryBuilder = {
    copy(q = q.withInput(inputVariables))
  }

  def withInterestingOrder(interestingOrder: InterestingOrder): PlannerQueryBuilder = {
    val existingIO = q.last.interestingOrder
    val newIO = InterestingOrder(interestingOrder.requiredOrderCandidate, existingIO.interestingOrderCandidates ++ interestingOrder.interestingOrderCandidates)
    copy(q = q.updateTailOrSelf(_.withInterestingOrder(newIO)))
  }

  def withPropagatedTailInterestingOrder(): PlannerQueryBuilder = {
    copy(q = q.withTailInterestingOrder(q.last.interestingOrder))
  }

  private def currentlyExposedSymbols: Set[String] = {
    q.lastQueryHorizon.exposedSymbols(q.lastQueryGraph.allCoveredIds)
  }

  def currentlyAvailableVariables: Set[String] = {
    val allPlannerQueries = q.allPlannerQueries
    val previousAvailableSymbols = if (allPlannerQueries.length > 1) {
      val current = allPlannerQueries(allPlannerQueries.length - 2)
      current.horizon.exposedSymbols(current.queryGraph.allCoveredIds)
    } else Set.empty

    // for the last planner query we should not consider the return projection
    previousAvailableSymbols ++ q.lastQueryGraph.allCoveredIds
  }

  def currentQueryGraph: QueryGraph = q.lastQueryGraph

  def allSeenPatternNodes: collection.Set[String] = {
    val qg = q.lastQueryGraph
    qg.allPatternNodes ++ qg.argumentIds.filter(semanticTable.containsNode)
  }

  def readOnly: Boolean = q.queryGraph.readOnly

  def build(): SinglePlannerQuery = {

    def fixArgumentIdsOnOptionalMatch(plannerQuery: SinglePlannerQuery): SinglePlannerQuery = {
      val optionalMatches = plannerQuery.queryGraph.optionalMatches
      val (_, newOptionalMatches) = optionalMatches.foldMap(plannerQuery.queryGraph.idsWithoutOptionalMatchesOrUpdates) {
        case (args, qg) =>
          (args ++ qg.allCoveredIds, qg.withArgumentIds(args intersect qg.dependencies))
      }
      plannerQuery
        .amendQueryGraph(_.withOptionalMatches(newOptionalMatches.toIndexedSeq))
        .updateTail(fixArgumentIdsOnOptionalMatch)
    }

    def fixArgumentIdsOnMerge(plannerQuery: SinglePlannerQuery): SinglePlannerQuery = {
      val newMergeMatchGraph = plannerQuery.queryGraph.mergeQueryGraph.map {
        qg =>
          val nodesAndRels = QueryGraph.coveredIdsForPatterns(qg.patternNodes, qg.patternRelationships)
          val predicateDependencies = qg.withoutArguments().dependencies
          val requiredArguments = nodesAndRels ++ predicateDependencies
          val availableArguments = qg.argumentIds
          qg.withArgumentIds(requiredArguments intersect availableArguments)
      }

      val updatePQ = newMergeMatchGraph match {
        case None =>
          plannerQuery
        case Some(qg) =>
          plannerQuery.amendQueryGraph(_.withMergeMatch(qg))
      }

      updatePQ.updateTail(fixArgumentIdsOnMerge)
    }

    val fixedArgumentIds = q.foldMap {
      case (head, tail) =>
        val symbols = head.horizon.exposedSymbols(head.queryGraph.allCoveredIds)
        val newTailGraph = tail.queryGraph.withArgumentIds(symbols)
        tail.withQueryGraph(newTailGraph)
    }

    def groupInequalities(plannerQuery: SinglePlannerQuery): SinglePlannerQuery = {

      plannerQuery
        .amendQueryGraph(_.mapSelections {
          case Selections(predicates) =>
            val newPredicates = groupInequalityPredicates(predicates.toSeq).toSet
            Selections(newPredicates)
        })
        .updateTail(groupInequalities)
    }

    val withFixedOptionalMatchArgumentIds = fixArgumentIdsOnOptionalMatch(fixedArgumentIds)
    val withFixedMergeArgumentIds = fixArgumentIdsOnMerge(withFixedOptionalMatchArgumentIds)
    val groupedInequalities = groupInequalities(withFixedMergeArgumentIds)
    val withInlinedTypePredicates = inlineRelationshipTypePredicates(groupedInequalities)
    withInlinedTypePredicates
  }
}

object PlannerQueryBuilder {
  def apply(semanticTable: SemanticTable): PlannerQueryBuilder =
    PlannerQueryBuilder(SinglePlannerQuery.empty, semanticTable)
  def apply(semanticTable: SemanticTable, argumentIds: Set[String]): PlannerQueryBuilder =
    PlannerQueryBuilder(RegularSinglePlannerQuery(queryGraph = QueryGraph(argumentIds = argumentIds)), semanticTable)

  def inlineRelationshipTypePredicates(plannerQuery: SinglePlannerQuery): SinglePlannerQuery = {
    plannerQuery.amendQueryGraph { qg =>
      val typePredicates: Map[String, (Predicate, Seq[RelTypeName])] = findRelationshipTypePredicatesPerSymbol(qg)

      final case class Result(newPatternRelationships: Set[PatternRelationship], inlinedPredicates: Set[Predicate])

      val result = qg.patternRelationships.foldLeft(Result(qg.patternRelationships, Set.empty)) {
        case (resultSoFar@Result(newPatternRelationships, inlinedPredicates), rel) =>
          if (rel.types.nonEmpty) resultSoFar
          else {
            typePredicates.get(rel.name).fold(resultSoFar) { case (pred, types) =>
              Result(newPatternRelationships - rel + rel.copy(types = types), inlinedPredicates + pred)
            }
          }
      }

      qg.copy(
        patternRelationships = result.newPatternRelationships,
        selections = qg.selections.copy(predicates = qg.selections.predicates -- result.inlinedPredicates)
      )
    }.updateTail(inlineRelationshipTypePredicates)
  }

  private def findRelationshipTypePredicatesPerSymbol(qg: QueryGraph): Map[String, (Predicate, Seq[RelTypeName])] = {
    qg.selections.predicates.foldLeft(Map.empty[String, (Predicate, Seq[RelTypeName])]) {
      // WHERE r:REL
      case (acc, pred@Predicate(_, HasTypes(Variable(name), relTypes))) =>
        acc + (name -> (pred -> relTypes))

      // WHERE r:REL OR r:OTHER_REL
      case (acc, pred@Predicate(_, Ors(HasTypes(Variable(name), headRelTypes) +: exprs))) =>
        val tailRelTypesOnTheSameVariable = exprs.collect {
          case HasTypes(Variable(`name`), relTypes) => relTypes
        }.toSeq

        // all predicates must refer to the same variable to be equivalent to [r:A|B|C]
        if (tailRelTypesOnTheSameVariable.length == exprs.length) {
          val oredRelTypes = (headRelTypes +: tailRelTypesOnTheSameVariable).flatten
          acc + (name -> (pred -> oredRelTypes))
        } else {
          acc
        }

      case (acc, _) =>
        acc
    }
  }
}
