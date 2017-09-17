/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_4.ast.convert.plannerQuery

import org.neo4j.cypher.internal.compiler.v3_4.helpers.ListSupport
import org.neo4j.cypher.internal.frontend.v3_4.ast.RelationshipStartItem
import org.neo4j.cypher.internal.frontend.v3_4.helpers.UnNamedNameGenerator
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticTable
import org.neo4j.cypher.internal.ir.v3_4._
import org.neo4j.cypher.internal.v3_4.expressions.SemanticDirection

case class PlannerQueryBuilder(private val q: PlannerQuery, semanticTable: SemanticTable, returns: Seq[IdName] = Seq.empty)
  extends ListSupport {

  def withReturns(returns: Seq[IdName]): PlannerQueryBuilder = copy(returns = returns)

  def amendQueryGraph(f: QueryGraph => QueryGraph): PlannerQueryBuilder =
    copy(q = q.updateTailOrSelf(_.amendQueryGraph(f)))

  def withHorizon(horizon: QueryHorizon): PlannerQueryBuilder =
    copy(q = q.updateTailOrSelf(_.withHorizon(horizon)))

  def withTail(newTail: PlannerQuery): PlannerQueryBuilder = {
    copy(q = q.updateTailOrSelf(_.withTail(newTail.amendQueryGraph(_.addArgumentIds(currentlyExposedSymbols.toIndexedSeq)))))
  }

  private def currentlyExposedSymbols: Set[IdName] = {
    q.lastQueryHorizon.exposedSymbols(q.lastQueryGraph.allCoveredIds)
  }

  def currentlyAvailableVariables: Set[IdName] = {
    val allPlannerQueries = q.allPlannerQueries
    val previousAvailableSymbols = if (allPlannerQueries.length > 1) {
      val current = allPlannerQueries(allPlannerQueries.length - 2)
      current.horizon.exposedSymbols(current.queryGraph.allCoveredIds)
    } else Set.empty

    // for the last planner query we should not consider the return projection
    previousAvailableSymbols ++ q.lastQueryGraph.allCoveredIds
  }

  def currentQueryGraph: QueryGraph = q.lastQueryGraph

  def allSeenPatternNodes: Set[IdName] = {
    val allPlannerQueries = q.allPlannerQueries
    val previousPatternNodes = if (allPlannerQueries.length > 1) {
      val current = allPlannerQueries(allPlannerQueries.length - 2)
      val projectedNodes = current.horizon.exposedSymbols(current.queryGraph.allCoveredIds).collect {
        case id@IdName(n) if semanticTable.containsNode(n) => id
      }
      projectedNodes ++ current.queryGraph.allPatternNodes
    } else Set.empty
    previousPatternNodes ++ q.lastQueryGraph.allPatternNodes
  }

  def readOnly: Boolean = q.queryGraph.readOnly

  def build(): PlannerQuery = {

    def fixArgumentIdsOnOptionalMatch(plannerQuery: PlannerQuery): PlannerQuery = {
      val optionalMatches = plannerQuery.queryGraph.optionalMatches
      val (_, newOptionalMatches) = optionalMatches.foldMap(plannerQuery.queryGraph.coveredIds) { case (args, qg) =>
        (args ++ qg.allCoveredIds, qg.withArgumentIds(args intersect qg.allCoveredIds))
      }
      plannerQuery
        .amendQueryGraph(_.withOptionalMatches(newOptionalMatches.toIndexedSeq))
        .updateTail(fixArgumentIdsOnOptionalMatch)
    }

    def fixArgumentIdsOnMerge(plannerQuery: PlannerQuery): PlannerQuery = {
      val mergeMatchGraph = plannerQuery.queryGraph.mergeQueryGraph
      val newMergeMatchGraph = mergeMatchGraph.map {
        qg =>
          val requiredArguments = qg.coveredIdsExceptArguments intersect qg.argumentIds
          qg.withArgumentIds(requiredArguments)
      }
      plannerQuery.amendQueryGraph(qg => newMergeMatchGraph.map(qg.withMergeMatch).getOrElse(qg)).updateTail(fixArgumentIdsOnMerge)
    }

    def fixQueriesWithOnlyRelationshipIndex(plannerQuery: PlannerQuery): PlannerQuery = {
      val qg = plannerQuery.queryGraph
      val patternRelationships = qg.hints.collect {
        case r: RelationshipStartItem if !qg.patternRelationships.exists(_.name.name == r.name) =>
          val lNode = UnNamedNameGenerator.name(r.position)
          val rNode = UnNamedNameGenerator.name(r.position.bumped())

          PatternRelationship(IdName(r.name), (IdName(lNode), IdName(rNode)), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
      }

      val patternNodes = patternRelationships.flatMap(relationship => Set(relationship.nodes._1, relationship.nodes._2))
      plannerQuery
        .amendQueryGraph(_.addPatternRelationships(patternRelationships.toSeq).addPatternNodes(patternNodes.toSeq:_*))
        .updateTail(fixQueriesWithOnlyRelationshipIndex)
    }

    val fixedArgumentIds = q.foldMap {
      case (head, tail) =>
        val symbols = head.horizon.exposedSymbols(head.queryGraph.allCoveredIds)
        val newTailGraph = tail.queryGraph.withArgumentIds(symbols)
        tail.withQueryGraph(newTailGraph)
    }

    def groupInequalities(plannerQuery: PlannerQuery): PlannerQuery = {
      import org.neo4j.cypher.internal.apa.v3_4.NonEmptyList._

      plannerQuery
        .amendQueryGraph(_.mapSelections {
          case Selections(predicates) =>
            val optPredicates = predicates.toNonEmptyListOption
            val newPredicates: Set[Predicate] = optPredicates.map { predicates =>
              groupInequalityPredicates(predicates).toSet
            }.getOrElse(predicates)
            Selections(newPredicates)
        })
      .updateTail(groupInequalities)
    }

    val withFixedOptionalMatchArgumentIds = fixArgumentIdsOnOptionalMatch(fixedArgumentIds)
    val withFixedMergeArgumentIds = fixArgumentIdsOnMerge(withFixedOptionalMatchArgumentIds)
    val groupedInequalities = groupInequalities(withFixedMergeArgumentIds)
    fixQueriesWithOnlyRelationshipIndex(groupedInequalities)
  }
}

object PlannerQueryBuilder {
  def apply(semanticTable: SemanticTable) = new PlannerQueryBuilder(PlannerQuery.empty, semanticTable)
}
