/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_1.ast.convert.plannerQuery

import org.neo4j.cypher.internal.compiler.v3_1.helpers.CollectionSupport
import org.neo4j.cypher.internal.compiler.v3_1.planner._
import org.neo4j.cypher.internal.compiler.v3_1.planner.logical.plans.IdName
import org.neo4j.cypher.internal.frontend.v3_1.SemanticTable

case class PlannerQueryBuilder(private val q: PlannerQuery, semanticTable: SemanticTable, returns: Seq[IdName] = Seq.empty)
  extends CollectionSupport {

  def withReturns(returns: Seq[IdName]): PlannerQueryBuilder = copy(returns = returns)

  def amendQueryGraph(f: QueryGraph => QueryGraph): PlannerQueryBuilder =
    copy(q = q.updateTailOrSelf(_.amendQueryGraph(f)))

  def withHorizon(horizon: QueryHorizon): PlannerQueryBuilder =
    copy(q = q.updateTailOrSelf(_.withHorizon(horizon)))

  def withTail(newTail: PlannerQuery): PlannerQueryBuilder = {
    copy(q = q.updateTailOrSelf(_.withTail(newTail)))
  }

  def currentlyAvailableVariables: Set[IdName] = {
    q.allPlannerQueries.init.foldLeft(Set.empty[IdName]) {
      case (acc, current) if current.horizon.isInstanceOf[QueryProjection] => // actual projection
        current.horizon.exposedSymbols(current.queryGraph)
      case (acc, current) => // with * or horizon which simply introduces new symbols (unwind or load csv)
        current.horizon.exposedSymbols(current.queryGraph) ++ acc
    } ++ q.allPlannerQueries.last.lastQueryGraph.allCoveredIds // for the last planner query we should not consider the return projection
  }

  def currentQueryGraph: QueryGraph = q.lastQueryGraph

  def allSeenPatternNodes: Set[IdName] = {
    val previousPatternNodes = q.allPlannerQueries.init.foldLeft(Set.empty[IdName]) { (acc, current) =>
      val projectedNodes = current.horizon.exposedSymbols(current.queryGraph).collect {
        case id@IdName(n) if semanticTable.contains(n) && semanticTable.isNode(n) => id
      }
      projectedNodes ++ current.queryGraph.allPatternNodes
    }
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
        .amendQueryGraph(_.withOptionalMatches(newOptionalMatches))
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

    val fixedArgumentIds = q.foldMap {
      case (head, tail) =>
        val symbols = head.horizon.exposedSymbols(head.queryGraph)
        val newTailGraph = tail.queryGraph.withArgumentIds(symbols)
        tail.withQueryGraph(newTailGraph)
    }

    def groupInequalities(plannerQuery: PlannerQuery): PlannerQuery = {
      import org.neo4j.cypher.internal.frontend.v3_1.helpers.NonEmptyList._

      plannerQuery
        .amendQueryGraph(_.mapSelections {
          case Selections(predicates) =>
            val optPredicates = predicates.toNonEmptyListOption
            val newPredicates = optPredicates.map { predicates =>
              groupInequalityPredicates(predicates).toList.toSet
            }.getOrElse(predicates)
            Selections(newPredicates)
        })
      .updateTail(groupInequalities)
    }

    val withFixedOptionalMatchArgumentIds = fixArgumentIdsOnOptionalMatch(fixedArgumentIds)
    val withFixedMergeArgumentIds = fixArgumentIdsOnMerge(withFixedOptionalMatchArgumentIds)
    groupInequalities(withFixedMergeArgumentIds)
  }
}

object PlannerQueryBuilder {
  def apply(semanticTable: SemanticTable) = new PlannerQueryBuilder(PlannerQuery.empty, semanticTable)
}
