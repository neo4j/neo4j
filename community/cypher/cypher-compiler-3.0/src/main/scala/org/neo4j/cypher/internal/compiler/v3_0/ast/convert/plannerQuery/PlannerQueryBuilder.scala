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
package org.neo4j.cypher.internal.compiler.v3_0.ast.convert.plannerQuery

import org.neo4j.cypher.internal.compiler.v3_0.helpers.CollectionSupport
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v3_0.planner.{UpdateGraph, Selections, PlannerQuery, QueryGraph, QueryHorizon}
import org.neo4j.cypher.internal.frontend.v3_0.helpers.NonEmptyList._

case class PlannerQueryBuilder(private val q: PlannerQuery, returns: Seq[IdName] = Seq.empty)
  extends CollectionSupport {

  def withReturns(returns: Seq[IdName]): PlannerQueryBuilder = copy(returns = returns)

  def updateQueryGraph(f: QueryGraph => QueryGraph): PlannerQueryBuilder =
    copy(q = q.updateTailOrSelf(_.updateQueryGraph(f)))

  def updateUpdateGraph(f: UpdateGraph => UpdateGraph): PlannerQueryBuilder =
    copy(q = q.updateTailOrSelf(_.updateUpdateGraph(f)))

  def withHorizon(horizon: QueryHorizon): PlannerQueryBuilder =
    copy(q = q.updateTailOrSelf(_.withHorizon(horizon)))

  def withTail(newTail: PlannerQuery): PlannerQueryBuilder = {
    copy(q = q.updateTailOrSelf(_.withTail(newTail)))
  }

  def currentlyAvailableIdentifiers: Set[IdName] =
    currentQueryGraph.coveredIds

  def currentQueryGraph: QueryGraph = {
    var current = q
    while (current.tail.nonEmpty) {
      current = current.tail.get
    }
    current.queryGraph
  }

  def readOnly: Boolean = q.updateGraph.isEmpty

  def build(): PlannerQuery = {

    def fixArgumentIdsOnOptionalMatch(plannerQuery: PlannerQuery): PlannerQuery = {
      val optionalMatches = plannerQuery.queryGraph.optionalMatches
      val (_, newOptionalMatches) = optionalMatches.foldMap(plannerQuery.queryGraph.coveredIds) { case (args, qg) =>
        (args ++ qg.allCoveredIds, qg.withArgumentIds(args intersect qg.allCoveredIds))
      }
      plannerQuery
        .updateQueryGraph(_.withOptionalMatches(newOptionalMatches))
        .updateTail(fixArgumentIdsOnOptionalMatch)
    }

    val fixedArgumentIds = q.foldMap {
      case (head, tail) =>
        val symbols = head.horizon.exposedSymbols(head.queryGraph)
        val newTailGraph = tail.queryGraph.withArgumentIds(symbols)
        tail.withQueryGraph(newTailGraph)
    }

    def groupInequalities(plannerQuery: PlannerQuery): PlannerQuery = {
      plannerQuery
        .updateQueryGraph(_.mapSelections {
          case Selections(predicates) =>
            val optPredicates = predicates.toNonEmptyListOption
            val newPredicates = optPredicates.map { predicates =>
              groupInequalityPredicates(predicates).toList.toSet
            }.getOrElse(predicates)
            Selections(newPredicates)
        })
      .updateTail(groupInequalities)
    }

    val withFixedArgumentIds = fixArgumentIdsOnOptionalMatch(fixedArgumentIds)
    val withGroupedInequalities = groupInequalities(withFixedArgumentIds)

    withGroupedInequalities.withUpdateGraph(updateGraph = q.updateGraph)
  }
}

object PlannerQueryBuilder {
  val empty = new PlannerQueryBuilder(PlannerQuery.empty)
}
