/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.ast.convert.plannerQuery

import org.neo4j.cypher.internal.compiler.v2_3.helpers.CollectionSupport
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_3.planner.{Selections, PlannerQuery, QueryGraph, QueryHorizon}
import org.neo4j.cypher.internal.frontend.v2_3.helpers.NonEmptyList._

case class PlannerQueryBuilder(private val q: PlannerQuery, returns: Seq[IdName] = Seq.empty)
  extends CollectionSupport {

  def withReturns(returns: Seq[IdName]): PlannerQueryBuilder = copy(returns = returns)

  def updateGraph(f: QueryGraph => QueryGraph): PlannerQueryBuilder =
    copy(q = q.updateTailOrSelf(_.updateGraph(f)))

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
    current.graph
  }

  def build(): PlannerQuery = {

    def fixArgumentIdsOnOptionalMatch(plannerQuery: PlannerQuery): PlannerQuery = {
      val optionalMatches = plannerQuery.graph.optionalMatches
      val (_, newOptionalMatches) = optionalMatches.foldMap(plannerQuery.graph.coveredIds) { case (args, qg) =>
        (args ++ qg.allCoveredIds, qg.withArgumentIds(args intersect qg.allCoveredIds))
      }
      plannerQuery
        .updateGraph(_.withOptionalMatches(newOptionalMatches))
        .updateTail(fixArgumentIdsOnOptionalMatch)
    }

    val fixedArgumentIds = q.foldMap {
      case (head, tail) =>
        val symbols = head.horizon.exposedSymbols(head.graph)
        val newTailGraph = tail.graph.withArgumentIds(symbols)
        tail.withGraph(newTailGraph)
    }

    def groupInequalities(plannerQuery: PlannerQuery): PlannerQuery = {
      plannerQuery
        .updateGraph(_.mapSelections {
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

    withGroupedInequalities
  }
}

object PlannerQueryBuilder {
  val empty = new PlannerQueryBuilder(PlannerQuery.empty)
}
