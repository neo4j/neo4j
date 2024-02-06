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
package org.neo4j.router.impl.query

import org.neo4j.cypher.internal.ast._
import org.neo4j.router.util.Errors

class StaticUseEvaluation {

  /**
   * This function finds graph selections only on the highest level without descending into sub-queries.
   * It returns one item for Single Queries and multiple items for Union Queries.
   */
  def evaluateStaticTopQueriesGraphSelections(statement: Statement): Seq[Option[CatalogName]] =
    topQueriesGraphSelections(statement).map(maybeGraphSelection => maybeGraphSelection.map(evaluateStatic))

  private def evaluateStaticOption(graphSelection: GraphSelection): Option[CatalogName] =
    graphSelection.graphReference match {
      case s: GraphDirectReference => Some(s.catalogName)
      case _           => None
    }

  private def evaluateStatic(graphSelection: GraphSelection): CatalogName =
    evaluateStaticOption(graphSelection).getOrElse(Errors.dynamicGraphNotAllowed(graphSelection))

  private def singleQueries(statement: Statement): Seq[SingleQuery] =
    statement match {
      case sq: SingleQuery => Seq(sq)
      case union: Union    => singleQueries(union.lhs) ++ singleQueries(union.rhs)
      case _               => Seq()
    }

  private def topQueriesGraphSelections(statement: Statement): Seq[Option[GraphSelection]] = {
    statement match {
      case sc: SchemaCommand => Seq(sc.useGraph)
      case _                 => queryTopGraphSelections(statement)
    }
  }

  private def queryTopGraphSelections(statement: Statement): Seq[Option[GraphSelection]] = {
    singleQueries(statement).map(singleQuery => {
      val clause = singleQuery.clauses.headOption
      clause.collect {
        case gs: GraphSelection => gs
      }
    })
  }
}
