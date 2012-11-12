package org.neo4j.cypher.internal.executionplan.builders

/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import org.neo4j.cypher.internal.commands.Predicate
import org.neo4j.cypher.internal.executionplan.{QueryToken, Unsolved, PartiallySolvedQuery, PlanBuilder}
import org.neo4j.cypher.internal.pipes.{FilterPipe, Pipe}

class FilterBuilder extends PlanBuilder {
  def apply(p: Pipe, q: PartiallySolvedQuery) = {
    val item = q.where.filter(pred => yesOrNo(pred, p))
    val pred: Predicate = item.map(_.token).reduce(_ ++ _)
    val newPipe = new FilterPipe(p, pred)
    val newQuery = q.where.filterNot(item.contains) ++ item.map(_.solve)

    (newPipe, q.copy(where = newQuery))
  }

  private def yesOrNo(q: QueryToken[_], p: Pipe) = q match {
    case Unsolved(pred: Predicate) => p.symbols.satisfies(pred.dependencies)
    case _ => false
  }

  def isDefinedAt(p: Pipe, q: PartiallySolvedQuery) = q.where.exists(pred => yesOrNo(pred, p))

  def priority: Int = PlanBuilder.Filter
}