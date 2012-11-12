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
package org.neo4j.cypher.internal.executionplan.builders

import org.neo4j.cypher.internal.commands.NamedPath
import org.neo4j.cypher.internal.executionplan.{Unsolved, QueryToken, PartiallySolvedQuery, PlanBuilder}
import org.neo4j.cypher.internal.pipes.{NamedPathPipe, Pipe}

class NamedPathBuilder extends PlanBuilder {
  def apply(p: Pipe, q: PartiallySolvedQuery) = {
    val item = q.namedPaths.filter(yesOrNo(_, p)).head
    val namedPaths = item.token

    val pipe = new NamedPathPipe(p, namedPaths)

    (pipe, q.copy(namedPaths = q.namedPaths.filterNot(_ == item) :+ item.solve))
  }

  def isDefinedAt(p: Pipe, q: PartiallySolvedQuery) = q.namedPaths.exists(yesOrNo(_, p))

  private def yesOrNo(q: QueryToken[_], p: Pipe) = q match {
    case Unsolved(np: NamedPath) => {
      p.symbols.satisfies(np.pathPattern.flatMap(_.possibleStartPoints))
    }
    case _ => false
  }

  def priority: Int = PlanBuilder.NamedPath
}