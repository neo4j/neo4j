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

import org.neo4j.cypher.internal.executionplan.{Unsolved, QueryToken, PartiallySolvedQuery, PlanBuilder}
import org.neo4j.cypher.internal.commands.ShortestPath
import org.neo4j.cypher.internal.pipes.{SingleShortestPathPipe, AllShortestPathsPipe, Pipe}
import org.neo4j.cypher.SyntaxException

class ShortestPathBuilder extends PlanBuilder {
  def apply(p: Pipe, q: PartiallySolvedQuery) = {
    val item = q.patterns.filter(yesOrNo(p, _)).head
    val shortestPath = item.token.asInstanceOf[ShortestPath]

    val pipe = if (shortestPath.single)
      new SingleShortestPathPipe(p, shortestPath)
    else
      new AllShortestPathsPipe(p, shortestPath)


    (pipe, q.copy(patterns = q.patterns.filterNot(_ == item) :+ item.solve))
  }

  def isDefinedAt(p: Pipe, q: PartiallySolvedQuery) = q.patterns.exists(yesOrNo(p, _))

  private def yesOrNo(p: Pipe, token: QueryToken[_]): Boolean = token match {
    case Unsolved(sp: ShortestPath) => p.symbols.satisfies(sp.dependencies)
    case _ => false
  }

  def priority: Int = PlanBuilder.ShortestPath

  def checkForUnsolvedShortestPaths(querySoFar: PartiallySolvedQuery, pipe: Pipe) {
    val unsolvedShortestPaths = querySoFar.patterns.
      filterNot(_.solved).
      filter(_.token.isInstanceOf[ShortestPath]).
      map(_.token.asInstanceOf[ShortestPath])

    if (unsolvedShortestPaths.nonEmpty) {
      unsolvedShortestPaths.foreach(qt => {
        val missing = pipe.symbols.missingDependencies(qt.dependencies).map(_.name).mkString("`", "`,`", "`")
        throw new SyntaxException("To find a shortest path, both ends of the path need to be provided. Couldn't find " + missing)
      })
    }
  }

}