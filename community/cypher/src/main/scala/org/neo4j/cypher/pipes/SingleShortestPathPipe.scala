/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.cypher.pipes

import org.neo4j.graphalgo.GraphAlgoFactory
import org.neo4j.cypher.commands.ShortestPath
import java.lang.String
import org.neo4j.graphdb.{Expander, Node}

class SingleShortestPathPipe(source: Pipe, ast: ShortestPath) extends ShortestPathPipe(source,ast) {

  override protected def findResult[U](expander: Expander, start: Node, end: Node, f: (Map[String, Any]) => U, depth: Int, m: Map[String, Any]) {
    val finder = GraphAlgoFactory.shortestPath(expander, depth)
    val findSinglePath = finder.findSinglePath(start, end)

    (findSinglePath, optional) match {
      case (null, true) => f(m ++ Map(pathName -> null))
      case (null, false) =>
      case (path, _) => f(m ++ Map(pathName -> path))
    }
  }

  override def executionPlan(): String = source.executionPlan() + "\r\n" + "SingleShortestPath(" + ast + ")"
}