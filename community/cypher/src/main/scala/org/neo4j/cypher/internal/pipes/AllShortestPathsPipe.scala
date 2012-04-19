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
package org.neo4j.cypher.internal.pipes

import org.neo4j.graphdb.{Node, Expander}
import java.lang.String
import org.neo4j.graphalgo.GraphAlgoFactory
import scala.collection.JavaConverters._
import org.neo4j.cypher.internal.commands.ShortestPath
import collection.mutable.Map

class AllShortestPathsPipe(source: Pipe, ast: ShortestPath) extends ShortestPathPipe(source, ast) {

  protected def findResult[U](expander: Expander, start: Node, end: Node, depth: Int, m: Map[String, Any]): Traversable[Map[String, Any]] = {
    val finder = GraphAlgoFactory.shortestPath(expander, depth)
    val foundPaths = finder.findAllPaths(start, end).asScala.toList

    (foundPaths, optional) match {
      case (List(), true) => Seq(m += pathName -> null)
      case (List(), false) => Seq()
      case (paths, _) => paths.map(path => {
        val newMap: Map[String, Any] = m.clone()
        newMap += pathName -> path
      })
    }
  }

  override def executionPlan(): String = source.executionPlan() + "\r\n" + "AllShortestPath(" + ast + ")"
}