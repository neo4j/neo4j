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

import collection.Seq
import org.neo4j.cypher.internal.commands.{StartItem, NodeById}
import org.neo4j.graphdb.{Node, GraphDatabaseService}
import org.neo4j.cypher.internal.pipes.{NodeStartPipe, Pipe}
import GetGraphElements.getElements
import org.neo4j.cypher.internal.executionplan._

class NodeByIdBuilder(graph: GraphDatabaseService) extends PlanBuilder {
  def priority: Int = PlanBuilder.NodeById

  def apply(inPipe: Pipe, inQ: PartiallySolvedQuery): (Pipe, PartiallySolvedQuery) = {
      val startItemToken = interestingStartItems(inQ).head
      val Unsolved(NodeById(key, expression)) = startItemToken

      val pipe = new NodeStartPipe(inPipe, key, m => getElements[Node](expression(m), key, graph.getNodeById))

      val remainingQ:Seq[QueryToken[StartItem]] = inQ.start.filterNot(_ == startItemToken) :+ startItemToken.solve

      (pipe, inQ.copy(start = remainingQ))
    }

  def isDefinedAt(p: Pipe, q: PartiallySolvedQuery): Boolean = interestingStartItems(q).nonEmpty

  private def interestingStartItems(q: PartiallySolvedQuery): Seq[QueryToken[StartItem]] = q.start.filter({
    case Unsolved(NodeById(_, expression)) => true
    case _ => false
  })
}