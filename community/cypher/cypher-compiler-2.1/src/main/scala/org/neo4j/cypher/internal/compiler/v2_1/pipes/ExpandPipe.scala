/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.pipes

import org.neo4j.cypher.internal.compiler.v2_1.symbols._
import org.neo4j.cypher.internal.compiler.v2_1.ExecutionContext
import org.neo4j.cypher.InternalException
import org.neo4j.graphdb.{Relationship, Direction, Node}

case class ExpandPipe(source: Pipe, from: String, relName: String, to: String, dir: Direction, types: Seq[String])
                     (implicit pipeMonitor: PipeMonitor) extends PipeWithSource(source, pipeMonitor) {
  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    input.flatMap {
      row =>
        getFromNode(row) match {
          case n: Node =>
            val relationships: Iterator[Relationship] = state.query.getRelationshipsFor(n, dir, types)
            relationships.map {
              case r => row.newWith(Seq(relName -> r, to -> r.getOtherNode(n)))
            }

          case null => None

          case value => throw new InternalException(s"Expected to find a node at $from but found $value instead")
        }
    }
  }

  def getFromNode(row: ExecutionContext): Any =
    row.getOrElse(from, throw new InternalException(s"Expected to find a node at $from but found nothing"))

  def executionPlanDescription =
    source.executionPlanDescription.
      andThen(this, "Expand", "from" -> from, "to" -> to, "relName" -> relName)

  val symbols = source.symbols.add(to, CTNode).add(relName, CTRelationship)
}
