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
import org.neo4j.cypher.internal.compiler.v2_1.commands.Predicate
import org.neo4j.cypher.internal.compiler.v2_1.PlanDescription.Arguments.IntroducedIdentifier

case class OptionalExpandPipe(source: Pipe, from: String, relName: String, to: String, dir: Direction, types: Seq[String], predicate: Predicate)
                     (implicit pipeMonitor: PipeMonitor) extends PipeWithSource(source, pipeMonitor) {
  val nulls: ExecutionContext =
    ExecutionContext.empty.newWith(Seq(relName -> null, to -> null))

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {

    implicit val s = state

    input.flatMap {
      row =>
        val fromNode = getFromNode(row)
        fromNode match {
          case n: Node =>
            val relationships = state.query.getRelationshipsFor(n, dir, types)
            val contextWithRelationships = relationships.map {
              case r => row.newWith(Seq(relName -> r, to -> r.getOtherNode(n)))
            }.filter(ctx => predicate.isTrue(ctx))

            if (contextWithRelationships.hasNext) {
              contextWithRelationships
            } else {
              Iterator(row ++ nulls)
            }

          case value if value == null =>
            Iterator(row ++ nulls)

          case value =>
            throw new InternalException(s"Expected to find a node at $from but found $value instead")
        }
    }
  }

  def getFromNode(row: ExecutionContext): Any =
    row.getOrElse(from, throw new InternalException(s"Expected to find a node at $from but found nothing"))

  def planDescription =
    source.planDescription.
      andThen(this, "OptionalExpand", IntroducedIdentifier(relName), IntroducedIdentifier(to))

  def symbols = source.symbols.add(to, CTNode).add(relName, CTRelationship)
}
