/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.Expression
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.mutation.GraphElementPropertyFunctions
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.Id
import org.neo4j.cypher.internal.frontend.v3_3.CypherTypeException
import org.neo4j.graphdb.{Node, Path, Relationship}

import scala.collection.JavaConverters._

case class DeletePipe(src: Pipe, expression: Expression, forced: Boolean)
                     (val id: Id = new Id)
  extends PipeWithSource(src) with GraphElementPropertyFunctions {


  override protected def internalCreateResults(input: Iterator[ExecutionContext],
                                               state: QueryState): Iterator[ExecutionContext] = {
    input.map { row =>
      expression(row)(state) match {
        case null => // do nothing
        case r: Relationship =>
          deleteRelationship(r)(state)
        case n: Node =>
          deleteNode(n)(state)
        case p: Path =>
          deletePath(p)(state)
        case other =>
          throw new CypherTypeException(s"Expected a Node, Relationship or Path, but got a ${other.getClass.getSimpleName}")
      }
      row
    }
  }

  private def deleteNode(n: Node)(implicit state: QueryState) = if (!state.query.nodeOps.isDeletedInThisTx(n)) {
    if (forced) state.query.detachDeleteNode(n)
    else state.query.nodeOps.delete(n)
  }

  private def deleteRelationship(r: Relationship)(implicit state: QueryState) =
    if (!state.query.relationshipOps.isDeletedInThisTx(r)) state.query.relationshipOps.delete(r)

  private def deletePath(p: Path)(implicit state: QueryState) = p.iterator().asScala.foreach {
    case n: Node =>
      deleteNode(n)
    case r: Relationship =>
      deleteRelationship(r)
    case other =>
      throw new CypherTypeException(s"Expected a Node or Relationship, but got a ${other.getClass.getSimpleName}")
  }
}
