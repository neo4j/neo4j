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
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.{EdgeValue, NodeValue, PathValue}

import scala.collection.JavaConverters._

case class DeletePipe(src: Pipe, expression: Expression, forced: Boolean)
                     (val id: Id = new Id)
  extends PipeWithSource(src) with GraphElementPropertyFunctions {


  override protected def internalCreateResults(input: Iterator[ExecutionContext],
                                               state: QueryState): Iterator[ExecutionContext] = {
    input.map { row =>
      expression(row)(state) match {
        case Values.NO_VALUE => // do nothing
        case r: EdgeValue =>
          deleteRelationship(r)(state)
        case n: NodeValue =>
          deleteNode(n)(state)
        case p: PathValue =>
          deletePath(p)(state)
        case other =>
          throw new CypherTypeException(s"Expected a Node, Relationship or Path, but got a ${other.getClass.getSimpleName}")
      }
      row
    }
  }

  private def deleteNode(n: NodeValue)(implicit state: QueryState) = if (!state.query.nodeOps.isDeletedInThisTx(n.id())) {
    if (forced) state.query.detachDeleteNode(n.id())
    else state.query.nodeOps.delete(n.id())
  }

  private def deleteRelationship(r: EdgeValue)(implicit state: QueryState) =
    if (!state.query.relationshipOps.isDeletedInThisTx(r.id())) state.query.relationshipOps.delete(r.id())

  private def deletePath(p: PathValue)(implicit state: QueryState) = p.asList().iterator().asScala.foreach {
    case n: NodeValue =>
      deleteNode(n)
    case r: EdgeValue =>
      deleteRelationship(r)
    case other =>
      throw new CypherTypeException(s"Expected a Node or Relationship, but got a ${other.getClass.getSimpleName}")
  }
}
