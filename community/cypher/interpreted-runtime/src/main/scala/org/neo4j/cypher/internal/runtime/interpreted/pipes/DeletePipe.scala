/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.opencypher.v9_0.util.CypherTypeException
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.GraphElementPropertyFunctions
import org.opencypher.v9_0.util.attribution.Id
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.{RelationshipValue, NodeValue, PathValue}

import scala.collection.JavaConverters._

case class DeletePipe(src: Pipe, expression: Expression, forced: Boolean)
                     (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(src) with GraphElementPropertyFunctions {


  override protected def internalCreateResults(input: Iterator[ExecutionContext],
                                               state: QueryState): Iterator[ExecutionContext] = {
    input.map { row =>
      expression(row, state) match {
        case Values.NO_VALUE => // do nothing
        case r: RelationshipValue =>
          deleteRelationship(r, state)
        case n: NodeValue =>
          deleteNode(n, state)
        case p: PathValue =>
          deletePath(p, state)
        case other =>
          throw new CypherTypeException(s"Expected a Node, Relationship or Path, but got a ${other.getClass.getSimpleName}")
      }
      row
    }
  }

  private def deleteNode(n: NodeValue, state: QueryState) = if (!state.query.nodeOps.isDeletedInThisTx(n.id())) {
    if (forced) state.query.detachDeleteNode(n.id())
    else state.query.nodeOps.delete(n.id())
  }

  private def deleteRelationship(r: RelationshipValue, state: QueryState) =
    if (!state.query.relationshipOps.isDeletedInThisTx(r.id())) state.query.relationshipOps.delete(r.id())

  private def deletePath(p: PathValue, state: QueryState) = p.asList().iterator().asScala.foreach {
    case n: NodeValue =>
      deleteNode(n, state)
    case r: RelationshipValue =>
      deleteRelationship(r, state)
    case other =>
      throw new CypherTypeException(s"Expected a Node or Relationship, but got a ${other.getClass.getSimpleName}")
  }
}
