/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.IsList
import org.neo4j.cypher.internal.util.v3_4.attribution.Id
import org.neo4j.values.virtual.{ListValue, VirtualValues}

import scala.collection.JavaConverters._

sealed trait SeekArgs {
  def expressions(ctx: ExecutionContext, state: QueryState): ListValue
  def registerOwningPipe(pipe: Pipe): Unit
}

object SeekArgs {
  object empty extends SeekArgs {
    def expressions(ctx: ExecutionContext, state: QueryState):  ListValue = VirtualValues.EMPTY_LIST

    override def registerOwningPipe(pipe: Pipe){}
  }
}

case class SingleSeekArg(expr: Expression) extends SeekArgs {
  def expressions(ctx: ExecutionContext, state: QueryState): ListValue =
    expr(ctx, state) match {
      case value => VirtualValues.list(value)
    }

  override def registerOwningPipe(pipe: Pipe): Unit = expr.registerOwningPipe(pipe)
}

case class ManySeekArgs(coll: Expression) extends SeekArgs {
  def expressions(ctx: ExecutionContext, state: QueryState): ListValue = {
    coll(ctx, state) match {
      case IsList(values) => values
    }
  }

  override def registerOwningPipe(pipe: Pipe): Unit = coll.registerOwningPipe(pipe)
}

case class NodeByIdSeekPipe(ident: String, nodeIdsExpr: SeekArgs)
                           (val id: Id = Id.INVALID_ID) extends Pipe {

  nodeIdsExpr.registerOwningPipe(this)

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val ctx = state.newExecutionContext(executionContextFactory)
    val nodeIds = nodeIdsExpr.expressions(ctx, state)
    new NodeIdSeekIterator(
      ident,
      ctx,
      executionContextFactory,
      state.query.nodeOps,
      nodeIds.iterator().asScala
    )
  }
}
