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
package org.neo4j.cypher.internal.compiler.v3_2.pipes

import org.neo4j.cypher.internal.compiler.v3_2.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_2.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v3_2.helpers.{IsList, ListSupport}
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.Id

sealed trait SeekArgs {
  def expressions(ctx: ExecutionContext, state: QueryState): Iterable[Any]
  def registerOwningPipe(pipe: Pipe): Unit
}

object SeekArgs {
  object empty extends SeekArgs {
    def expressions(ctx: ExecutionContext, state: QueryState): Iterable[Any] = Iterable.empty

    override def registerOwningPipe(pipe: Pipe){}
  }
}

case class SingleSeekArg(expr: Expression) extends SeekArgs {
  def expressions(ctx: ExecutionContext, state: QueryState): Iterable[Any] =
    expr(ctx)(state) match {
      case value => Iterable(value)
    }

  override def registerOwningPipe(pipe: Pipe): Unit = expr.registerOwningPipe(pipe)
}

case class ManySeekArgs(coll: Expression) extends SeekArgs {
  def expressions(ctx: ExecutionContext, state: QueryState): Iterable[Any] = {
    coll(ctx)(state) match {
      case IsList(values) => values
    }
  }

  override def registerOwningPipe(pipe: Pipe): Unit = coll.registerOwningPipe(pipe)
}

case class NodeByIdSeekPipe(ident: String, nodeIdsExpr: SeekArgs)
                           (val id: Id = new Id)
                           (implicit pipeMonitor: PipeMonitor)
  extends Pipe
  with ListSupport  {

  nodeIdsExpr.registerOwningPipe(this)

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val ctx = state.createOrGetInitialContext()
    val nodeIds = nodeIdsExpr.expressions(ctx, state)
    new NodeIdSeekIterator(ident, ctx, state.query.nodeOps, nodeIds.iterator)
  }

  override def monitor = pipeMonitor
}
