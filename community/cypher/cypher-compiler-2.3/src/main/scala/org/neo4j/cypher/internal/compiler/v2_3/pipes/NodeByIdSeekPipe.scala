/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.pipes

import org.neo4j.cypher.internal.compiler.v2_3.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{ReadsAllNodes, Effects}
import org.neo4j.cypher.internal.compiler.v2_3.helpers.{CollectionSupport, IsCollection}
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.{NoChildren, PlanDescriptionImpl}
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.symbols.CTNode

sealed trait SeekArgs {
  def expressions(ctx: ExecutionContext, state: QueryState): Iterable[Any]
}

object SeekArgs {
  object empty extends SeekArgs {
    def expressions(ctx: ExecutionContext, state: QueryState): Iterable[Any] = Iterable.empty
  }
}

case class SingleSeekArg(expr: Expression) extends SeekArgs {
  def expressions(ctx: ExecutionContext, state: QueryState): Iterable[Any] =
    expr(ctx)(state) match {
      case value => Iterable(value)
    }
}

case class ManySeekArgs(coll: Expression) extends SeekArgs {
  def expressions(ctx: ExecutionContext, state: QueryState): Iterable[Any] = {
    coll(ctx)(state) match {
      case IsCollection(values) => values
    }
  }
}

case class NodeByIdSeekPipe(ident: String, nodeIdsExpr: SeekArgs)
                           (val estimatedCardinality: Option[Double] = None)(implicit pipeMonitor: PipeMonitor)
  extends Pipe
  with CollectionSupport
  with RonjaPipe {

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    //register as parent so that stats are associated with this pipe
    state.decorator.registerParentPipe(this)

    val ctx = state.initialContext.getOrElse(ExecutionContext.empty)
    val nodeIds = nodeIdsExpr.expressions(ctx, state)
    new NodeIdSeekIterator(ident, ctx, state.query.nodeOps, nodeIds.iterator)
  }

  def exists(predicate: Pipe => Boolean): Boolean = predicate(this)

  def planDescriptionWithoutCardinality = new PlanDescriptionImpl(this.id, "NodeByIdSeek", NoChildren, Seq(), identifiers)

  def symbols: SymbolTable = new SymbolTable(Map(ident -> CTNode))

  override def monitor = pipeMonitor

  def dup(sources: List[Pipe]): Pipe = {
    require(sources.isEmpty)
    this
  }

  def sources: Seq[Pipe] = Seq.empty

  override def localEffects = Effects(ReadsAllNodes)

  def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))
}
