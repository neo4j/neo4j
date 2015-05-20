/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.pipes

import org.neo4j.cypher.internal.compiler.v2_2.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_2.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v2_2.commands.expressions.ParameterExpression
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.{ReadsNodes, Effects}
import org.neo4j.cypher.internal.compiler.v2_2.helpers.{IsCollection, CollectionSupport}
import org.neo4j.cypher.internal.compiler.v2_2.planDescription.{NoChildren, PlanDescriptionImpl}
import org.neo4j.cypher.internal.compiler.v2_2.symbols.{CTNode, SymbolTable}

sealed trait EntityByIdRhs {
  def expressions(ctx: ExecutionContext, state: QueryState): Iterable[Any]
}

case class EntityByIdExpression(expression: Expression) extends EntityByIdRhs {
  def expressions(ctx: ExecutionContext, state: QueryState) =
    expression(ctx)(state) match {
      case IsCollection(values) => values
    }
}

case class EntityByIdParameter(parameter: ParameterExpression) extends EntityByIdRhs {
  def expressions(ctx: ExecutionContext, state: QueryState) =
    parameter(ctx)(state) match {
      case IsCollection(values) => values
    }
}

case class EntityByIdExprs(exprs: Seq[Expression]) extends EntityByIdRhs {
  def expressions(ctx: ExecutionContext, state: QueryState) =
    exprs.map(_.apply(ctx)(state))
}

case class NodeByIdSeekPipe(ident: String, nodeIdsExpr: EntityByIdRhs)
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

  def planDescription = new PlanDescriptionImpl(this, "NodeByIdSeek", NoChildren, Seq(), identifiers)

  def symbols: SymbolTable = new SymbolTable(Map(ident -> CTNode))

  override def monitor = pipeMonitor

  def dup(sources: List[Pipe]): Pipe = {
    require(sources.isEmpty)
    this
  }

  def sources: Seq[Pipe] = Seq.empty

  override def localEffects = Effects(ReadsNodes)

  def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))
}
