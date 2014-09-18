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
package org.neo4j.cypher.internal.compiler.v2_2.pipes

import org.neo4j.cypher.internal.compiler.v2_2.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_2.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v2_2.commands.expressions.ParameterExpression
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v2_2.planDescription.PlanDescription.Arguments.IntroducedIdentifier
import org.neo4j.cypher.internal.compiler.v2_2.planDescription.{NoChildren, PlanDescriptionImpl}
import org.neo4j.cypher.internal.compiler.v2_2.symbols.{CTNode, SymbolTable}
import org.neo4j.cypher.internal.helpers.{IsCollection, CollectionSupport}
import org.neo4j.graphdb.Node

sealed trait EntityByIdRhs {
  def expressions(ctx: ExecutionContext, state: QueryState): Iterable[Any]
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
                           (val estimatedCardinality: Option[Long] = None)(implicit pipeMonitor: PipeMonitor)
  extends Pipe
  with CollectionSupport
  with RonjaPipe {

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val ctx = state.initialContext.getOrElse(ExecutionContext.empty)
    val nodeIds = nodeIdsExpr.expressions(ctx, state)
    new NodeIdSeekIterator(ident, ctx, state.query.nodeOps, nodeIds.iterator)
  }

  def exists(predicate: Pipe => Boolean): Boolean = predicate(this)

  def planDescription = new PlanDescriptionImpl(this, "NodeByIdSeek", NoChildren, Seq(IntroducedIdentifier(ident)))

  def symbols: SymbolTable = new SymbolTable(Map(ident -> CTNode))

  override def monitor = pipeMonitor

  def dup(sources: List[Pipe]): Pipe = {
    require(sources.isEmpty)
    this
  }

  def sources: Seq[Pipe] = Seq.empty

  override def localEffects = Effects.READS_NODES

  def setEstimatedCardinality(estimated: Long) = copy()(Some(estimated))
}
