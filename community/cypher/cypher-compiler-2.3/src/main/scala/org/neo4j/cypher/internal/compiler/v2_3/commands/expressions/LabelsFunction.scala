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
package org.neo4j.cypher.internal.compiler.v2_3.commands.expressions

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v2_3.spi.QueryContext
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.CypherTypeException
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.graphdb.Node

case class LabelsFunction(nodeExpr: Expression) extends Expression {

  override def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = nodeExpr(ctx) match {
    case n: Node =>
      val queryCtx: QueryContext = state.query
      queryCtx.getLabelsForNode(n.getId).map { queryCtx.getLabelName }.toList
    case null =>
      null
    case _ =>
      throw new CypherTypeException("labels() expected a Node but was called with something else")
  }

  def rewrite(f: (Expression) => Expression) = f(LabelsFunction(nodeExpr.rewrite(f)))

  def arguments = Seq(nodeExpr)

  def symbolTableDependencies = nodeExpr.symbolTableDependencies

  protected def calculateType(symbols: SymbolTable) = {
    nodeExpr.evaluateType(CTNode, symbols)
    CTCollection(CTString)
  }

  override def localEffects(symbols: SymbolTable) = Effects()
}
