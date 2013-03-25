/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.commands.expressions

import org.neo4j.cypher.internal.symbols.{CollectionType, LabelType, NodeType, SymbolTable}
import org.neo4j.cypher.internal.ExecutionContext
import org.neo4j.graphdb.Node
import org.neo4j.cypher.CypherTypeException
import org.neo4j.cypher.internal.pipes.QueryState
import org.neo4j.cypher.internal.spi.QueryContext

case class LabelsFunction(nodeExpr: Expression) extends Expression {

  override def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = nodeExpr(ctx) match {
    case n: Node =>
      val queryCtx: QueryContext = state.query
      queryCtx.getLabelsForNode(n.getId).map { queryCtx.getLabelName(_) }.toSeq
    case _ =>
      throw new CypherTypeException("labels() expected a Node but was called with something else")
  }

  def rewrite(f: (Expression) => Expression) = f(LabelsFunction(nodeExpr.rewrite(f)))

  def children = Seq(nodeExpr)

  def symbolTableDependencies = nodeExpr.symbolTableDependencies

  protected def calculateType(symbols: SymbolTable) = {
    nodeExpr.evaluateType(NodeType(), symbols)
    new CollectionType(LabelType())
  }
}