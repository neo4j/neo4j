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

import org.neo4j.cypher.internal.compiler.v2_3.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.KeyToken
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v2_3.spi.QueryContext
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.{SemanticDirection, CypherTypeException}
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.graphdb.Node

case class GetDegree(node: Expression, typ: Option[KeyToken], direction: SemanticDirection) extends NullInNullOutExpression(node) {

  val getDegree: (QueryContext, Long) => Int = typ match {
    case None    => (qtx, node) => qtx.nodeGetDegree(node, direction)
    case Some(t) => (qtx, node) => t.getOptId(qtx) match {
      case None            => 0
      case Some(relTypeId) => qtx.nodeGetDegree(node, direction, relTypeId)
    }
  }

  def compute(value: Any, m: ExecutionContext)(implicit state: QueryState): Any = value match {
    case n: Node => getDegree(state.query, n.getId)
    case other   => throw new CypherTypeException(s"Type mismatch: expected a node but was $other of type ${other.getClass.getSimpleName}")
  }

  def arguments: Seq[Expression] = Seq(node)

  def rewrite(f: (Expression) => Expression): Expression = f(GetDegree(node.rewrite(f), typ, direction))

  protected def calculateType(symbols: SymbolTable): CypherType = CTInteger

  def symbolTableDependencies: Set[String] = node.symbolTableDependencies
}
