/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.KeyToken
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.NodeValue

case class GetDegree(node: Expression, typ: Option[KeyToken], direction: SemanticDirection) extends NullInNullOutExpression(node) {

  val getDegree: (QueryState, Long) => Long = typ match {
    case None    => (state, node) => state.query.nodeGetDegree(node, direction, state.cursors.nodeCursor)
    case Some(t) => (state, node) => t.getOptId(state.query) match {
      case None            => 0
      case Some(relTypeId) => state.query.nodeGetDegree(node, direction, relTypeId, state.cursors.nodeCursor)
    }
  }

  override def compute(value: AnyValue, m: ExecutionContext, state: QueryState): AnyValue = value match {
    case n: NodeValue => Values.longValue(getDegree(state, n.id()))
    case other   => throw new CypherTypeException(s"Type mismatch: expected a node but was $other of type ${other.getClass.getSimpleName}")
  }

  override def arguments: Seq[Expression] = Seq(node)

  override def children: Seq[AstNode[_]] = Seq(node) ++ typ

  override def rewrite(f: Expression => Expression): Expression = f(GetDegree(node.rewrite(f), typ, direction))
}
