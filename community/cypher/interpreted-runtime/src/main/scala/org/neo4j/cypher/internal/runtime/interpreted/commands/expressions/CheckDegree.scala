/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.CheckDegree.nodeIdOrDefaultId
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.KeyToken
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.operations.CypherFunctions.asIntExact
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.storable.Values.booleanValue
import org.neo4j.values.virtual.VirtualNodeValue

abstract class CheckDegree(node: Expression, typ: Option[KeyToken], direction: SemanticDirection, maxDegree: Expression)
    extends Expression {

  protected val getDegree: (Int, QueryState, Long) => Long = typ match {
    case None => (max, state, node) => state.query.nodeGetDegreeWithMax(max, node, direction, state.cursors.nodeCursor)
    case Some(t) => (max, state, node) =>
        t.getOptId(state.query) match {
          case None => 0
          case Some(relTypeId) =>
            state.query.nodeGetDegreeWithMax(max, node, direction, relTypeId, state.cursors.nodeCursor)
        }
  }

  protected def computePredicate(state: QueryState, node: Long, max: Int): Boolean

  override def apply(row: ReadableRow, state: QueryState): AnyValue = {
    maxDegree.apply(row, state) match {
      case x if x eq NO_VALUE => NO_VALUE
      case e =>
        val nodeValue = node(row, state)
        val nodeId = nodeIdOrDefaultId(nodeValue)
        booleanValue(computePredicate(state, nodeId, asIntExact(e)))
    }
  }

  override def arguments: Seq[Expression] = Seq(node)
  override def children: Seq[AstNode[_]] = Seq(node) ++ typ
}

object CheckDegree {

  private def nodeIdOrDefaultId(node: AnyValue): Long = node match {
    case node: VirtualNodeValue          => node.id()
    case node if node eq Values.NO_VALUE => -1L
    case other => throw new CypherTypeException(
        s"Type mismatch: expected a node but was $other of type ${other.getClass.getSimpleName}"
      )
  }
}

case class HasDegreeGreaterThan(
  node: Expression,
  typ: Option[KeyToken],
  direction: SemanticDirection,
  maxDegree: Expression
) extends CheckDegree(node, typ, direction, maxDegree) {

  override protected def computePredicate(state: QueryState, node: Long, max: Int): Boolean =
    getDegree(max + 1, state, node) > max

  override def rewrite(f: Expression => Expression): Expression =
    f(HasDegreeGreaterThan(node.rewrite(f), typ, direction, maxDegree))

}

case class HasDegreeGreaterThanOrEqual(
  node: Expression,
  typ: Option[KeyToken],
  direction: SemanticDirection,
  maxDegree: Expression
) extends CheckDegree(node, typ, direction, maxDegree) {

  override protected def computePredicate(state: QueryState, node: Long, max: Int): Boolean =
    getDegree(max, state, node) >= max

  override def rewrite(f: Expression => Expression): Expression =
    f(HasDegreeGreaterThanOrEqual(node.rewrite(f), typ, direction, maxDegree))

}

case class HasDegree(node: Expression, typ: Option[KeyToken], direction: SemanticDirection, maxDegree: Expression)
    extends CheckDegree(node, typ, direction, maxDegree) {

  override protected def computePredicate(state: QueryState, node: Long, max: Int): Boolean =
    getDegree(max + 1, state, node) == max

  override def rewrite(f: Expression => Expression): Expression =
    f(HasDegree(node.rewrite(f), typ, direction, maxDegree))

}

case class HasDegreeLessThan(
  node: Expression,
  typ: Option[KeyToken],
  direction: SemanticDirection,
  maxDegree: Expression
) extends CheckDegree(node, typ, direction, maxDegree) {

  override protected def computePredicate(state: QueryState, node: Long, max: Int): Boolean =
    getDegree(max, state, node) < max

  override def rewrite(f: Expression => Expression): Expression =
    f(HasDegreeLessThan(node.rewrite(f), typ, direction, maxDegree))

}

case class HasDegreeLessThanOrEqual(
  node: Expression,
  typ: Option[KeyToken],
  direction: SemanticDirection,
  maxDegree: Expression
) extends CheckDegree(node, typ, direction, maxDegree) {

  override protected def computePredicate(state: QueryState, node: Long, max: Int): Boolean =
    getDegree(max + 1, state, node) <= max

  override def rewrite(f: Expression => Expression): Expression =
    f(HasDegreeLessThanOrEqual(node.rewrite(f), typ, direction, maxDegree))

}
