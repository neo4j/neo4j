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
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyType
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.operations.CypherFunctions.asIntExact
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.booleanValue

abstract class CheckDegreePrimitive(offset: Int, direction: SemanticDirection, maxDegree: Expression)
    extends Expression
    with SlottedExpression {

  protected def computePredicate(state: QueryState, node: Long, direction: SemanticDirection, max: Int): Boolean

  override def apply(row: ReadableRow, state: QueryState): AnyValue = {
    maxDegree.apply(row, state) match {
      case x if x eq Values.NO_VALUE => Values.NO_VALUE
      case e => booleanValue(computePredicate(state, row.getLongAt(offset), direction, asIntExact(e)))
    }
  }

  override def children: Seq[AstNode[_]] = Seq.empty
}

abstract class CheckDegreeWithTypePrimitive(
  offset: Int,
  typeId: Int,
  direction: SemanticDirection,
  maxDegree: Expression
) extends Expression
    with SlottedExpression {

  protected def computePredicate(
    state: QueryState,
    node: Long,
    relType: Int,
    direction: SemanticDirection,
    max: Int
  ): Boolean

  override def apply(row: ReadableRow, state: QueryState): AnyValue = {
    maxDegree.apply(row, state) match {
      case x if x eq Values.NO_VALUE => Values.NO_VALUE
      case e =>
        booleanValue(computePredicate(state, row.getLongAt(offset), typeId, direction, asIntExact(e)))
    }
  }

  override def children: Seq[AstNode[_]] = Seq.empty
}

abstract class CheckDegreeWithTypePrimitiveLate(
  offset: Int,
  typeName: LazyType,
  direction: SemanticDirection,
  maxDegree: Expression
) extends Expression
    with SlottedExpression {

  protected def computePredicate(
    state: QueryState,
    node: Long,
    relType: Int,
    direction: SemanticDirection,
    max: Int
  ): Boolean

  override def apply(row: ReadableRow, state: QueryState): AnyValue = {
    maxDegree.apply(row, state) match {
      case x if x eq Values.NO_VALUE => Values.NO_VALUE
      case e => booleanValue(computePredicate(
          state,
          row.getLongAt(offset),
          typeName.getId(state.query),
          direction,
          asIntExact(e)
        ))
    }
  }
  override def children: Seq[AstNode[_]] = Seq.empty
}

trait GreaterThan {

  protected def computePredicate(state: QueryState, node: Long, direction: SemanticDirection, max: Int): Boolean =
    state.query.nodeGetDegreeWithMax(max + 1, node, direction, state.cursors.nodeCursor) > max

  protected def computePredicate(
    state: QueryState,
    node: Long,
    relType: Int,
    direction: SemanticDirection,
    max: Int
  ): Boolean =
    state.query.nodeGetDegreeWithMax(max + 1, node, direction, relType, state.cursors.nodeCursor) > max
}

trait GreaterThanOrEqual {

  protected def computePredicate(state: QueryState, node: Long, direction: SemanticDirection, max: Int): Boolean =
    state.query.nodeGetDegreeWithMax(max, node, direction, state.cursors.nodeCursor) >= max

  protected def computePredicate(
    state: QueryState,
    node: Long,
    relType: Int,
    direction: SemanticDirection,
    max: Int
  ): Boolean =
    state.query.nodeGetDegreeWithMax(max, node, direction, relType, state.cursors.nodeCursor) >= max
}

trait Equal {

  protected def computePredicate(state: QueryState, node: Long, direction: SemanticDirection, max: Int): Boolean =
    state.query.nodeGetDegreeWithMax(max + 1, node, direction, state.cursors.nodeCursor) == max

  protected def computePredicate(
    state: QueryState,
    node: Long,
    relType: Int,
    direction: SemanticDirection,
    max: Int
  ): Boolean =
    state.query.nodeGetDegreeWithMax(max + 1, node, direction, relType, state.cursors.nodeCursor) == max
}

trait LessThanOrEqual {

  protected def computePredicate(state: QueryState, node: Long, direction: SemanticDirection, max: Int): Boolean =
    state.query.nodeGetDegreeWithMax(max + 1, node, direction, state.cursors.nodeCursor) <= max

  protected def computePredicate(
    state: QueryState,
    node: Long,
    relType: Int,
    direction: SemanticDirection,
    max: Int
  ): Boolean =
    state.query.nodeGetDegreeWithMax(max + 1, node, direction, relType, state.cursors.nodeCursor) <= max
}

trait LessThan {

  protected def computePredicate(state: QueryState, node: Long, direction: SemanticDirection, max: Int): Boolean =
    state.query.nodeGetDegreeWithMax(max, node, direction, state.cursors.nodeCursor) < max

  protected def computePredicate(
    state: QueryState,
    node: Long,
    relType: Int,
    direction: SemanticDirection,
    max: Int
  ): Boolean =
    state.query.nodeGetDegreeWithMax(max, node, direction, relType, state.cursors.nodeCursor) < max
}

case class HasDegreeGreaterThanPrimitive(offset: Int, direction: SemanticDirection, maxDegree: Expression)
    extends CheckDegreePrimitive(offset, direction, maxDegree) with GreaterThan

case class HasDegreeGreaterThanWithTypePrimitive(
  offset: Int,
  typeId: Int,
  direction: SemanticDirection,
  maxDegree: Expression
) extends CheckDegreeWithTypePrimitive(offset, typeId, direction, maxDegree) with GreaterThan

case class HasDegreeGreaterThanWithTypePrimitiveLate(
  offset: Int,
  typeName: LazyType,
  direction: SemanticDirection,
  maxDegree: Expression
) extends CheckDegreeWithTypePrimitiveLate(offset, typeName, direction, maxDegree) with GreaterThan

case class HasDegreeGreaterThanOrEqualPrimitive(offset: Int, direction: SemanticDirection, maxDegree: Expression)
    extends CheckDegreePrimitive(offset, direction, maxDegree) with GreaterThanOrEqual

case class HasDegreeGreaterThanOrEqualWithTypePrimitive(
  offset: Int,
  typeId: Int,
  direction: SemanticDirection,
  maxDegree: Expression
) extends CheckDegreeWithTypePrimitive(offset, typeId, direction, maxDegree) with GreaterThanOrEqual

case class HasDegreeGreaterThanOrEqualWithTypePrimitiveLate(
  offset: Int,
  typeName: LazyType,
  direction: SemanticDirection,
  maxDegree: Expression
) extends CheckDegreeWithTypePrimitiveLate(offset, typeName, direction, maxDegree) with GreaterThanOrEqual

case class HasDegreePrimitive(offset: Int, direction: SemanticDirection, maxDegree: Expression)
    extends CheckDegreePrimitive(offset, direction, maxDegree) with Equal

case class HasDegreeWithTypePrimitive(offset: Int, typeId: Int, direction: SemanticDirection, maxDegree: Expression)
    extends CheckDegreeWithTypePrimitive(offset, typeId, direction, maxDegree) with Equal

case class HasDegreeWithTypePrimitiveLate(
  offset: Int,
  typeName: LazyType,
  direction: SemanticDirection,
  maxDegree: Expression
) extends CheckDegreeWithTypePrimitiveLate(offset, typeName, direction, maxDegree) with Equal

case class HasDegreeLessThanOrEqualPrimitive(offset: Int, direction: SemanticDirection, maxDegree: Expression)
    extends CheckDegreePrimitive(offset, direction, maxDegree) with LessThanOrEqual

case class HasDegreeLessThanOrEqualWithTypePrimitive(
  offset: Int,
  typeId: Int,
  direction: SemanticDirection,
  maxDegree: Expression
) extends CheckDegreeWithTypePrimitive(offset, typeId, direction, maxDegree) with LessThanOrEqual

case class HasDegreeLessThanOrEqualWithTypePrimitiveLate(
  offset: Int,
  typeName: LazyType,
  direction: SemanticDirection,
  maxDegree: Expression
) extends CheckDegreeWithTypePrimitiveLate(offset, typeName, direction, maxDegree) with LessThanOrEqual

case class HasDegreeLessThanPrimitive(offset: Int, direction: SemanticDirection, maxDegree: Expression)
    extends CheckDegreePrimitive(offset, direction, maxDegree) with LessThan

case class HasDegreeLessThanWithTypePrimitive(
  offset: Int,
  typeId: Int,
  direction: SemanticDirection,
  maxDegree: Expression
) extends CheckDegreeWithTypePrimitive(offset, typeId, direction, maxDegree) with LessThan

case class HasDegreeLessThanWithTypePrimitiveLate(
  offset: Int,
  typeName: LazyType,
  direction: SemanticDirection,
  maxDegree: Expression
) extends CheckDegreeWithTypePrimitiveLate(offset, typeName, direction, maxDegree) with LessThan
