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

import org.neo4j.cypher.internal.expressions.ASTCachedProperty
import org.neo4j.cypher.internal.planner.spi.ReadTokenContext
import org.neo4j.cypher.internal.runtime.IsNoValue
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.KeyToken
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.kernel.api.StatementConstants
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualRelationshipValue

abstract class AbstractCachedProperty extends Expression {

  // abstract stuff

  def getId(ctx: ReadableRow): Long
  def getPropertyKey(tokenContext: ReadTokenContext): Int
  def getCachedProperty(ctx: ReadableRow): Value
  def setCachedProperty(ctx: ReadableRow, value: Value): Unit

  def getTxStateProperty(state: QueryState, id: Long, propId: Int): Value
  def property(state: QueryState, id: Long, propId: Int): Value

  // encapsulated cached-property logic

  def apply(row: ReadableRow, state: QueryState): AnyValue = {
    val id = getId(row)
    if (id == StatementConstants.NO_SUCH_ENTITY) {
      Values.NO_VALUE
    } else {
      getPropertyKey(state.query) match {
        case StatementConstants.NO_SUCH_PROPERTY_KEY => Values.NO_VALUE
        case propId =>
          val maybeTxStateValue = getTxStateProperty(state, id, propId)
          maybeTxStateValue match {
            case null =>
              val cached = getCachedProperty(row)
              if (cached == null) {
                // if the cached node property has been invalidated
                val value = property(state, id, propId)
                // Re-cache the value
                setCachedProperty(row, value)
                value
              } else {
                cached
              }
            case txStateValue => txStateValue
          }
      }
    }
  }

  override def rewrite(f: Expression => Expression): Expression = f(this)

  override def arguments: Seq[Expression] = Seq()
}

abstract class AbstractCachedNodeProperty extends AbstractCachedProperty {

  override def getTxStateProperty(state: QueryState, id: Long, propId: Int): Value =
    state.query.nodeReadOps.getTxStateProperty(id, propId)

  override def property(state: QueryState, id: Long, propId: Int): Value =
    state.query.nodeProperty(id, propId, state.cursors.nodeCursor, state.cursors.propertyCursor, throwOnDeleted = true)
}

abstract class AbstractCachedNodeHasProperty extends AbstractCachedProperty {

  override def getTxStateProperty(state: QueryState, id: Long, propId: Int): Value =
    state.query.nodeReadOps.getTxStateProperty(id, propId)

  override def property(state: QueryState, id: Long, propId: Int): Value = {
    if (state.query.nodeHasProperty(id, propId, state.cursors.nodeCursor, state.cursors.propertyCursor)) {
      Values.TRUE
    } else {
      Values.NO_VALUE
    }
  }
}

abstract class AbstractCachedRelationshipProperty extends AbstractCachedProperty {

  override def getTxStateProperty(state: QueryState, id: Long, propId: Int): Value =
    state.query.relationshipReadOps.getTxStateProperty(id, propId)

  override def property(state: QueryState, id: Long, propId: Int): Value = state.query.relationshipProperty(
    id,
    propId,
    state.cursors.relationshipScanCursor,
    state.cursors.propertyCursor,
    throwOnDeleted = true
  )
}

abstract class AbstractCachedRelationshipHasProperty extends AbstractCachedProperty {

  override def getTxStateProperty(state: QueryState, id: Long, propId: Int): Value =
    state.query.relationshipReadOps.getTxStateProperty(id, propId)

  override def property(state: QueryState, id: Long, propId: Int): Value = {
    val hasProperty = state.query.relationshipHasProperty(
      id,
      propId,
      state.cursors.relationshipScanCursor,
      state.cursors.propertyCursor
    )
    if (hasProperty) Values.TRUE else Values.NO_VALUE
  }
}

case class CachedNodeProperty(nodeName: String, propertyKey: KeyToken, key: ASTCachedProperty.RuntimeKey)
    extends AbstractCachedNodeProperty {
  override def toString: String = key.propertyAccessString

  override def getId(ctx: ReadableRow): Long =
    ctx.getByName(nodeName) match {
      case IsNoValue()         => StatementConstants.NO_SUCH_NODE
      case n: VirtualNodeValue => n.id()
      case other               => throw new CypherTypeException(s"Type mismatch: expected a node but was $other")
    }

  override def getCachedProperty(ctx: ReadableRow): Value = ctx.getCachedProperty(key)

  override def setCachedProperty(ctx: ReadableRow, value: Value): Unit = ctx.setCachedProperty(key, value)

  override def getPropertyKey(tokenContext: ReadTokenContext): Int =
    propertyKey.getOptId(tokenContext).getOrElse(StatementConstants.NO_SUCH_PROPERTY_KEY)

  override def children: Seq[AstNode[_]] = Seq(propertyKey)
}

case class CachedNodeHasProperty(nodeName: String, propertyKey: KeyToken, key: ASTCachedProperty.RuntimeKey)
    extends AbstractCachedNodeHasProperty {
  override def toString: String = key.propertyAccessString

  override def getId(ctx: ReadableRow): Long =
    ctx.getByName(nodeName) match {
      case IsNoValue()         => StatementConstants.NO_SUCH_NODE
      case n: VirtualNodeValue => n.id()
      case other               => throw new CypherTypeException(s"Type mismatch: expected a node but was $other")
    }

  override def getCachedProperty(ctx: ReadableRow): Value = ctx.getCachedProperty(key)

  override def setCachedProperty(ctx: ReadableRow, value: Value): Unit = ctx.setCachedProperty(key, value)

  override def getPropertyKey(tokenContext: ReadTokenContext): Int =
    propertyKey.getOptId(tokenContext).getOrElse(StatementConstants.NO_SUCH_PROPERTY_KEY)

  override def children: Seq[AstNode[_]] = Seq(propertyKey)
}

case class CachedRelationshipProperty(nodeName: String, propertyKey: KeyToken, key: ASTCachedProperty.RuntimeKey)
    extends AbstractCachedRelationshipProperty {
  override def toString: String = key.propertyAccessString

  override def getId(ctx: ReadableRow): Long =
    ctx.getByName(nodeName) match {
      case IsNoValue()                 => StatementConstants.NO_SUCH_RELATIONSHIP
      case r: VirtualRelationshipValue => r.id()
      case other => throw new CypherTypeException(s"Type mismatch: expected a relationship but was $other")
    }

  override def getCachedProperty(ctx: ReadableRow): Value = ctx.getCachedProperty(key)

  override def setCachedProperty(ctx: ReadableRow, value: Value): Unit = ctx.setCachedProperty(key, value)

  override def getPropertyKey(tokenContext: ReadTokenContext): Int =
    propertyKey.getOptId(tokenContext).getOrElse(StatementConstants.NO_SUCH_PROPERTY_KEY)

  override def children: Seq[AstNode[_]] = Seq(propertyKey)
}

case class CachedRelationshipHasProperty(nodeName: String, propertyKey: KeyToken, key: ASTCachedProperty.RuntimeKey)
    extends AbstractCachedRelationshipHasProperty {
  override def toString: String = key.propertyAccessString

  override def getId(ctx: ReadableRow): Long =
    ctx.getByName(nodeName) match {
      case IsNoValue()                 => StatementConstants.NO_SUCH_RELATIONSHIP
      case r: VirtualRelationshipValue => r.id()
      case other => throw new CypherTypeException(s"Type mismatch: expected a relationship but was $other")
    }

  override def getCachedProperty(ctx: ReadableRow): Value = ctx.getCachedProperty(key)

  override def setCachedProperty(ctx: ReadableRow, value: Value): Unit = ctx.setCachedProperty(key, value)

  override def getPropertyKey(tokenContext: ReadTokenContext): Int =
    propertyKey.getOptId(tokenContext).getOrElse(StatementConstants.NO_SUCH_PROPERTY_KEY)

  override def children: Seq[AstNode[_]] = Seq(propertyKey)
}
