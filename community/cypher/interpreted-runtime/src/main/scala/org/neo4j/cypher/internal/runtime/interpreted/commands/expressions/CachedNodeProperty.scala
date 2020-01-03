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

import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.runtime.{ExecutionContext, IsNoValue}
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.KeyToken
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.v4_0.expressions.ASTCachedProperty
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.kernel.api.StatementConstants
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.{Value, Values}
import org.neo4j.values.virtual.{VirtualNodeValue, VirtualRelationshipValue}

abstract class AbstractCachedProperty extends Expression {

  // abstract stuff

  def getId(ctx: ExecutionContext): Long
  def getPropertyKey(tokenContext: TokenContext): Int
  def getCachedProperty(ctx: ExecutionContext): Value
  def setCachedProperty(ctx: ExecutionContext, value: Value): Unit

  def getTxStateProperty(state: QueryState, id: Long, propId: Int): Value
  def property(state: QueryState, id: Long, propId: Int): Value

  // encapsulated cached-property logic

  def apply(ctx: ExecutionContext, state: QueryState): AnyValue = {
    val id = getId(ctx)
    if (id == StatementConstants.NO_SUCH_ENTITY)
      Values.NO_VALUE
    else {
      getPropertyKey(state.query) match {
        case StatementConstants.NO_SUCH_PROPERTY_KEY => Values.NO_VALUE
        case propId =>
          val maybeTxStateValue = getTxStateProperty(state, id, propId)
          maybeTxStateValue match {
            case null =>
              val cached = getCachedProperty(ctx)
              if (cached == null) {
                // if the cached node property has been invalidated
                val value = property(state, id, propId)
                // Re-cache the value
                setCachedProperty(ctx, value)
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
  override def getTxStateProperty(state: QueryState, id: Long, propId: Int): Value = state.query.nodeOps.getTxStateProperty(id, propId)

  override def property(state: QueryState,
                        id: Long,
                        propId: Int): Value = state.query.nodeProperty(id, propId, state.cursors.nodeCursor, state.cursors.propertyCursor, throwOnDeleted = true)
}

abstract class AbstractCachedRelationshipProperty extends AbstractCachedProperty {
  override def getTxStateProperty(state: QueryState, id: Long, propId: Int): Value = state.query.relationshipOps.getTxStateProperty(id, propId)

  override def property(state: QueryState,
                        id: Long,
                        propId: Int): Value = state.query.relationshipProperty(id, propId, state.cursors.relationshipScanCursor, state.cursors.propertyCursor, throwOnDeleted = true)
}

case class CachedNodeProperty(nodeName: String, propertyKey: KeyToken, key: ASTCachedProperty)
  extends AbstractCachedNodeProperty
{
  override def toString: String = key.propertyAccessString

  override def getId(ctx: ExecutionContext): Long =
    ctx.getByName(nodeName) match {
      case IsNoValue() => StatementConstants.NO_SUCH_NODE
      case n: VirtualNodeValue => n.id()
      case other => throw new CypherTypeException(s"Type mismatch: expected a node but was $other")
    }

  override def getCachedProperty(ctx: ExecutionContext): Value = ctx.getCachedProperty(key)

  override def setCachedProperty(ctx: ExecutionContext, value: Value): Unit = ctx.setCachedProperty(key, value)

  override def getPropertyKey(tokenContext: TokenContext): Int = propertyKey.getOptId(tokenContext).getOrElse(StatementConstants.NO_SUCH_PROPERTY_KEY)

  override def children: Seq[AstNode[_]] = Seq(propertyKey)
}

case class CachedRelationshipProperty(nodeName: String, propertyKey: KeyToken, key: ASTCachedProperty)
  extends AbstractCachedRelationshipProperty
{
  override def toString: String = key.propertyAccessString

  override def getId(ctx: ExecutionContext): Long =
    ctx.getByName(nodeName) match {
      case IsNoValue() => StatementConstants.NO_SUCH_RELATIONSHIP
      case r: VirtualRelationshipValue => r.id()
      case other => throw new CypherTypeException(s"Type mismatch: expected a relationship but was $other")
    }

  override def getCachedProperty(ctx: ExecutionContext): Value = ctx.getCachedProperty(key)

  override def setCachedProperty(ctx: ExecutionContext, value: Value): Unit = ctx.setCachedProperty(key, value)

  override def getPropertyKey(tokenContext: TokenContext): Int = propertyKey.getOptId(tokenContext).getOrElse(StatementConstants.NO_SUCH_PROPERTY_KEY)

  override def children: Seq[AstNode[_]] = Seq(propertyKey)
}
