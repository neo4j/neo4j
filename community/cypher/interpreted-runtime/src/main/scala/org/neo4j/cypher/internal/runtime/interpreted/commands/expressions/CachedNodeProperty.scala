/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.cypher.internal.planner.v3_5.spi.TokenContext
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.KeyToken
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.v3_5.logical.plans
import org.neo4j.kernel.api.StatementConstants
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.cypher.internal.v3_5.util.CypherTypeException

abstract class AbstractCachedNodeProperty extends Expression {

  // abstract stuff

  def getNodeId(ctx: ExecutionContext): Long
  def getPropertyKey(tokenContext: TokenContext): Int
  def getCachedProperty(ctx: ExecutionContext): AnyValue

  // encapsulated cached-node-property logic

  def apply(ctx: ExecutionContext, state: QueryState): AnyValue = {
    val nodeId = getNodeId(ctx)
    if (nodeId == StatementConstants.NO_SUCH_NODE)
      Values.NO_VALUE
    else {
      getPropertyKey(state.query) match {
        case StatementConstants.NO_SUCH_PROPERTY_KEY => Values.NO_VALUE
        case propId =>
          val maybeTxStateValue = state.query.nodeOps.getTxStateProperty(nodeId, propId)
          maybeTxStateValue match {
            case Some(txStateValue) => txStateValue
            case None =>
              val cached = getCachedProperty(ctx)
              if (cached == null) // if the cached node property has been invalidated
                state.query.nodeProperty(nodeId, propId)
              else
                cached
          }
      }
    }
  }

  override def rewrite(f: Expression => Expression) = f(this)

  override def arguments: Seq[Expression] = Seq()
}

case class CachedNodeProperty(nodeName: String, propertyKey: KeyToken, key: plans.CachedNodeProperty)
  extends AbstractCachedNodeProperty
{
  def symbolTableDependencies = Set(nodeName, key.cacheKey)

  override def toString: String = key.cacheKey

  override def getNodeId(ctx: ExecutionContext): Long =
    ctx(nodeName) match {
      case Values.NO_VALUE => StatementConstants.NO_SUCH_NODE
      case n: VirtualNodeValue => n.id()
      case other => throw new CypherTypeException(s"Type mismatch: expected a node but was $other")
    }

  override def getCachedProperty(ctx: ExecutionContext): AnyValue = ctx.getCachedProperty(key)

  override def getPropertyKey(tokenContext: TokenContext): Int = propertyKey.getOptId(tokenContext).getOrElse(StatementConstants.NO_SUCH_PROPERTY_KEY)
}
