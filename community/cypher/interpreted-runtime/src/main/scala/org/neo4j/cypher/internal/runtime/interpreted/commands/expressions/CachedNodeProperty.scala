/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.KeyToken
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualNodeValue
import org.opencypher.v9_0.util.CypherTypeException

case class CachedNodeProperty(nodeName: String, propertyKey: KeyToken, cachedName: String)
  extends Expression with Product with Serializable
{
  def apply(ctx: ExecutionContext, state: QueryState): AnyValue =
    ctx(nodeName) match {
      case Values.NO_VALUE => Values.NO_VALUE
      case n: VirtualNodeValue =>
        propertyKey.getOptId(state.query) match {
          case None => Values.NO_VALUE
          case Some(propId) =>
            val maybeTxStateValue = state.query.nodeOps.getTxStateProperty(n.id(), propId)
            maybeTxStateValue match {
              case Some(txStateValue) => txStateValue
              case None => ctx(cachedName)
            }
        }
      case other => throw new CypherTypeException(s"Type mismatch: expected a node but was $other")
    }

  def rewrite(f: Expression => Expression) = f(this)

  def arguments = Seq()

  def symbolTableDependencies = Set(nodeName, cachedName)

  override def toString: String = cachedName
}
