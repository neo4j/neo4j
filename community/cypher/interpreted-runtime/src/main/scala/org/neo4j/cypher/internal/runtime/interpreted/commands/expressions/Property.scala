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

import org.opencypher.v9_0.util.{CypherTypeException, InvalidArgumentException}
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.KeyToken
import org.neo4j.cypher.internal.runtime.interpreted.IsMap
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.{DurationValue, PointValue, TemporalValue, Values}
import org.neo4j.values.virtual.{VirtualNodeValue, VirtualRelationshipValue}

import scala.util.{Failure, Success, Try}

case class Property(mapExpr: Expression, propertyKey: KeyToken)
  extends Expression with Product with Serializable
{
  def apply(ctx: ExecutionContext, state: QueryState): AnyValue = mapExpr(ctx, state) match {
    case n if n == Values.NO_VALUE => Values.NO_VALUE
    case n: VirtualNodeValue =>
      propertyKey.getOptId(state.query) match {
        case None => Values.NO_VALUE
        case Some(propId) => state.query.nodeOps.getProperty(n.id(), propId)
      }
    case r: VirtualRelationshipValue =>
      propertyKey.getOptId(state.query) match {
        case None => Values.NO_VALUE
        case Some(propId) => state.query.relationshipOps.getProperty(r.id(), propId)
      }
    case IsMap(mapFunc) => mapFunc(state.query).get(propertyKey.name)
    case t: TemporalValue[_,_] => t.get(propertyKey.name)
    case d: DurationValue => d.get(propertyKey.name)
    case p: PointValue => Try(p.get(propertyKey.name)) match {
      case Success(v) => v
      case Failure(e) => throw new InvalidArgumentException(e.getMessage, e)
    }
    case other => throw new CypherTypeException(s"Type mismatch: expected a map but was $other")
  }

  def rewrite(f: (Expression) => Expression) = f(Property(mapExpr.rewrite(f), propertyKey.rewrite(f)))

  override def children = Seq(mapExpr, propertyKey)

  def arguments = Seq(mapExpr)

  def symbolTableDependencies = mapExpr.symbolTableDependencies

  override def toString = s"$mapExpr.${propertyKey.name}"
}
