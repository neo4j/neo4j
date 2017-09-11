/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands.expressions

import org.neo4j.cypher.internal.apa.v3_4.CypherTypeException
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands.values.KeyToken
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.helpers.IsMap
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.pipes.QueryState
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.{EdgeValue, NodeValue}

case class Property(mapExpr: Expression, propertyKey: KeyToken)
  extends Expression with Product with Serializable
{
  def apply(ctx: ExecutionContext, state: QueryState): AnyValue = mapExpr(ctx, state) match {
    case n if n == Values.NO_VALUE => Values.NO_VALUE
    case n: NodeValue =>
      propertyKey.getOptId(state.query) match {
        case None => Values.NO_VALUE
        case Some(propId) => state.query.nodeOps.getProperty(n.id(), propId)
      }
    case r: EdgeValue =>
      propertyKey.getOptId(state.query) match {
        case None => Values.NO_VALUE
        case Some(propId) => state.query.relationshipOps.getProperty(r.id(), propId)
      }
    case IsMap(mapFunc) => mapFunc(state.query).get(propertyKey.name)
    case other => throw new CypherTypeException(s"Type mismatch: expected a map but was $other")
  }

  def rewrite(f: (Expression) => Expression) = f(Property(mapExpr.rewrite(f), propertyKey.rewrite(f)))

  override def children = Seq(mapExpr, propertyKey)

  def arguments = Seq(mapExpr)

  def symbolTableDependencies = mapExpr.symbolTableDependencies

  override def toString = s"$mapExpr.${propertyKey.name}"
}
