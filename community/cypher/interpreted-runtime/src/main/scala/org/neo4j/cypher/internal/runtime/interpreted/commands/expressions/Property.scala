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

import org.neo4j.cypher.internal.runtime.IsNoValue
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.IsMap
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.KeyToken
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.DurationValue
import org.neo4j.values.storable.PointValue
import org.neo4j.values.storable.TemporalValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualRelationshipValue

import scala.util.Failure
import scala.util.Success
import scala.util.Try

case class Property(mapExpr: Expression, propertyKey: KeyToken)
    extends Expression with Product with Serializable {

  def apply(row: ReadableRow, state: QueryState): AnyValue = mapExpr(row, state) match {
    case IsNoValue() => Values.NO_VALUE
    case n: VirtualNodeValue =>
      propertyKey.getOptId(state.query) match {
        case None => Values.NO_VALUE
        case Some(propId) => state.query.nodeReadOps.getProperty(
            n.id(),
            propId,
            state.cursors.nodeCursor,
            state.cursors.propertyCursor,
            throwOnDeleted = true
          )
      }
    case r: VirtualRelationshipValue =>
      propertyKey.getOptId(state.query) match {
        case None => Values.NO_VALUE
        case Some(propId) =>
          state.query.relationshipReadOps.getProperty(
            r,
            propId,
            state.cursors.relationshipScanCursor,
            state.cursors.propertyCursor,
            throwOnDeleted = true
          )
      }
    case IsMap(mapFunc)         => mapFunc(state).get(propertyKey.name)
    case t: TemporalValue[_, _] => t.get(propertyKey.name)
    case d: DurationValue       => d.get(propertyKey.name)
    case p: PointValue => Try(p.get(propertyKey.name)) match {
        case Success(v) => v
        case Failure(e) => throw new InvalidArgumentException(e.getMessage, e)
      }
    case other => throw new CypherTypeException(s"Type mismatch: expected a map but was $other")
  }

  override def rewrite(f: Expression => Expression): Expression =
    f(Property(mapExpr.rewrite(f), propertyKey.rewrite(f)))

  override def children = Seq(mapExpr, propertyKey)

  override def arguments: Seq[Expression] = Seq(mapExpr)

  override def toString = s"$mapExpr.${propertyKey.name}"
}
