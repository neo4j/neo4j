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

import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.CoerceTo.toNeo4jType
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.symbols.AnyType
import org.neo4j.cypher.internal.util.symbols.BooleanType
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.DateType
import org.neo4j.cypher.internal.util.symbols.DurationType
import org.neo4j.cypher.internal.util.symbols.FloatType
import org.neo4j.cypher.internal.util.symbols.GeometryType
import org.neo4j.cypher.internal.util.symbols.IntegerType
import org.neo4j.cypher.internal.util.symbols.ListType
import org.neo4j.cypher.internal.util.symbols.LocalDateTimeType
import org.neo4j.cypher.internal.util.symbols.LocalTimeType
import org.neo4j.cypher.internal.util.symbols.MapType
import org.neo4j.cypher.internal.util.symbols.NodeType
import org.neo4j.cypher.internal.util.symbols.NumberType
import org.neo4j.cypher.internal.util.symbols.PathType
import org.neo4j.cypher.internal.util.symbols.PointType
import org.neo4j.cypher.internal.util.symbols.RelationshipType
import org.neo4j.cypher.internal.util.symbols.StringType
import org.neo4j.cypher.internal.util.symbols.ZonedDateTimeType
import org.neo4j.cypher.internal.util.symbols.ZonedTimeType
import org.neo4j.cypher.operations.CypherCoercions
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.internal.kernel.api.procs.Neo4jTypes
import org.neo4j.values.AnyValue

case class CoerceTo(expr: Expression, typ: CypherType) extends Expression {
  private val coercer = CypherCoercions.coercerFromType(toNeo4jType(typ))

  override def apply(row: ReadableRow, state: QueryState): AnyValue = {
    coercer.apply(expr(row, state), state.query, state.cursors)
  }

  override def rewrite(f: Expression => Expression): Expression = f(CoerceTo(expr.rewrite(f), typ))

  override def arguments: Seq[Expression] = Seq(expr)

  override def children: Seq[AstNode[_]] = Seq(expr)
}

object CoerceTo {

  def toNeo4jType(typ: CypherType): Neo4jTypes.AnyType = typ match {
    case _: AnyType             => Neo4jTypes.NTAny
    case _: BooleanType         => Neo4jTypes.NTBoolean
    case _: DateType            => Neo4jTypes.NTDate
    case _: DurationType        => Neo4jTypes.NTDuration
    case _: FloatType           => Neo4jTypes.NTFloat
    case _: GeometryType        => Neo4jTypes.NTGeometry
    case _: IntegerType         => Neo4jTypes.NTInteger
    case ListType(innerType, _) => new Neo4jTypes.ListType(toNeo4jType(innerType))
    case _: LocalDateTimeType   => Neo4jTypes.NTLocalDateTime
    case _: LocalTimeType       => Neo4jTypes.NTLocalTime
    case _: MapType             => Neo4jTypes.NTMap
    case _: NodeType            => Neo4jTypes.NTNode
    case _: NumberType          => Neo4jTypes.NTNumber
    case _: PathType            => Neo4jTypes.NTPath
    case _: PointType           => Neo4jTypes.NTPoint
    case _: RelationshipType    => Neo4jTypes.NTRelationship
    case _: StringType          => Neo4jTypes.NTString
    case _: ZonedDateTimeType   => Neo4jTypes.NTDateTime
    case _: ZonedTimeType       => Neo4jTypes.NTTime
    case _ => throw new CypherTypeException(s"Wrong argument type: Can't coerce to $typ (${typ.getClass})")
  }
}
