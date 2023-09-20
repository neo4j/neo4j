/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.util

import scala.language.implicitConversions

package object symbols {
  val CTAny: AnyType = AnyType(isNullable = true)(InputPosition.NONE)
  val CTBoolean: BooleanType = BooleanType(isNullable = true)(InputPosition.NONE)
  val CTString: StringType = StringType(isNullable = true)(InputPosition.NONE)
  val CTNumber: NumberType = NumberType(isNullable = true)(InputPosition.NONE)
  val CTFloat: FloatType = FloatType(isNullable = true)(InputPosition.NONE)
  val CTInteger: IntegerType = IntegerType(isNullable = true)(InputPosition.NONE)
  val CTMap: MapType = MapType(isNullable = true)(InputPosition.NONE)
  val CTNode: NodeType = NodeType(isNullable = true)(InputPosition.NONE)
  val CTRelationship: RelationshipType = RelationshipType(isNullable = true)(InputPosition.NONE)
  val CTPoint: PointType = PointType(isNullable = true)(InputPosition.NONE)
  val CTDateTime: ZonedDateTimeType = ZonedDateTimeType(isNullable = true)(InputPosition.NONE)
  val CTLocalDateTime: LocalDateTimeType = LocalDateTimeType(isNullable = true)(InputPosition.NONE)
  val CTDate: DateType = DateType(isNullable = true)(InputPosition.NONE)
  val CTTime: ZonedTimeType = ZonedTimeType(isNullable = true)(InputPosition.NONE)
  val CTLocalTime: LocalTimeType = LocalTimeType(isNullable = true)(InputPosition.NONE)
  val CTDuration: DurationType = DurationType(isNullable = true)(InputPosition.NONE)
  val CTGeometry: GeometryType = GeometryType(isNullable = true)(InputPosition.NONE)
  val CTPath: PathType = PathType(isNullable = true)(InputPosition.NONE)
  val CTGraphRef: GraphRefType = GraphRefType(isNullable = true)(InputPosition.NONE)
  def CTList(inner: CypherType): ListType = ListType(inner, isNullable = true)(InputPosition.NONE)

  implicit def invariantTypeSpec(that: CypherType): TypeSpec = that.invariant
}
