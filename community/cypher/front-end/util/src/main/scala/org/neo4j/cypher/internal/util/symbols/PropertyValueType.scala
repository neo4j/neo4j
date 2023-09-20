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
package org.neo4j.cypher.internal.util.symbols

import org.neo4j.cypher.internal.util.InputPosition

case class PropertyValueType(isNullable: Boolean)(val position: InputPosition) extends CypherType {
  val parentType: CypherType = CTAny
  override val toString = "Property Value"
  override val toCypherTypeString = "PROPERTY VALUE"

  // This is technically a special case of a closed dynamic union
  override def sortOrder: Int = CypherTypeOrder.CLOSED_DYNAMIC_UNION.id

  // Property value expands to a closed dynamic union of all property types
  def expandToTypes: List[CypherType] = List(
    BooleanType(isNullable)(position),
    StringType(isNullable)(position),
    IntegerType(isNullable)(position),
    FloatType(isNullable)(position),
    DateType(isNullable)(position),
    LocalTimeType(isNullable)(position),
    ZonedTimeType(isNullable)(position),
    LocalDateTimeType(isNullable)(position),
    ZonedDateTimeType(isNullable)(position),
    DurationType(isNullable)(position),
    PointType(isNullable)(position),
    ListType(BooleanType(isNullable = false)(position), isNullable = isNullable)(position),
    ListType(StringType(isNullable = false)(position), isNullable = isNullable)(position),
    ListType(DateType(isNullable = false)(position), isNullable = isNullable)(position),
    ListType(LocalTimeType(isNullable = false)(position), isNullable = isNullable)(position),
    ListType(ZonedTimeType(isNullable = false)(position), isNullable = isNullable)(position),
    ListType(LocalDateTimeType(isNullable = false)(position), isNullable = isNullable)(position),
    ListType(ZonedDateTimeType(isNullable = false)(position), isNullable = isNullable)(position),
    ListType(DurationType(isNullable = false)(position), isNullable = isNullable)(position),
    ListType(PointType(isNullable = false)(position), isNullable = isNullable)(position),
    // LIST<INTEGER | FLOAT> will be stored as a LIST<FLOAT>, added here so that a check if that list
    // is storable will pass.
    ListType(
      ClosedDynamicUnionType(Set(
        IntegerType(isNullable = false)(position),
        FloatType(isNullable = false)(position)
      ))(position),
      isNullable = isNullable
    )(position)
  )

  override def updateIsNullable(isNullable: Boolean): CypherType = this.copy(isNullable = isNullable)(position)

  def withPosition(newPosition: InputPosition): CypherType = this.copy()(position = newPosition)
}
