/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.expressions

import org.neo4j.values.storable.ValueRepresentation

sealed trait CypherTypeName {
  protected def typeName: String
  // e.g BOOLEAN set(true, false, null) is nullable, BOOLEAN NOT NULL set(true, false) is not
  def isNullable: Boolean

  def hasValueRepresentation: Boolean = false

  def possibleValueRepresentations: List[ValueRepresentation] =
    throw new UnsupportedOperationException("possibleValueRepresentations not supported on ${getClass.getName}")
  def description: String = if (isNullable) typeName else s"$typeName NOT NULL"
  override def toString: String = description
}

/* Types not yet covered:
 *  ANY<...>
 */

case class NothingTypeName() extends CypherTypeName {
  protected val typeName: String = "NOTHING"

  // The NOTHING type never includes `null` but should not have a `NOT NULL`
  override def isNullable: Boolean = false
  override def description: String = typeName
}

case class NullTypeName() extends CypherTypeName {
  protected val typeName: String = "NULL"
  override def isNullable: Boolean = true
}

case class BooleanTypeName(isNullable: Boolean) extends CypherTypeName {
  protected val typeName: String = "BOOLEAN"

  override def hasValueRepresentation: Boolean = true

  override def possibleValueRepresentations: List[ValueRepresentation] = List(ValueRepresentation.BOOLEAN)
}

case class StringTypeName(isNullable: Boolean) extends CypherTypeName {
  protected val typeName: String = "STRING"

  override def hasValueRepresentation: Boolean = true

  override def possibleValueRepresentations: List[ValueRepresentation] =
    List(ValueRepresentation.UTF8_TEXT, ValueRepresentation.UTF16_TEXT)
}

case class IntegerTypeName(isNullable: Boolean) extends CypherTypeName {
  protected val typeName: String = "INTEGER"

  override def hasValueRepresentation: Boolean = true

  override def possibleValueRepresentations: List[ValueRepresentation] = List(
    ValueRepresentation.INT8,
    ValueRepresentation.INT16,
    ValueRepresentation.INT32,
    ValueRepresentation.INT64
  )
}

case class FloatTypeName(isNullable: Boolean) extends CypherTypeName {
  protected val typeName: String = "FLOAT"

  override def hasValueRepresentation: Boolean = true

  override def possibleValueRepresentations: List[ValueRepresentation] =
    List(ValueRepresentation.FLOAT32, ValueRepresentation.FLOAT64)
}

case class DateTypeName(isNullable: Boolean) extends CypherTypeName {
  protected val typeName: String = "DATE"

  override def hasValueRepresentation: Boolean = true

  override def possibleValueRepresentations: List[ValueRepresentation] = List(ValueRepresentation.DATE)
}

case class LocalTimeTypeName(isNullable: Boolean) extends CypherTypeName {
  protected val typeName: String = "LOCAL TIME"

  override def hasValueRepresentation: Boolean = true

  override def possibleValueRepresentations: List[ValueRepresentation] = List(ValueRepresentation.LOCAL_TIME)
}

case class ZonedTimeTypeName(isNullable: Boolean) extends CypherTypeName {
  protected val typeName: String = "ZONED TIME"

  override def hasValueRepresentation: Boolean = true

  override def possibleValueRepresentations: List[ValueRepresentation] = List(ValueRepresentation.ZONED_TIME)
}

case class LocalDateTimeTypeName(isNullable: Boolean) extends CypherTypeName {
  protected val typeName: String = "LOCAL DATETIME"

  override def hasValueRepresentation: Boolean = true

  override def possibleValueRepresentations: List[ValueRepresentation] = List(ValueRepresentation.LOCAL_DATE_TIME)
}

case class ZonedDateTimeTypeName(isNullable: Boolean) extends CypherTypeName {
  protected val typeName: String = "ZONED DATETIME"

  override def hasValueRepresentation: Boolean = true

  override def possibleValueRepresentations: List[ValueRepresentation] = List(ValueRepresentation.ZONED_DATE_TIME)
}

case class DurationTypeName(isNullable: Boolean) extends CypherTypeName {
  protected val typeName: String = "DURATION"

  override def hasValueRepresentation: Boolean = true

  override def possibleValueRepresentations: List[ValueRepresentation] = List(ValueRepresentation.DURATION)
}

case class PointTypeName(isNullable: Boolean) extends CypherTypeName {
  protected val typeName: String = "POINT"

  override def hasValueRepresentation: Boolean = true

  override def possibleValueRepresentations: List[ValueRepresentation] = List(ValueRepresentation.GEOMETRY)
}

case class NodeTypeName(isNullable: Boolean) extends CypherTypeName {
  protected val typeName: String = "NODE"
}

case class RelationshipTypeName(isNullable: Boolean) extends CypherTypeName {
  protected val typeName: String = "RELATIONSHIP"
}

case class MapTypeName(isNullable: Boolean) extends CypherTypeName {
  protected val typeName: String = "MAP"
}

case class ListTypeName(innerType: CypherTypeName, isNullable: Boolean) extends CypherTypeName {
  protected val typeName: String = s"LIST<${innerType.description}>"

  override def hasValueRepresentation: Boolean = possibleValueRepresentations.nonEmpty

  override def possibleValueRepresentations: List[ValueRepresentation] = innerType match {
    case _: BooleanTypeName => List(ValueRepresentation.BOOLEAN_ARRAY)
    case _: StringTypeName  => List(ValueRepresentation.TEXT_ARRAY)
    case _: IntegerTypeName => List(
        ValueRepresentation.INT8_ARRAY,
        ValueRepresentation.INT16_ARRAY,
        ValueRepresentation.INT32_ARRAY,
        ValueRepresentation.INT64_ARRAY
      )
    case _: FloatTypeName         => List(ValueRepresentation.FLOAT32_ARRAY, ValueRepresentation.FLOAT64_ARRAY)
    case _: DateTypeName          => List(ValueRepresentation.DATE_ARRAY)
    case _: LocalTimeTypeName     => List(ValueRepresentation.LOCAL_TIME_ARRAY)
    case _: ZonedTimeTypeName     => List(ValueRepresentation.ZONED_TIME_ARRAY)
    case _: LocalDateTimeTypeName => List(ValueRepresentation.LOCAL_DATE_TIME_ARRAY)
    case _: ZonedDateTimeTypeName => List(ValueRepresentation.ZONED_DATE_TIME_ARRAY)
    case _: DurationTypeName      => List(ValueRepresentation.DURATION_ARRAY)
    case _: PointTypeName         => List(ValueRepresentation.GEOMETRY_ARRAY)
    case _                        => List.empty
  }
}

case class PathTypeName(isNullable: Boolean) extends CypherTypeName {
  protected val typeName: String = "PATH"
}

case class PropertyValueTypeName(isNullable: Boolean) extends CypherTypeName {
  protected val typeName: String = "PROPERTY VALUE"
}

case class AnyTypeName(isNullable: Boolean) extends CypherTypeName {
  protected val typeName: String = "ANY"
}
