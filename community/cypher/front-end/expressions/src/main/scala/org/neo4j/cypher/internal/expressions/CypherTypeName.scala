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

sealed trait CypherTypeName {
  protected def typeName: String
  // e.g BOOLEAN set(true, false, null) is nullable, BOOLEAN NOT NULL set(true, false) is not
  def isNullable: Boolean
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
}

case class StringTypeName(isNullable: Boolean) extends CypherTypeName {
  protected val typeName: String = "STRING"
}

case class IntegerTypeName(isNullable: Boolean) extends CypherTypeName {
  protected val typeName: String = "INTEGER"
}

case class FloatTypeName(isNullable: Boolean) extends CypherTypeName {
  protected val typeName: String = "FLOAT"
}

case class DateTypeName(isNullable: Boolean) extends CypherTypeName {
  protected val typeName: String = "DATE"
}

case class LocalTimeTypeName(isNullable: Boolean) extends CypherTypeName {
  protected val typeName: String = "LOCAL TIME"
}

case class ZonedTimeTypeName(isNullable: Boolean) extends CypherTypeName {
  protected val typeName: String = "ZONED TIME"
}

case class LocalDateTimeTypeName(isNullable: Boolean) extends CypherTypeName {
  protected val typeName: String = "LOCAL DATETIME"
}

case class ZonedDateTimeTypeName(isNullable: Boolean) extends CypherTypeName {
  protected val typeName: String = "ZONED DATETIME"
}

case class DurationTypeName(isNullable: Boolean) extends CypherTypeName {
  protected val typeName: String = "DURATION"
}

case class PointTypeName(isNullable: Boolean) extends CypherTypeName {
  protected val typeName: String = "POINT"
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
