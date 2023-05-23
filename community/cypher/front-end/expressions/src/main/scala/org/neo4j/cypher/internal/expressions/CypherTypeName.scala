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
  def description: String = s"$typeName"
  override def toString: String = description
}

/* Types not yet covered:
 *  NOTHING
 *  NULL
 *  NODE
 *  RELATIONSHIP
 *  MAP
 *  LIST
 *  PATH
 *  ANY<...>
 *  PROPERTY VALUE
 *  ANY
 */

case class BooleanTypeName() extends CypherTypeName {
  protected val typeName: String = "BOOLEAN"
}

case class StringTypeName() extends CypherTypeName {
  protected val typeName: String = "STRING"
}

case class IntegerTypeName() extends CypherTypeName {
  protected val typeName: String = "INTEGER"
}

case class FloatTypeName() extends CypherTypeName {
  protected val typeName: String = "FLOAT"
}

case class DateTypeName() extends CypherTypeName {
  protected val typeName: String = "DATE"
}

case class LocalTimeTypeName() extends CypherTypeName {
  protected val typeName: String = "LOCAL TIME"
}

case class ZonedTimeTypeName() extends CypherTypeName {
  protected val typeName: String = "ZONED TIME"
}

case class LocalDateTimeTypeName() extends CypherTypeName {
  protected val typeName: String = "LOCAL DATETIME"
}

case class ZonedDateTimeTypeName() extends CypherTypeName {
  protected val typeName: String = "ZONED DATETIME"
}

case class DurationTypeName() extends CypherTypeName {
  protected val typeName: String = "DURATION"
}

case class PointTypeName() extends CypherTypeName {
  protected val typeName: String = "POINT"
}
