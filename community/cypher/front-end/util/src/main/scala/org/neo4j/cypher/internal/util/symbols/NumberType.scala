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

case class NumberType(isNullable: Boolean)(val position: InputPosition) extends CypherType {
  val parentType: AnyType = CTAny
  override val toString = "Number"
  override val toCypherTypeString = "NUMBER"

  override def normalizedCypherTypeString(): String = {
    val normalizedType = CypherType.normalizeTypes(this)
    if (normalizedType.isNullable) normalizedType.toCypherTypeString
    else s"${normalizedType.toCypherTypeString} NOT NULL"
  }
  override def sortOrder: Int = CypherTypeOrder.CLOSED_DYNAMIC_UNION.id

  override def hasCypherParserSupport: Boolean = false

  override def hasValueRepresentation: Boolean = true

  override def updateIsNullable(isNullable: Boolean): CypherType = this.copy(isNullable = isNullable)(position)

  def withPosition(newPosition: InputPosition): CypherType = this.copy()(position = newPosition)
}
