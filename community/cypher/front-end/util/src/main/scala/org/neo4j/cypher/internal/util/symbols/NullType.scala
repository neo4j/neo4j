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

case class NullType()(val position: InputPosition) extends CypherType {
  val parentType: CypherType = this
  override val toString = "Null"
  override val toCypherTypeString = "NULL"

  override def sortOrder: Int = CypherTypeOrder.NULL.id
  override def isNullable: Boolean = true

  override def updateIsNullable(isNullable: Boolean): CypherType =
    if (isNullable) this
    else NothingType()(position)

  override def isSubtypeOf(otherCypherType: CypherType): Boolean = otherCypherType.isNullable

  def withPosition(newPosition: InputPosition): CypherType = this.copy()(position = newPosition)

  // NULL should not be used in general areas of Cypher, intended for use in surface based areas e.g. Type Predicate Expressions
  override lazy val covariant: TypeSpec =
    throw new UnsupportedOperationException("NULL type is not supported for use in this context")

  override lazy val invariant: TypeSpec =
    throw new UnsupportedOperationException("NULL type is not supported for use in this context")

  override lazy val contravariant: TypeSpec =
    throw new UnsupportedOperationException("NULL type is not supported for use in this context")

  override def leastUpperBound(other: CypherType): CypherType =
    throw new UnsupportedOperationException("NULL type is not supported for use in this context")

  override def greatestLowerBound(other: CypherType): Option[CypherType] =
    throw new UnsupportedOperationException("NULL type is not supported for use in this context")
}
