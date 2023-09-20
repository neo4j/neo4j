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

case class NothingType()(val position: InputPosition) extends CypherType {
  val parentType: CypherType = this
  override val toString = "Nothing"
  override val toCypherTypeString = "NOTHING"

  override def sortOrder: Int = CypherTypeOrder.NOTHING.id

  // The NOTHING type never includes `null` but should not have a `NOT NULL`
  override def isNullable: Boolean = false
  override def description: String = toCypherTypeString

  override def updateIsNullable(isNullable: Boolean): CypherType = this

  override def isSubtypeOf(otherCypherType: CypherType): Boolean = true

  def withPosition(newPosition: InputPosition): CypherType = this.copy()(position = newPosition)

  // NOTHING should not be used in general areas of Cypher, intended for use in surface based areas e.g. Type Predicate Expressions
  override lazy val covariant: TypeSpec =
    throw new UnsupportedOperationException("NOTHING type is not supported for use in this context")

  override lazy val invariant: TypeSpec =
    throw new UnsupportedOperationException("NOTHING type is not supported for use in this context")

  override lazy val contravariant: TypeSpec =
    throw new UnsupportedOperationException("NOTHING type is not supported for use in this context")

  override def leastUpperBound(other: CypherType): CypherType =
    throw new UnsupportedOperationException("NOTHING type is not supported for use in this context")

  override def greatestLowerBound(other: CypherType): Option[CypherType] =
    throw new UnsupportedOperationException("NOTHING type is not supported for use in this context")

}
