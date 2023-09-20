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

case class MapType(isNullable: Boolean)(val position: InputPosition) extends CypherType {
  val parentType: CypherType = CTAny
  override val toString = "Map"
  override val toCypherTypeString = "MAP"

  override def sortOrder: Int = CypherTypeOrder.MAP.id

  override def updateIsNullable(isNullable: Boolean): CypherType = this.copy(isNullable = isNullable)(position)

  def withPosition(newPosition: InputPosition): CypherType = this.copy()(position = newPosition)
}
