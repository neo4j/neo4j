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
package org.neo4j.cypher.internal.expressions.functions

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.expressions.FunctionTypeSignature
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.CTUUID

case object UUIDConstructor extends Function {
  def name = "uuid"

  override val signatures = Vector(
    FunctionTypeSignature(
      function = this,
      names = Vector(),
      argumentTypes = Vector(),
      outputType = CTUUID,
      description = "Returns a randomly generated `UUID`.",
      category = Category.UUID,
      scopes = Set(CypherVersion.Cypher25)
    ),
    FunctionTypeSignature(
      function = this,
      names = Vector("name"),
      argumentTypes = Vector(CTString),
      outputType = CTUUID,
      description = "Converts a `STRING` value to a `UUID`.",
      category = Category.UUID,
      argumentDescriptions = Map(
        "name" -> "The `STRING` must be 32 hexadecimal digits displayed in 5 groups, separated by 4 hyphens."
      ),
      scopes = Set(CypherVersion.Cypher25)
    ),
    FunctionTypeSignature(
      function = this,
      names = Vector("mostSigBits", "leastSigBits"),
      argumentTypes = Vector(CTInteger, CTInteger),
      outputType = CTUUID,
      description = "Converts two `INTEGER` values to a `UUID`.",
      category = Category.UUID,
      argumentDescriptions = Map(
        "mostSigBits" -> "The most significant bits.",
        "leastSigBits" -> "The least significant bits."
      ),
      scopes = Set(CypherVersion.Cypher25)
    )
  )
}
