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

import org.neo4j.cypher.internal.expressions.FunctionTypeSignature
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.ClosedDynamicUnionType

case object ToBoolean extends Function {
  override def name = "toBoolean"

  override val signatures = Vector(
    FunctionTypeSignature(
      this,
      ClosedDynamicUnionType(Set(CTBoolean, CTString, CTInteger))(InputPosition.NONE),
      CTBoolean,
      "Converts a `BOOLEAN`, `STRING` or `INTEGER` value to a `BOOLEAN` value. For `INTEGER` values, 0 is defined to be false and any other `INTEGER` is defined to be true.",
      Category.SCALAR
    )
  )
}
