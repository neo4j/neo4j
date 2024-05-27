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
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTString

case object ToStringOrNull extends Function {
  override def name = "toStringOrNull"

  override val signatures = Vector(
    FunctionTypeSignature(
      this,
      CTAny,
      CTString,
      "Converts an `INTEGER`, `FLOAT`, `BOOLEAN`, `POINT` or temporal type (i.e. `DATE`, `ZONED TIME`, `LOCAL TIME`, `ZONED DATETIME`, `LOCAL DATETIME` or `DURATION`) value to a `STRING`, or null if the value cannot be converted.",
      Category.STRING
    )
    // TODO: Also Category.SCALAR, in case we entangle the categories. For now we keep the same as toString
  )
}
