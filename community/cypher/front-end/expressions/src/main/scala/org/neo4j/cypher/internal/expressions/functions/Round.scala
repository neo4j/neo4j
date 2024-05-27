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
import org.neo4j.cypher.internal.util.symbols.CTFloat
import org.neo4j.cypher.internal.util.symbols.CTNumber
import org.neo4j.cypher.internal.util.symbols.CTString

case object Round extends Function {
  def name = "round"

  override val signatures = Vector(
    FunctionTypeSignature(
      this,
      CTFloat,
      CTFloat,
      "Returns the value of a number rounded to the nearest `INTEGER`.",
      category = Category.NUMERIC
    ),
    FunctionTypeSignature(
      function = this,
      names = Vector("value", "precision"),
      argumentTypes = Vector(CTFloat, CTNumber),
      outputType = CTFloat,
      description = "Returns the value of a number rounded to the specified precision using rounding mode HALF_UP.",
      category = Category.NUMERIC
    ),
    FunctionTypeSignature(
      function = this,
      names = Vector("value", "precision", "mode"),
      argumentTypes = Vector(CTFloat, CTNumber, CTString),
      outputType = CTFloat,
      description =
        "Returns the value of a number rounded to the specified precision with the specified rounding mode.",
      category = Category.NUMERIC
    )
  )
}
