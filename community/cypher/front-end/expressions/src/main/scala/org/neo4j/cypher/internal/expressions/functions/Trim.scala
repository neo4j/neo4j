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
import org.neo4j.cypher.internal.util.symbols.CTString

case object Trim extends Function {
  def name = "trim"

  override val signatures = Vector(
    FunctionTypeSignature(
      function = this,
      names = Vector("trimSpecification", "input"),
      argumentTypes = Vector(CTString, CTString),
      outputType = CTString,
      description = "Returns the given `STRING` with leading and trailing whitespace removed.",
      category = Category.STRING,
      overrideDefaultAsString = Some(name + "(input :: STRING) :: STRING"),
      overriddenArgumentTypeName = Some(Map("trimSpecification" -> "[LEADING, TRAILING, BOTH]"))
    ),
    FunctionTypeSignature(
      function = this,
      names = Vector("trimSpecification", "trimCharacterString", "input"),
      argumentTypes = Vector(CTString, CTString, CTString),
      outputType = CTString,
      description = "Returns the given `STRING` with leading and/or trailing `trimCharacterString` removed.",
      category = Category.STRING,
      overrideDefaultAsString =
        Some(name + "([[LEADING | TRAILING | BOTH] [trimCharacterString :: STRING] FROM] input :: STRING) :: STRING"),
      overriddenArgumentTypeName = Some(Map("trimSpecification" -> "[LEADING, TRAILING, BOTH]"))
    )
  )
}
