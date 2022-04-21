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
package org.neo4j.cypher.internal.expressions.functions

import org.neo4j.cypher.internal.expressions.TypeSignature
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTString

case object ToBoolean extends Function {
  override def name = "toBoolean"

  override val signatures = Vector(
    TypeSignature(this, CTString, CTBoolean, "Converts a string value to a boolean value.", Category.SCALAR),
    TypeSignature(this, CTBoolean, CTBoolean, "Converts a boolean value to a boolean value.", Category.SCALAR),
    TypeSignature(
      this,
      CTInteger,
      CTBoolean,
      "Converts a integer value to a boolean value. 0 is defined to be FALSE and any other integer is defined to be TRUE.",
      Category.SCALAR
    )
  )
}
