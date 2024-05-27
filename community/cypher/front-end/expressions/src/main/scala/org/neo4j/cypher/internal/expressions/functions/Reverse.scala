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
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.ClosedDynamicUnionType

case object Reverse extends Function {
  def name = "reverse"

  override val signatures = Vector(
    FunctionTypeSignature(
      this,
      ClosedDynamicUnionType(Set(CTString, CTList(CTAny)))(InputPosition.NONE),
      ClosedDynamicUnionType(Set(CTString, CTList(CTAny)))(InputPosition.NONE),
      "Returns a `STRING` or `LIST<ANY>` in which the order of all characters or elements in the given `STRING` or `LIST<ANY>` have been reversed.",
      Category.STRING
    )
  )
}
