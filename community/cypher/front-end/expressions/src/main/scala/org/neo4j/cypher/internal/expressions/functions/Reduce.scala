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
import org.neo4j.cypher.internal.util.symbols.CTList

// this implementation exists only to handle the case where "reduce(x = 0, x in y : foo)" is parsed as a function invocation,
// rather than a ReduceExpression
case object Reduce extends Function {
  def name = "reduce"

  // TODO: Get specification formalized by CLG
  override val signatures = Vector(
    FunctionTypeSignature(
      function = this,
      names = Vector("accumulator", "variable"),
      argumentTypes = Vector(CTAny, CTList(CTAny)),
      outputType = CTAny,
      description =
        "Runs an expression against individual elements of a `LIST<ANY>`, storing the result of the expression in an accumulator.",
      category = Category.LIST,
      overrideDefaultAsString = Some(
        name + "(accumulator :: VARIABLE = initial :: ANY, variable :: VARIABLE IN list :: LIST<ANY> | expression :: ANY) :: ANY"
      )
    )
  )
}
