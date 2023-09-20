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
package org.neo4j.cypher.internal.ast.semantics.functions

import org.neo4j.cypher.internal.expressions.Expression.SemanticContext
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTFloat
import org.neo4j.cypher.internal.util.symbols.CTInteger

class PercentileDiscTest extends FunctionTestBase("percentileDisc") {

  override val context = SemanticContext.Results

  test("shouldHandleAllSpecializations") {
    testValidTypes(CTInteger, CTInteger)(CTFloat | CTInteger)
    testValidTypes(CTInteger, CTFloat)(CTFloat | CTInteger)
    testValidTypes(CTFloat, CTInteger)(CTFloat | CTInteger)
    testValidTypes(CTFloat, CTFloat)(CTFloat | CTInteger)
  }

  test("shouldHandleCombinedSpecializations") {
    testValidTypes(CTFloat | CTInteger, CTFloat | CTInteger)(CTInteger | CTFloat)
  }

  test("shouldFailIfWrongArguments") {
    testInvalidApplication(CTFloat)("Insufficient parameters for function 'percentileDisc'")
    testInvalidApplication(CTFloat, CTFloat, CTFloat)("Too many parameters for function 'percentileDisc'")
  }

  test("shouldFailTypeCheckWhenAddingIncompatible") {
    testInvalidApplication(CTInteger, CTBoolean)(
      "Type mismatch: expected Float but was Boolean"
    )
    testInvalidApplication(CTBoolean, CTInteger)(
      "Type mismatch: expected Float or Integer but was Boolean"
    )
  }
}
