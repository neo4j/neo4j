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

import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTDate
import org.neo4j.cypher.internal.util.symbols.CTFloat
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTNumber
import org.neo4j.cypher.internal.util.symbols.CTString

class ToIntegerTest extends FunctionTestBase("toInteger") {

  test("shouldAcceptCorrectTypes") {
    testValidTypes(CTString)(CTInteger)
    testValidTypes(CTFloat)(CTInteger)
    testValidTypes(CTInteger)(CTInteger)
    testValidTypes(CTNumber.covariant)(CTInteger)
    testValidTypes(CTAny.covariant)(CTInteger)
    testValidTypes(CTBoolean)(CTInteger)
  }

  // Currently we coerce CTList to boolean. This is going away and when it does we should reinstate this test
  ignore("shouldFailTypeCheckForIncompatibleListArgument") {
    testInvalidApplication(CTList(CTAny).covariant)(
      "Type mismatch: expected Boolean, Float, Integer or String but was List<T>"
    )
  }

  test("shouldFailTypeCheckForIncompatibleArguments") {
    testInvalidApplication(CTNode)(
      "Type mismatch: expected Boolean, Float, Integer or String but was Node"
    )

    testInvalidApplication(CTDate)(
      "Type mismatch: expected Boolean, Float, Integer or String but was Date"
    )
  }

  test("shouldFailIfWrongNumberOfArguments") {
    testInvalidApplication()(
      "Insufficient parameters for function 'toInteger'"
    )
    testInvalidApplication(CTString, CTString)(
      "Too many parameters for function 'toInteger'"
    )
  }
}
