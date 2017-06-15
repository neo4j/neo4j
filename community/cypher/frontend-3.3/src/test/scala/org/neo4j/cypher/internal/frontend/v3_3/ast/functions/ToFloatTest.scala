/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.frontend.v3_3.ast.functions

import org.neo4j.cypher.internal.frontend.v3_3.symbols._

class ToFloatTest extends FunctionTestBase("toFloat")  {

  test("shouldAcceptCorrectTypes") {
    testValidTypes(CTString)(CTFloat)
    testValidTypes(CTFloat)(CTFloat)
    testValidTypes(CTInteger)(CTFloat)
    testValidTypes(CTNumber.covariant)(CTFloat)
    testValidTypes(CTAny.covariant)(CTFloat)
  }

  test("shouldFailTypeCheckForIncompatibleArguments") {
    testInvalidApplication(CTList(CTAny).covariant)(
      "Type mismatch: expected Float, Integer, Number or String but was List<T>"
    )

    testInvalidApplication(CTNode)(
      "Type mismatch: expected Float, Integer, Number or String but was Node"
    )

    testInvalidApplication(CTBoolean)(
      "Type mismatch: expected Float, Integer, Number or String but was Boolean"
    )
  }

  test("shouldFailIfWrongNumberOfArguments") {
    testInvalidApplication()(
      "Insufficient parameters for function 'toFloat'"
    )
    testInvalidApplication(CTString, CTString)(
      "Too many parameters for function 'toFloat'"
    )
  }
}
