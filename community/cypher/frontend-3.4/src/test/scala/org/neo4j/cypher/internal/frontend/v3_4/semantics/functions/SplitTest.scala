/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v3_4.semantics.functions

import org.neo4j.cypher.internal.util.v3_4.symbols._

import scala.languageFeature.existentials

class SplitTest extends FunctionTestBase("split")  {

  test("shouldAcceptCorrectTypes") {
    testValidTypes(CTString,CTString)(CTList(CTString))
  }

  test("shouldFailTypeCheckForIncompatibleArguments") {
    testInvalidApplication(CTString, CTBoolean)(
      "Type mismatch: expected String but was Boolean"
    )

    testInvalidApplication(CTInteger, CTString)(
      "Type mismatch: expected String but was Integer"
    )
  }

  test("shouldFailIfWrongNumberOfArguments") {
    testInvalidApplication()(
      "Insufficient parameters for function 'split'"
    )
    testInvalidApplication(CTString)(
      "Insufficient parameters for function 'split'"
    )
    testInvalidApplication(CTString,CTString,CTString)(
      "Too many parameters for function 'split'"
    )
  }
}
