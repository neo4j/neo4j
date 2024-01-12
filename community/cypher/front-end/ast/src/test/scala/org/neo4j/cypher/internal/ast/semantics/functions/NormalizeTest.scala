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

import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTFloat
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTString

class NormalizeTest extends FunctionTestBase("normalize") {

  test("shouldFailIfWrongArguments") {
    testInvalidApplication()("Insufficient parameters for function 'normalize'")
    testInvalidApplication(CTString, CTString, CTString)("Too many parameters for function 'normalize'")
    testInvalidApplication(CTString, CTString, CTString, CTString)("Too many parameters for function 'normalize'")
    testInvalidApplication(CTBoolean)("Type mismatch: expected String but was Boolean")
    testInvalidApplication(CTInteger, CTString)("Type mismatch: expected String but was Integer")
    testInvalidApplication(CTString, CTInteger)("Type mismatch: expected String but was Integer")
  }

  test("shouldHandleCorrectTypes") {
    testValidTypes(CTString)(CTString)
    testValidTypes(CTString, CTString)(CTString)
  }

  test("shouldAllowMixedTypes") {
    testValidTypes(CTFloat | CTString)(CTString)
  }
}
