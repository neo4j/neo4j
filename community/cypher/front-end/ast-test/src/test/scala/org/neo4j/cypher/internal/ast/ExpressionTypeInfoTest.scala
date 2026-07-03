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
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.ast.semantics.ExpressionTypeInfo
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.TypeSpec
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ExpressionTypeInfoTest extends CypherFunSuite {

  test("Should reuse ExpressionTypeInfo") {
    val a = ExpressionTypeInfo(TypeSpec.exact(CTBoolean), None)
    val b = ExpressionTypeInfo(TypeSpec.exact(CTBoolean), None)

    (a eq b) shouldBe true
  }

  test("Should not reuse ExpressionTypeInfo for different types") {
    val a = ExpressionTypeInfo(TypeSpec.exact(CTBoolean), Some(TypeSpec.exact(CTBoolean)))
    val b = ExpressionTypeInfo(TypeSpec.exact(CTBoolean), None)

    a should not equal b
  }
}
