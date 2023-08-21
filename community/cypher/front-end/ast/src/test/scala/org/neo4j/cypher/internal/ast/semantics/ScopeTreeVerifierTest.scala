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
package org.neo4j.cypher.internal.ast.semantics

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.semantics.ScopeTestHelper.intSymbol
import org.neo4j.cypher.internal.util.helpers.StringHelper.RichString
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ScopeTreeVerifierTest extends CypherFunSuite with AstConstructionTestSupport {

  test("should reject scopes mapping the wrong name to a symbol") {
    val x = intSymbol("x", varFor("x"))
    val a = intSymbol("a", varFor("a"))
    val given = Scope(Map("a" -> a, "b" -> x), Seq())

    val result = ScopeTreeVerifier.verify(given).map(_.fixNewLines)

    result.head should startWith(
      s"'b' points to symbol with different name '$x' in scope ${given.toIdString}. Scope tree:"
    )
  }
}
