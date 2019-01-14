/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.v3_5.ast.semantics

import org.neo4j.cypher.internal.v3_5.ast.semantics.ScopeTestHelper._
import org.neo4j.cypher.internal.v3_5.util.helpers.StringHelper.RichString
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

class ScopeTreeVerificationTest extends CypherFunSuite {

  test("should reject scopes mapping the wrong name to a symbol") {
    val given = Scope(Map("a" -> intSymbol("a", 3), "b" -> intSymbol("x", 5)), Seq())

    val result = ScopeTreeVerifier.verify(given).map(_.fixNewLines)

    result should equal(Seq(s"""'b' points to symbol with different name 'x@5(5): Integer' in scope ${given.toIdString}. Scope tree:
                               |${given.toIdString} {
                               |  a: 3
                               |  b: 5
                               |}
                               |""".stripMargin.fixNewLines))
  }
}
