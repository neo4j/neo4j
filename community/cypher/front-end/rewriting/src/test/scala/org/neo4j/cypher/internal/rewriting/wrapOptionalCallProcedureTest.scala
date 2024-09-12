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
package org.neo4j.cypher.internal.rewriting

import org.neo4j.cypher.internal.rewriting.rewriters.wrapOptionalCallProcedure
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class wrapOptionalCallProcedureTest extends CypherFunSuite with RewriteTest {

  override val rewriterUnderTest: Rewriter = wrapOptionalCallProcedure.getRewriter(null)

  test("rewrite optional call procedure") {
    assertRewrite(
      "OPTIONAL CALL foo() YIELD a, b RETURN a, b",
      "OPTIONAL CALL (*) { CALL foo() YIELD a, b RETURN a AS a, b AS b } RETURN a, b"
    )
    assertRewrite(
      "OPTIONAL CALL foo() YIELD a, b WHERE a > 0 RETURN a, b",
      "OPTIONAL CALL (*) { CALL foo() YIELD a, b WHERE a > 0 RETURN a AS a, b AS b } RETURN a, b"
    )
  }

  test("does not rewrite") {
    assertIsNotRewritten("CALL foo() YIELD a, b WITH * WHERE a > b RETURN *")
    assertIsNotRewritten("CALL foo() YIELD a, b RETURN *")
  }
}
