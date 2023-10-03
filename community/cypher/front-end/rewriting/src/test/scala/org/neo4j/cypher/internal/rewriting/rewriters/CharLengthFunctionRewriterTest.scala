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
package org.neo4j.cypher.internal.rewriting.rewriters

import org.neo4j.cypher.internal.rewriting.RewriteTest
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.TestName

class CharLengthFunctionRewriterTest extends CypherFunSuite with RewriteTest with TestName {

  override val rewriterUnderTest: Rewriter = CharLengthFunctionRewriter.instance

  override protected def assertRewrite(originalQuery: String, expectedQuery: String): Unit =
    super.assertRewrite(originalQuery, expectedQuery)

  override protected def assertIsNotRewritten(query: String): Unit =
    super.assertIsNotRewritten(query)

  test("RETURN size('abc') AS result") {
    assertIsNotRewritten(testName)
  }

  test("RETURN character_length('abc') AS result") {
    assertIsNotRewritten(testName)
  }

  test("RETURN char_length('abc') AS result") {
    assertRewrite(testName, "RETURN character_length('abc') AS result")
  }
}
