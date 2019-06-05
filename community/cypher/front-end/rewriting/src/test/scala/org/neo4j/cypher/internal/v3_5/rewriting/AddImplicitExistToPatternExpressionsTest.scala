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
package org.neo4j.cypher.internal.v3_5.rewriting

import org.neo4j.cypher.internal.v3_5.ast._
import org.neo4j.cypher.internal.v3_5.ast.semantics.SemanticState
import org.neo4j.cypher.internal.v3_5.rewriting.rewriters.addImplicitExistToPatternExpressions
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

class AddImplicitExistToPatternExpressionsTest extends CypherFunSuite with AstConstructionTestSupport {

  import org.neo4j.cypher.internal.v3_5.parser.ParserFixture.parser

  testRewrite(
    "MATCH (n) WHERE (n)--(m) RETURN n",
    "MATCH (n) WHERE EXISTS((n)--(m)) RETURN n")

  testRewrite(
    "MATCH (n) WHERE NOT (n)--(m) RETURN n",
    "MATCH (n) WHERE NOT EXISTS((n)--(m)) RETURN n")

  testNoRewrite("RETURN (n)--(m)")

  testNoRewrite("RETURN [(n)--(m) | n.prop]")

  testNoRewrite("MATCH (n) WHERE 2 IN [(n)--(m) | n.prop] RETURN n")

  private def testNoRewrite(query: String): Unit = {
    test(query + " is not rewritten") {
      assertRewrite(query, query)
    }
  }

  private def testRewrite(originalQuery: String, expectedQuery: String): Unit = {
    test(originalQuery + " is rewritten to " + expectedQuery) {
      assertRewrite(originalQuery, expectedQuery)
    }
  }

  private def assertRewrite(originalQuery: String, expectedQuery: String) = {
    val original = parser.parse(originalQuery)
    val expected = parser.parse(expectedQuery)

    val checkResult = original.semanticCheck(SemanticState.clean)
    val rewriter = addImplicitExistToPatternExpressions(checkResult.state)

    val result = original.rewrite(rewriter)
    assert(result === expected)
  }
}
