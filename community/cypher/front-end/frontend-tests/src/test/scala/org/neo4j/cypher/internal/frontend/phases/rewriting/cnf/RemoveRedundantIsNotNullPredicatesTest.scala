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
package org.neo4j.cypher.internal.frontend.phases.rewriting.cnf

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.rewriting.RewriteTest
import org.neo4j.cypher.internal.rewriting.rewriters.astRewriters.NormalizePredicates
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.inSequence
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class RemoveRedundantIsNotNullPredicatesTest extends CypherFunSuite with RewriteTest {

  override def rewriterUnderTest: Rewriter = {
    val state = mock[BaseState]
    val context = mock[BaseContext]
    when(context.cancellationChecker).thenReturn(CancellationChecker.neverCancelled())

    inSequence(
      NormalizePredicates.getRewriter(
        SemanticState.clean,
        Map.empty,
        new AnonymousVariableNameGenerator(),
        CancellationChecker.neverCancelled(),
        CypherVersion.Cypher25
      ),
      RemoveRedundantIsNotNullPredicates.instance(state, context)
    )
  }

  test("should remove IS NOT NULL in MATCH WHERE") {
    assertRewrite(
      "MATCH (n) WHERE n.prop IS NOT NULL AND n.prop = 123 RETURN n.prop AS result",
      "MATCH (n) WHERE n.prop = 123 RETURN n.prop AS result"
    )
  }

  test("should remove IS NOT NULL in node pattern") {
    assertRewrite(
      "MATCH (n WHERE n.prop IS NOT NULL) WHERE n.prop = 123 RETURN n.prop AS result",
      "MATCH (n) WHERE n.prop = 123 RETURN n.prop AS result"
    )
  }

  test("should remove IS NOT NULL with property predicate in node pattern") {
    assertRewrite(
      "MATCH (n {prop: 123}) WHERE n.prop IS NOT NULL RETURN n.prop AS result",
      "MATCH (n) WHERE n.prop = 123 RETURN n.prop AS result"
    )
  }

  test("should remove IS NOT NULL in WITH WHERE") {
    assertRewrite(
      "MATCH (n) WITH n WHERE n.prop IS NOT NULL AND n.prop = 123 RETURN n.prop AS result",
      "MATCH (n) WITH n WHERE n.prop = 123 RETURN n.prop AS result"
    )
  }

  test("should remove multiple IS NOT NULL") {
    assertRewrite(
      """MATCH (n), (m)
        |WHERE
        |  (n.x IS NOT NULL AND m.y IS NOT NULL) AND
        |  (n.x = m.x AND n.y = m.y)
        |RETURN n AS result
        |""".stripMargin,
      """MATCH (n), (m)
        |WHERE n.x = m.x AND n.y = m.y
        |RETURN n AS result
        |""".stripMargin.stripMargin
    )
  }

  test("should remove multiple IS NOT NULL before and after equality predicate") {
    assertRewrite(
      """MATCH (n), (m)
        |WHERE
        |  n.x IS NOT NULL AND
        |  (n.x = m.x AND n.y = m.y) AND
        |  n.y IS NOT NULL
        |RETURN n AS result
        |""".stripMargin,
      """MATCH (n), (m)
        |WHERE n.x = m.x AND n.y = m.y
        |RETURN n AS result
        |""".stripMargin.stripMargin
    )
  }

  test("should remove IS NOT NULL inside OR") {
    assertRewrite(
      """MATCH (n), (m)
        |WHERE
        |  (n.x IS NOT NULL AND n.x = m.x)
        |  OR
        |  (n.y IS NOT NULL AND n.y = m.y)
        |RETURN n AS result
        |""".stripMargin,
      """MATCH (n), (m)
        |WHERE
        |  n.x = m.x
        |  OR
        |  n.y = m.y
        |RETURN n AS result
        |""".stripMargin.stripMargin
    )
  }

  test("should remove IS NOT NULL in FILTER") {
    assertRewrite(
      CypherVersion.Cypher25,
      "MATCH (n) FILTER n.prop IS NOT NULL AND n.prop = 123 RETURN n.prop AS result",
      "MATCH (n) FILTER n.prop = 123 RETURN n.prop AS result"
    )
  }

  test("should remove IS NOT NULL inside a subquery expression") {
    assertRewrite(
      """MATCH (n)
        |RETURN EXISTS {
        |  MATCH (n)-[r]->(m)
        |  WHERE m.prop IS NOT NULL AND m.prop = 123
        |} AS result
        |""".stripMargin,
      """MATCH (n)
        |RETURN EXISTS {
        |  MATCH (n)-[r]->(m)
        |  WHERE m.prop = 123
        |} AS result
        |""".stripMargin
    )
  }

  test("should not remove IS NOT NULL in RETURN") {
    assertIsNotRewritten("MATCH (n) RETURN (n.prop IS NOT NULL AND n.prop = 123) AS result")
  }

  test("should not remove IS NOT NULL in WITH") {
    assertIsNotRewritten("MATCH (n) WITH (n.prop IS NOT NULL AND n.prop = 123) AS result RETURN result")
  }

  test("should not remove IS NOT NULL inside NOT") {
    assertIsNotRewritten("MATCH (n) WHERE NOT (n.prop IS NOT NULL AND n.prop = 123) RETURN n AS result")
  }
}
