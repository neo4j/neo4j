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
package org.neo4j.cypher.internal.frontend.phases

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class IsolateSubqueriesInMutatingPatternsNoSemanticAnalysisTest extends CypherFunSuite with RewritePhaseTest
    with AstConstructionTestSupport {

  override def rewriterPhaseUnderTest: Transformer[BaseContext, BaseState, BaseState] =
    IsolateSubqueriesInMutatingPatterns

  override def astRewriteAndAnalyze: Boolean = false

  test("Does not rewrite subquery expression in MERGE") {
    // Must run without SemanticAnalysis, because it is forbidden
    assertNotRewritten(
      "MERGE (a {p: COUNT { MATCH (b)  }})"
    )
  }
}

class IsolateSubqueriesInMutatingPatternsTest extends CypherFunSuite with RewritePhaseTest
    with AstConstructionTestSupport {

  // Rewrite away WITH * in tests directly
  override def rewriterPhaseUnderTest: Transformer[BaseContext, BaseState, BaseState] =
    IsolateSubqueriesInMutatingPatterns andThen
      SemanticAnalysis(false, semanticFeatures: _*) andThen
      ExpandStarRewriter

  test("Rewrites subquery expression in CREATE") {
    assertRewritten(
      "CREATE (a {p: COUNT { MATCH (b) }})",
      """WITH COUNT { MATCH (b) } AS `  UNNAMED0`
        |CREATE (a {p: `  UNNAMED0`})""".stripMargin
    )
  }

  test("Rewrites subquery expression in CREATE that has a dependency on the previous clause") {
    assertRewritten(
      """MATCH (b)
        |CREATE (a {p: COUNT { MATCH (b) }})""".stripMargin,
      """MATCH (b)
        |WITH b, COUNT { MATCH (b) } AS `  UNNAMED0`
        |CREATE (a {p: `  UNNAMED0`})""".stripMargin
    )
  }

  test("Rewrites subquery expression in CREATE that has a dependency on a previous clause") {
    assertRewritten(
      """MATCH (b)
        |WITH b
        |CREATE (c)
        |CREATE (a {p: COUNT { MATCH (b) }})""".stripMargin,
      """MATCH (b)
        |WITH b
        |CREATE (c)
        |WITH b, c, COUNT { MATCH (b) } AS `  UNNAMED0`
        |CREATE (a {p: `  UNNAMED0`})""".stripMargin
    )
  }

  test("Does not rewrite CREATE wih cross-references") {
    // These are deprecated, but we cannot rewrite these and keep the same semantics.
    // The queries are going to be non-deterministic, until we forbid them in 6.0

    assertNotRewritten("CREATE (a), (b {prop: EXISTS { (a)-[r2]->(c) }})")
    assertNotRewritten("CREATE (a)-[r:R]->(b {prop: EXISTS { (a)-[r2]->(c) }})")
    assertNotRewritten("CREATE (a)-[r:R]->(b {prop: CASE WHEN true THEN EXISTS { (a)-[r2]->(c) } END})")
    assertNotRewritten("CREATE (a)-[r:R]->(b {prop: EXISTS { (c) WHERE EXISTS { (c)<-[r2]-(a) }}})")
  }

  test("Rewrites subquery expression in REMOVE") {
    assertRewritten(
      "REMOVE (COLLECT { MATCH (a) RETURN a }[0]).prop",
      """WITH COLLECT { MATCH (a) RETURN a }[0] AS `  UNNAMED0`
        |REMOVE `  UNNAMED0`.prop
        |""".stripMargin
    )
  }

  test("Rewrites subquery expression in DELETE") {
    assertRewritten(
      "DELETE (COLLECT { MATCH (a) RETURN a }[0])",
      """WITH COLLECT { MATCH (a) RETURN a }[0] AS `  UNNAMED0`
        |DELETE `  UNNAMED0`
        |""".stripMargin
    )
  }

  test("Does not rewrite subquery expression in SET") {
    assertNotRewritten(
      "SET (COLLECT { MATCH (a) RETURN a }[0]).prop = 5"
    )
  }

  test("Does not rewrite subquery expression in FOREACH") {
    assertNotRewritten("FOREACH(y IN [1] | REMOVE (COLLECT { MATCH (a) RETURN a }[0]).prop )")
  }

  test("Rewrites case expression") {
    assertRewritten(
      """
        |MATCH (a)
        |REMOVE (CASE WHEN true THEN a ELSE null END).prop
        |""".stripMargin,
      """MATCH (a)
        |WITH a AS a, CASE WHEN true THEN a ELSE null END AS `  UNNAMED0`
        |REMOVE `  UNNAMED0`.prop
        |""".stripMargin
    )
  }

  test("Rewrites multiple subquery expressions") {
    assertRewritten(
      """
        |DELETE (COLLECT { MATCH (a) RETURN a }[0]),
        |       (COLLECT { MATCH (a) RETURN a }[1])
        |""".stripMargin,
      """WITH COLLECT { MATCH (a) RETURN a }[0] AS `  UNNAMED0`,
        |     COLLECT { MATCH (a) RETURN a }[1] AS `  UNNAMED1`
        |DELETE `  UNNAMED0`, `  UNNAMED1` 
        |""".stripMargin
    )
  }

  test("Inserts sort-of-empty importing WITH if the rewritten updating clause is the first clause in a subquery") {
    assertRewritten(
      """CALL {
        |  CREATE (a {p: COUNT { MATCH (b) }})
        |}""".stripMargin,
      // WITH COUNT { MATCH (b) } AS `  UNNAMED0`
      // cannot be the first WITH inside CALL - it does not qualify as an importing WITH.
      // Since the original query did not have any importing WITH, we would want to place an empty
      // importing WITH in the beginning. Even if we have AST to represent an empty WITH, it would not render
      // as parseable Cypher and thus not work in Composite.
      // Therefore, we introduce a useless UNWIND, so that the following WITH is not seen as an importing WITH.
      """CALL {
        |  UNWIND [false] AS `  UNNAMED1` // <- useless UNWIND here
        |  WITH `  UNNAMED1` AS `  UNNAMED1`, COUNT { MATCH (b) } AS `  UNNAMED0`
        |  CREATE (a {p: `  UNNAMED0`})
        |}""".stripMargin
    )
  }

  test("Does not insert empty importing WITH if the rewritten updating clause is the second clause in a subquery") {
    assertRewritten(
      """CALL {
        |  MATCH (foo)
        |  CREATE (a {p: COUNT { MATCH (b) }})
        |}""".stripMargin,
      """CALL {
        |  MATCH (foo)
        |  WITH foo, COUNT { MATCH (b) } AS `  UNNAMED0`
        |  CREATE (a {p: `  UNNAMED0`})
        |}""".stripMargin
    )
  }

}
