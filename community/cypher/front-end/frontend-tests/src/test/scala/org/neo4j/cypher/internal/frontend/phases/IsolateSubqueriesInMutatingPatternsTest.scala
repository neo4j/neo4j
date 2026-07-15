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

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.AddedInRewriteGeneral
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.DefaultWith
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.ExpandClauses
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.IsolateSubqueriesInMutatingPatterns
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.SemanticAnalysis
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.ScopeSurveyor
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class IsolateSubqueriesInMutatingPatternsNoSemanticAnalysisTest extends CypherFunSuite with RewritePhaseTest
    with AstConstructionTestSupport {

  override def rewriterPhaseUnderTest: Transformer[BaseContext, BaseState, BaseState] =
    ScopeSurveyor andThen IsolateSubqueriesInMutatingPatterns

  test("Does not rewrite subquery expression in MERGE") {
    // Must run without SemanticAnalysis, because it is forbidden
    assertNotRewritten(
      "MERGE (a {p: COUNT { MATCH (b)  }})",
      invalidSemantics = true
    )
  }
}

class IsolateSubqueriesInMutatingPatternsTest extends CypherFunSuite with RewritePhaseTest
    with AstConstructionTestSupport {

  // Rewrite away WITH * in tests directly
  override def rewriterPhaseUnderTest: Transformer[BaseContext, BaseState, BaseState] =
    ScopeSurveyor andThen
      IsolateSubqueriesInMutatingPatterns andThen
      SemanticAnalysis(Some(false)) andThen
      ScopeSurveyor andThen
      ExpandClauses

  private val additionalExpectedAstUpdates = (expectedStatement: Statement) => {
    expectedStatement.endoRewrite(bottomUp(Rewriter.lift {
      // The original/rewritten statement will have AddedInRewriteGeneral,
      // the explicit WITH in the expected will have DefaultWith
      // so let's update that before checking the equality
      case w: With if w.withType == DefaultWith =>
        w.copy(withType = AddedInRewriteGeneral())(w.position)
    }))
  }

  test("Rewrites subquery expression in CREATE") {
    assertRewritten(
      "CREATE (a {p: COUNT { MATCH (b) }})",
      """WITH COUNT { MATCH (b) } AS `  UNNAMED0`
        |CREATE (a {p: `  UNNAMED0`})""".stripMargin,
      additionalExpectedAstUpdates = additionalExpectedAstUpdates
    )
  }

  test("Rewrites subquery expression in dynamic labels in CREATE") {
    assertRewritten(
      "CREATE (a:$(COLLECT { MATCH (b) RETURN b.name }))",
      """WITH COLLECT { MATCH (b) RETURN b.name } AS `  UNNAMED0`
        |CREATE (a:$all(`  UNNAMED0`))""".stripMargin,
      additionalExpectedAstUpdates = additionalExpectedAstUpdates
    )
  }

  test("Rewrites subquery expression in dynamic types in CREATE") {
    assertRewritten(
      "CREATE (a)-[b:$(COLLECT { MATCH (n) RETURN n.name })]->(c)",
      """WITH COLLECT { MATCH (n) RETURN n.name } AS `  UNNAMED0`
        |CREATE (a)-[b:$all(`  UNNAMED0`)]->(c)""".stripMargin,
      additionalExpectedAstUpdates = additionalExpectedAstUpdates
    )
  }

  test("Rewrites subquery expression for all dynamic labels/types in CREATE") {
    assertRewritten(
      "CREATE (a:$(COLLECT { MATCH (n) RETURN n.label }))-[b:$(COLLECT { MATCH (n) RETURN n.name })]->(c:$(toString(1)))",
      """WITH COLLECT { MATCH (n) RETURN n.label } AS `  UNNAMED0`, COLLECT { MATCH (n) RETURN n.name } AS `  UNNAMED1`
        |CREATE (a:$all(`  UNNAMED0`))-[b:$all(`  UNNAMED1`)]->(c:$all(toString(1)))""".stripMargin,
      additionalExpectedAstUpdates = additionalExpectedAstUpdates
    )
  }

  test("Rewrites subquery expression in CREATE that has a dependency on the previous clause") {
    assertRewritten(
      """MATCH (b)
        |CREATE (a {p: COUNT { MATCH (b) }})""".stripMargin,
      """MATCH (b)
        |WITH b, COUNT { MATCH (b) } AS `  UNNAMED0`
        |CREATE (a {p: `  UNNAMED0`})""".stripMargin,
      additionalExpectedAstUpdates = additionalExpectedAstUpdates
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
        |CREATE (a {p: `  UNNAMED0`})""".stripMargin,
      additionalExpectedAstUpdates = expectedStatement => {
        expectedStatement.endoRewrite(bottomUp(Rewriter.lift {
          // The original/rewritten statement will have AddedInRewriteGeneral on the extra WITH,
          // both explicit WITHs in the expected will have DefaultWith
          // so let's update the added WITH before checking the equality
          case w: With if w.returnItems.items.exists(r => r.name.equals("  UNNAMED0")) =>
            w.copy(withType = AddedInRewriteGeneral())(w.position)
        }))
      },
      excludedVersions = Set(CypherVersion.Cypher25)
    )

    assertRewritten(
      """MATCH (b)
        |WITH b
        |CREATE (c)
        |CREATE (a {p: COUNT { MATCH (b) }})""".stripMargin,
      """MATCH (b)
        |WITH b
        |CREATE (c)
        |WITH b, COUNT { MATCH (b) } AS `  UNNAMED0`
        |CREATE (a {p: `  UNNAMED0`})""".stripMargin,
      additionalExpectedAstUpdates = expectedStatement => {
        expectedStatement.endoRewrite(bottomUp(Rewriter.lift {
          // The original/rewritten statement will have AddedInRewriteGeneral on the extra WITH,
          // both explicit WITHs in the expected will have DefaultWith
          // so let's update the added WITH before checking the equality
          case w: With if w.returnItems.items.exists(r => r.name.equals("  UNNAMED0")) =>
            w.copy(withType = AddedInRewriteGeneral())(w.position)
        }))
      },
      excludedVersions = Set(CypherVersion.Cypher5)
    )
  }

  test("Rewrites subquery expression in CREATE that has a dependency on a previous clause with NEXT") {
    assertRewritten(
      """MATCH (b)
        |WITH b
        |CREATE (c)
        |CREATE (a {p: COUNT { MATCH (b) }})
        |RETURN a
        |
        |NEXT
        |
        |CREATE (x {p: COUNT { MATCH (a) }})
        |RETURN a""".stripMargin,
      """MATCH (b)
        |WITH b AS b
        |CREATE (c)
        |WITH b AS b, COUNT { MATCH (b) } AS `  UNNAMED0`
        |CREATE (a {p: `  UNNAMED0`})
        |WITH a AS a
        |WITH a AS a, COUNT { MATCH (a) } AS `  UNNAMED1`
        |CREATE (x {p: `  UNNAMED1`})
        |RETURN a AS a""".stripMargin,
      additionalActualAstCleanup = expectedStatement => {
        expectedStatement.endoRewrite(bottomUp(Rewriter.lift {
          case w: With => w.copy(withType = AddedInRewriteGeneral())(w.position)
        }))
      },
      additionalExpectedAstUpdates = expectedStatement => {
        expectedStatement.endoRewrite(bottomUp(Rewriter.lift {
          case w: With => w.copy(withType = AddedInRewriteGeneral())(w.position)
        }))
      },
      excludedVersions = Set(CypherVersion.Cypher5)
    )
  }

  test("Rewrites subquery expr in CREATE that has a dependency on a previous clause with NEXT forwarded variable") {
    assertRewritten(
      """MATCH (b)
        |WITH b
        |CREATE (c)
        |CREATE (a {p: COUNT { MATCH (b) }})
        |RETURN a, b
        |
        |NEXT
        |
        |CREATE (x {p: COUNT { MATCH (a) }})
        |RETURN a, b""".stripMargin,
      """MATCH (b)
        |WITH b AS b
        |CREATE (c)
        |WITH b AS b, COUNT { MATCH (b) } AS `  UNNAMED0`
        |CREATE (a {p: `  UNNAMED0`})
        |WITH a AS a, b AS b
        |WITH a AS a, b AS b, COUNT { MATCH (a) } AS `  UNNAMED1`
        |CREATE (x {p: `  UNNAMED1`})
        |RETURN a AS a, b AS b""".stripMargin,
      additionalActualAstCleanup = expectedStatement => {
        expectedStatement.endoRewrite(bottomUp(Rewriter.lift {
          case w: With => w.copy(withType = AddedInRewriteGeneral())(w.position)
        }))
      },
      additionalExpectedAstUpdates = expectedStatement => {
        expectedStatement.endoRewrite(bottomUp(Rewriter.lift {
          case w: With => w.copy(withType = AddedInRewriteGeneral())(w.position)
        }))
      },
      excludedVersions = Set(CypherVersion.Cypher5)
    )
  }

  test("Rewrites subquery expression in CREATE that has a dependency on a previous clause with NEXT in subquery") {
    assertRewritten(
      """CALL () {
        |MATCH (b)
        |WITH b
        |CREATE (c)
        |CREATE (a {p: COUNT { MATCH (b) }})
        |RETURN a
        |
        |NEXT
        |
        |CREATE (x {p: COUNT { MATCH (a) }})
        |}
        |FINISH""".stripMargin,
      """CALL () {
        |  MATCH (b)
        |  WITH b AS b
        |  CREATE (c)
        |  WITH b AS b, COUNT { MATCH (b) } AS `  UNNAMED0`
        |  CREATE (a {p: `  UNNAMED0`})
        |  WITH a AS a
        |  WITH a AS a, COUNT { MATCH (a) } AS `  UNNAMED1`
        |  CREATE (x {p: `  UNNAMED1`})
        |}
        |FINISH""".stripMargin,
      additionalActualAstCleanup = expectedStatement => {
        expectedStatement.endoRewrite(bottomUp(Rewriter.lift {
          case w: With => w.copy(withType = AddedInRewriteGeneral())(w.position)
        }))
      },
      additionalExpectedAstUpdates = expectedStatement => {
        expectedStatement.endoRewrite(bottomUp(Rewriter.lift {
          case w: With => w.copy(withType = AddedInRewriteGeneral())(w.position)
        }))
      },
      excludedVersions = Set(CypherVersion.Cypher5)
    )
  }

  test("Does not rewrite CREATE with cross-references") {
    // These are deprecated, but we cannot rewrite these and keep the same semantics.
    // The queries are going to be non-deterministic. The query is invalid in Cypher 25
    assertNotRewritten(
      "CREATE (a), (b {prop: EXISTS { (a)-[r2]->(c) }})",
      excludedVersions = Set(CypherVersion.Cypher25)
    )
  }

  test("Rewrites subquery expression in REMOVE") {
    assertRewritten(
      "REMOVE (COLLECT { MATCH (a) RETURN a }[0]).prop",
      """WITH COLLECT { MATCH (a) RETURN a }[0] AS `  UNNAMED0`
        |REMOVE `  UNNAMED0`.prop
        |""".stripMargin,
      additionalExpectedAstUpdates = additionalExpectedAstUpdates
    )
  }

  test("Rewrites subquery expression in DELETE") {
    assertRewritten(
      "DELETE (COLLECT { MATCH (a) RETURN a }[0])",
      """WITH COLLECT { MATCH (a) RETURN a }[0] AS `  UNNAMED0`
        |DELETE `  UNNAMED0`
        |""".stripMargin,
      additionalExpectedAstUpdates = additionalExpectedAstUpdates
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
        |""".stripMargin,
      additionalExpectedAstUpdates = additionalExpectedAstUpdates
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
        |""".stripMargin,
      additionalExpectedAstUpdates = additionalExpectedAstUpdates
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
        |}""".stripMargin,
      additionalExpectedAstUpdates = additionalExpectedAstUpdates,
      excludedVersions = Set(CypherVersion.Cypher25)
    )

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
        |  WITH COUNT { MATCH (b) } AS `  UNNAMED0`
        |  CREATE (a {p: `  UNNAMED0`})
        |}""".stripMargin,
      additionalExpectedAstUpdates = additionalExpectedAstUpdates,
      excludedVersions = Set(CypherVersion.Cypher5)
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
        |}""".stripMargin,
      additionalExpectedAstUpdates = additionalExpectedAstUpdates,
      excludedVersions = Set(CypherVersion.Cypher25)
    )

    assertRewritten(
      """CALL {
        |  MATCH (foo)
        |  CREATE (a {p: COUNT { MATCH (b) }})
        |}""".stripMargin,
      """CALL {
        |  MATCH (foo)
        |  WITH COUNT { MATCH (b) } AS `  UNNAMED0`
        |  CREATE (a {p: `  UNNAMED0`})
        |}""".stripMargin,
      additionalExpectedAstUpdates = additionalExpectedAstUpdates,
      excludedVersions = Set(CypherVersion.Cypher5)
    )
  }

  test("Do not insert additional WITH in scope clause subquery call") {
    assertRewritten(
      """CALL () {
        |  CREATE (a {p: COUNT { MATCH (b) }})
        |}""".stripMargin,
      """CALL () {
        |  WITH COUNT { MATCH (b) } AS `  UNNAMED0`
        |  CREATE (a {p: `  UNNAMED0`})
        |}""".stripMargin,
      additionalExpectedAstUpdates = additionalExpectedAstUpdates
    )
  }

}
