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
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.ExpandStarProjections
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.ComputeExpressionDependencies
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.ScopeSurveyor
import org.neo4j.cypher.internal.rewriting.conditions.ContainsNoStarProjections
import org.neo4j.cypher.internal.rewriting.conditions.NoReferenceEqualityAmongVariables
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ExpandStarProjectionsTest extends CypherFunSuite with RewritePhaseTest with AstConstructionTestSupport {

  override def rewriterPhaseUnderTest: Transformer[BaseContext, BaseState, BaseState] =
    ScopeSurveyor andThen ComputeExpressionDependencies andThen ExpandStarProjections

  override def checkSemanticsAfterRewrite: Boolean = true

  override val phaseTestConfig = PhaseTestConfig(
    excludedVersions = Set(CypherVersion.Cypher5),
    semanticFeatures = Seq(SemanticFeature.UseAsMultipleGraphsSelector)
  )

  test("expands RETURN * into the variables in scope") {
    assertRewritten(
      "MATCH (n)-[r]->(m) RETURN *",
      "MATCH (n)-[r]->(m) RETURN m AS m, n AS n, r AS r"
    )
  }

  test("expands RETURN * in alphabetical order") {
    assertRewritten(
      "WITH 2 AS b, 1 AS a RETURN *",
      "WITH 2 AS b, 1 AS a RETURN a AS a, b AS b"
    )
  }

  test("keeps explicit items after the expanded ones") {
    assertRewritten(
      "MATCH (n) RETURN *, n.p AS p",
      "MATCH (n) RETURN n AS n, n.p AS p"
    )
  }

  test("expands WITH * only to the variables the query goes on to use") {
    assertRewritten(
      "MATCH (n), (m) WITH *, 1 AS z RETURN n AS n, z AS z",
      "MATCH (n), (m) WITH n AS n, 1 AS z RETURN n AS n, z AS z"
    )
  }

  test("counts a use inside a grouping key of a later clause") {
    assertRewritten(
      "MATCH (n), (m) WITH *, 1 AS z RETURN count(*) AS c GROUP BY n.p",
      "MATCH (n), (m) WITH n AS n, 1 AS z RETURN count(*) AS c GROUP BY n.p"
    )
  }

  test("expands WITH * in Cypher 5 without asking whether the variables are used") {
    assertRewritten(
      "MATCH (n), (m) WITH *, 1 AS z RETURN n AS n, z AS z",
      "MATCH (n), (m) WITH m AS m, n AS n, 1 AS z RETURN n AS n, z AS z",
      excludedVersions = Set(CypherVersion.Cypher25)
    )
  }

  test("drops a WITH * that projects everything it was given") {
    assertRewritten(
      "MATCH (n) WITH * RETURN n AS n",
      "MATCH (n) RETURN n AS n"
    )
  }

  test("keeps a WITH * that filters") {
    assertRewritten(
      "MATCH (n) WITH * WHERE n.p > 1 RETURN n AS n",
      "MATCH (n) WITH n AS n WHERE n.p > 1 RETURN n AS n"
    )
  }

  test("expands the imports of a CALL (*) to what the subquery uses") {
    assertRewritten(
      "MATCH (n) CALL (*) { RETURN n.p AS x } RETURN n AS n, x AS x",
      "MATCH (n) CALL (n) { RETURN n.p AS x } RETURN n AS n, x AS x"
    )
  }

  test("imports nothing when a CALL (*) uses nothing") {
    assertRewritten(
      "MATCH (n) CALL (*) { RETURN 1 AS x } RETURN x AS x",
      "MATCH (n) CALL () { RETURN 1 AS x } RETURN x AS x"
    )
  }

  test("leaves a statement without stars alone") {
    assertNotRewritten("MATCH (n) RETURN n AS n")
  }

  test("produces no star projections and no shared variable instances") {
    Seq(
      "MATCH (n)-[r]->(m) RETURN *",
      "MATCH (n), (m) WITH * WHERE n.p > 1 RETURN n AS n",
      "MATCH (n) WITH * RETURN n AS n",
      "MATCH (n) CALL (*) { WITH * RETURN 1 AS x } RETURN *",
      "MATCH (n) WHERE EXISTS { MATCH (n)-->(o) WITH * RETURN o AS o } RETURN *",
      "UNWIND [1, 2] AS i CALL (*) { WITH * RETURN i + 1 AS j } RETURN *"
    ).foreach { query =>
      withClue(s"$query\n") {
        val statement = prepareFrom(CypherVersion.Cypher25, query, rewriterPhaseUnderTest).statement()
        ContainsNoStarProjections(statement)(CancellationChecker.NeverCancelled) shouldBe empty
        NoReferenceEqualityAmongVariables(statement)(CancellationChecker.NeverCancelled) shouldBe empty
      }
    }
  }
}
