/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.semantics.SemanticChecker
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.frontend.phases.rewriting.cnf.flattenBooleanOperators
import org.neo4j.cypher.internal.rewriting.rewriters.LabelExpressionPredicateNormalizer
import org.neo4j.cypher.internal.rewriting.rewriters.QuantifiedPathPatternNodeInsertRewriter
import org.neo4j.cypher.internal.rewriting.rewriters.nameAllPatternElements
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeHasLabelsAndHasType
import org.neo4j.cypher.internal.rewriting.rewriters.normalizePredicates
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.inSequence
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.TestName

class EmptyRelationshipListEndpointProjectionTest extends CypherFunSuite with PlannerQueryRewriterTest
    with TestName {

  override def rewriter(anonymousVariableNameGenerator: AnonymousVariableNameGenerator): Rewriter = {
    val state = mock[LogicalPlanState]
    when(state.anonymousVariableNameGenerator).thenReturn(anonymousVariableNameGenerator)
    val plannerContext = mock[PlannerContext]
    when(plannerContext.cancellationChecker).thenReturn(CancellationChecker.NeverCancelled)
    EmptyRelationshipListEndpointProjection.instance(state, plannerContext)
  }

  override def rewriteAST(
    astOriginal: Statement,
    ceF: CypherExceptionFactory,
    anonVarGen: AnonymousVariableNameGenerator
  ): Statement = {
    val orgAstState = SemanticChecker.check(astOriginal).state
    astOriginal.endoRewrite(inSequence(
      LabelExpressionPredicateNormalizer.instance,
      normalizeHasLabelsAndHasType(orgAstState),
      QuantifiedPathPatternNodeInsertRewriter.instance,
      nameAllPatternElements.getRewriter(orgAstState, Map.empty, ceF, anonVarGen, CancellationChecker.neverCancelled()),
      normalizePredicates.getRewriter(orgAstState, Map.empty, ceF, anonVarGen, CancellationChecker.neverCancelled()),
      flattenBooleanOperators.instance(CancellationChecker.NeverCancelled)
    ))
  }

  test("MATCH (a)-[r*]->(b) RETURN r AS r") {
    // Not repeated
    assertIsNotRewritten(testName)
  }

  // ================================================
  // =============Same query graph===================
  // ================================================

  test("MATCH (a)-[r*0..5]->(b) MATCH (a)-[r*0..5]-(c) RETURN count(*) AS c") {
    // start in scope
    assertIsNotRewritten(testName)
  }

  test("MATCH (a)-[r*0..5]->(b) MATCH (c)-[r*0..5]-(b) RETURN count(*) AS c") {
    // end in scope
    assertIsNotRewritten(testName)
  }

  test("MATCH (a)-[r*0..5]->(b) MATCH (a)-[r*0..5]-(b) RETURN count(*) AS c") {
    // both in scope
    assertIsNotRewritten(testName)
  }

  test("MATCH (a)-[r*1..5]->(b) MATCH (c)-[r*1..5]-(d) RETURN count(*) AS c") {
    // not zero-length
    assertIsNotRewritten(testName)
  }

  test("MATCH (a)-[r]->{1,5}(b) MATCH (c)-[r*1..5]->(d) RETURN count(*) AS c") {
    // not zero-length
    assertRewrite(
      testName,
      "MATCH (a)-[r]->{1,5}(b) MATCH (c)-[anon_2*1..5]->(d) WHERE r = anon_2 RETURN count(*) AS c"
    )
  }

  test("MATCH (a)-[r]->*(b) MATCH (a)-[r*0..5]->(c) RETURN count(*) AS c") {
    // start in scope
    assertRewrite(
      testName,
      "MATCH (a)-[r]->*(b) MATCH (a)-[anon_2*0..5]->(c) WHERE r = anon_2 RETURN count(*) AS c"
    )
  }

  test("MATCH (a)-[r]->*(b) MATCH (c)-[r*0..5]->(b) RETURN count(*) AS c") {
    // end in scope
    assertRewrite(
      testName,
      "MATCH (a)-[r]->*(b) MATCH (c)-[anon_2*0..5]->(b) WHERE r = anon_2 RETURN count(*) AS c"
    )
  }

  test("MATCH (a)-[r]->*(b) MATCH (a)-[r*0..5]->(b) RETURN count(*) AS c") {
    // both in scope
    assertRewrite(
      testName,
      "MATCH (a)-[r]->*(b) MATCH (a)-[anon_2*0..5]->(b) WHERE r = anon_2 RETURN count(*) AS c"
    )
  }

  test("MATCH (a)-[r*0..5]->(b) MATCH (c)-[r*0..5]->(d) RETURN count(*) AS c") {
    assertRewriteMultiple(
      testName,
      "MATCH (a)-[anon_0*0..5]->(b) MATCH (c)-[r*0..5]->(d) WHERE r = anon_0 RETURN count(*) AS c",
      "MATCH (a)-[r*0..5]->(b) MATCH (c)-[anon_0*0..5]->(d) WHERE r = anon_0 RETURN count(*) AS c"
    )
  }

  test("MATCH (a)-[r*0..5]->(b) MATCH (c)-[r*0..5]-(d) RETURN count(*) AS c") {
    assertRewriteMultiple(
      testName,
      "MATCH (a)-[anon_0*0..5]->(b) MATCH (c)-[r*0..5]-(d) WHERE r = anon_0 RETURN count(*) AS c",
      "MATCH (a)-[r*0..5]->(b) MATCH (c)-[anon_0*0..5]-(d) WHERE r = anon_0 RETURN count(*) AS c"
    )
  }

  test("MATCH (a)-[r]->*(b) MATCH (c)-[r*0..5]->(d) RETURN count(*) AS c") {
    assertRewrite(
      testName,
      "MATCH (a)-[r]->*(b) MATCH (c)-[anon_2*0..5]->(d) WHERE r = anon_2 RETURN count(*) AS c"
    )
  }

  test("MATCH (a) (()-[r]-()--())* (b) MATCH (c)-[r*0..5]->(d) RETURN count(*) AS c") {
    assertRewrite(
      testName,
      "MATCH (a) (()-[r]-()--())* (b) MATCH (c)-[anon_4*0..5]->(d) WHERE r = anon_4 RETURN count(*) AS c"
    )
  }

  test("MATCH (a)-[r*0..5]->(b) MATCH (c)-[r*0..5]-(d) MATCH (e)-[r*0..5]-(f) RETURN count(*) AS c") {
    assertRewrite(
      testName,
      """
        |MATCH (a)-[anon_0*0..5]->(b)
        |MATCH (c)-[anon_1*0..5]-(d)
        |MATCH (e)-[r*0..5]-(f)
        |WHERE r = anon_0 AND r = anon_1
        |RETURN count(*) AS c""".stripMargin
    )
  }

  test("MATCH (a)-[r*0..5]-(b) MATCH (c)-[r*0..5]-(d) RETURN count(*) AS c") {
    assertRewriteMultiple(
      testName,
      "MATCH (a)-[r*0..5]-(b) MATCH (c)-[anon_0*0..5]-(d) WHERE r = anon_0 RETURN count(*) AS c",
      "MATCH (a)-[anon_0*0..5]-(b) MATCH (c)-[r*0..5]-(d) WHERE r = anon_0 RETURN count(*) AS c"
    )
  }

  // ================================================
  // ==========Different query graphs================
  // ================================================

  test("MATCH (a)-[r*0..5]->(b) WITH r AS r, a AS a SKIP 0 MATCH (a)-[r*0..5]-(c) RETURN count(*) AS c") {
    // start in scope
    assertIsNotRewritten(testName)
  }

  test("MATCH (a)-[r*0..5]->(b) WITH r AS r, b AS b SKIP 0 MATCH (c)-[r*0..5]-(b) RETURN count(*) AS c") {
    // end in scope
    assertIsNotRewritten(testName)
  }

  test("MATCH (a)-[r*0..5]->(b) WITH r AS r, a AS a SKIP 0 MATCH (a)-[r*0..5]-(b) RETURN count(*) AS c") {
    // both in scope
    assertIsNotRewritten(testName)
  }

  test("MATCH (a)-[r*1..5]->(b) WITH r AS r SKIP 0 MATCH (c)-[r*1..5]-(d) RETURN count(*) AS c") {
    // not zero-length
    assertIsNotRewritten(testName)
  }

  test("MATCH (a)-[r]->*(b) WITH r AS r, a AS a SKIP 0 MATCH (a)-[r*0..5]-(c) RETURN count(*) AS c") {
    // start in scope
    assertIsNotRewritten(testName)
  }

  test("MATCH (a)-[r]->*(b) WITH r AS r, b AS b SKIP 0 MATCH (c)-[r*0..5]-(b) RETURN count(*) AS c") {
    // end in scope
    assertIsNotRewritten(testName)
  }

  test("MATCH (a)-[r]->*(b) WITH r AS r, a AS a SKIP 0 MATCH (a)-[r*0..5]-(b) RETURN count(*) AS c") {
    // both in scope
    assertIsNotRewritten(testName)
  }

  test("MATCH (a)-[r]->+(b) WITH r AS r SKIP 0 MATCH (c)-[r*1..5]-(d) RETURN count(*) AS c") {
    // not zero-length
    assertIsNotRewritten(testName)
  }

  test("MATCH (a)-[r*0..5]->(b) WITH r AS r SKIP 0 MATCH (c)-[r*0..5]->(d) RETURN count(*) AS c") {
    assertRewrite(
      testName,
      "MATCH (a)-[r*0..5]->(b) WITH r AS r SKIP 0 MATCH (c)-[anon_0*0..5]->(d) WHERE r = anon_0 RETURN count(*) AS c"
    )
  }

  test("MATCH (a)-[r]->*(b) WITH r AS r SKIP 0 MATCH (c)-[r*0..5]->(d) RETURN count(*) AS c") {
    assertRewrite(
      testName,
      "MATCH (a)-[r]->*(b) WITH r AS r SKIP 0 MATCH (c)-[anon_2*0..5]->(d) WHERE r = anon_2 RETURN count(*) AS c"
    )
  }

  test("MATCH (a)-[r*0..5]->(b) WITH r AS r SKIP 0 MATCH (c)-[r*0..5]-(d) RETURN count(*) AS c") {
    assertRewrite(
      testName,
      "MATCH (a)-[r*0..5]->(b) WITH r AS r SKIP 0 MATCH (c)-[anon_0*0..5]-(d) WHERE r = anon_0 RETURN count(*) AS c"
    )
  }

  test(
    "MATCH (a)-[r*0..5]->(b) WITH r AS r SKIP 0 MATCH (c)-[r*0..5]-(d) WITH r AS r SKIP 0 MATCH (e)-[r*0..5]-(f) RETURN count(*) AS c"
  ) {
    assertRewrite(
      testName,
      """
        |MATCH (a)-[r*0..5]->(b)
        |WITH r AS r SKIP 0
        |MATCH (c)-[anon_0*0..5]-(d)
        |WHERE r = anon_0
        |WITH r AS r SKIP 0
        |MATCH (e)-[anon_1*0..5]-(f)
        |WHERE r = anon_1
        |RETURN count(*) AS c""".stripMargin
    )
  }
}
