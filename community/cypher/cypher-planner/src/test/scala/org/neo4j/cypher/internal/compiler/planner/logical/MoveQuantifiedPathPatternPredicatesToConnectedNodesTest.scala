/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
import org.neo4j.cypher.internal.rewriting.rewriters.computeDependenciesForExpressions
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

class MoveQuantifiedPathPatternPredicatesToConnectedNodesTest extends CypherFunSuite with PlannerQueryRewriterTest
    with TestName {

  override def rewriter(anonymousVariableNameGenerator: AnonymousVariableNameGenerator): Rewriter = {
    val state = mock[LogicalPlanState]
    when(state.anonymousVariableNameGenerator).thenReturn(anonymousVariableNameGenerator)
    val plannerContext = mock[PlannerContext]
    when(plannerContext.cancellationChecker).thenReturn(CancellationChecker.NeverCancelled)
    MoveQuantifiedPathPatternPredicatesToConnectedNodes.instance(state, plannerContext)
  }

  override def rewriteAST(
    astOriginal: Statement,
    ceF: CypherExceptionFactory,
    anonVarGen: AnonymousVariableNameGenerator
  ): Statement = {
    val orgAstState = SemanticChecker.check(astOriginal).state
    astOriginal.endoRewrite(inSequence(
      computeDependenciesForExpressions(orgAstState),
      LabelExpressionPredicateNormalizer.instance,
      normalizeHasLabelsAndHasType(orgAstState),
      QuantifiedPathPatternNodeInsertRewriter.instance,
      nameAllPatternElements.getRewriter(orgAstState, Map.empty, ceF, anonVarGen),
      normalizePredicates.getRewriter(orgAstState, Map.empty, ceF, anonVarGen),
      flattenBooleanOperators
    ))
  }

  test("MATCH () ((a)-->(b))+ () RETURN count(*) AS c") {
    assertIsNotRewritten(testName)
  }

  test("MATCH ((a:A)-->(b:B))+ RETURN count(*) AS c") {
    assertRewrite(testName, "MATCH (:A) ((a:A)-->(b:B))+ (:B) RETURN count(*) AS c")
  }

  test("MATCH (x) ((a)-->(b {foo: 'bar'}) WHERE a.prop > 5)+ RETURN count(*) AS c") {
    assertRewrite(
      testName,
      """MATCH (x) ((a)-->(b  {foo: 'bar'}) WHERE a.prop > 5)+ ({foo: 'bar'})
        |WHERE x.prop > 5
        |RETURN count(*) AS c""".stripMargin
    )
  }

  test("MATCH (x) ((a)-->(b) WHERE a.prop > b.prop)+ (y) RETURN count(*) AS c") {
    assertIsNotRewritten(testName)
  }

  test("MATCH (x) ((a)-->(b)-->(c) WHERE a.prop > c.prop)+ (y) RETURN count(*) AS c") {
    assertIsNotRewritten(testName)
  }

  test("MATCH (x) ((a)-->(b)-->(c) WHERE a.prop > b.prop)+ (y) RETURN count(*) AS c") {
    assertIsNotRewritten(testName)
  }

  test("MATCH (pre) MATCH (x) ((a)-->(b) WHERE a.prop > pre.prop)+ (y) RETURN count(*) AS c") {
    assertRewrite(
      testName,
      """MATCH (pre)
        |MATCH (x) ((a)-->(b) WHERE a.prop > pre.prop)+ (y)
        |WHERE x.prop > pre.prop
        |RETURN count(*) AS c""".stripMargin
    )
  }

  test("MATCH (x) ((a)-->(b) WHERE EXISTS { (a)-->(c:C) })+ RETURN count(*) AS c") {
    assertIsNotRewritten(testName)
  }

  test("MATCH (x) ((a)-->(b) WHERE EXISTS { (a)-->(c) WHERE any(x IN c.list WHERE x > 0) })+ RETURN count(*) AS c") {
    assertIsNotRewritten(testName)
  }

  test("MATCH (x) ((a)-->(b) WHERE any(x IN a.list WHERE x > 0))+ RETURN count(*) AS c") {
    // In production, x would be disambiguated at this point.
    assertRewrite(
      testName,
      """MATCH (x) ((a)-->(b) WHERE any(x IN a.list WHERE x > 0))+
        |WHERE any(x IN x.list WHERE x > 0)
        |RETURN count(*) AS c""".stripMargin
    )
  }
}
