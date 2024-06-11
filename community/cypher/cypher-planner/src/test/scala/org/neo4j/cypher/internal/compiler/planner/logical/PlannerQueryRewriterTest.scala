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

import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.semantics.SemanticChecker
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.SyntaxExceptionCreator
import org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery.StatementConverters
import org.neo4j.cypher.internal.compiler.helpers.WindowsSafeAnyRef
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.parser.v5.ast.factory.Cypher5AstParser
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.DummyPosition
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewritable.RewritableAny
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.helpers.NameDeduplicator.removeGeneratedNamesAndParamsOnTree
import org.neo4j.cypher.internal.util.helpers.fixedPoint
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

trait PlannerQueryRewriterTest {
  self: CypherFunSuite =>

  private def parse(query: String, exceptionFactory: CypherExceptionFactory): Statement = {
    new Cypher5AstParser(query, exceptionFactory, None).singleStatement()
  }

  def rewriter(anonymousVariableNameGenerator: AnonymousVariableNameGenerator): Rewriter

  def rewriteAST(
    astOriginal: Statement,
    cypherExceptionFactory: CypherExceptionFactory,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator
  ): Statement

  protected def assertRewrite(originalQuery: String, expectedQuery: String): Unit = {
    // We compare "solvedExpressionAsString" nested inside IRExpressions.
    // This saves us from windows line break mismatches in those strings.
    implicit val windowsSafe: WindowsSafeAnyRef[PlannerQuery] = new WindowsSafeAnyRef[PlannerQuery]

    val expectedGen = new AnonymousVariableNameGenerator()
    val actualGen = new AnonymousVariableNameGenerator()
    val expected =
      removeGeneratedNamesAndParamsOnTree(getTheWholePlannerQueryFrom(expectedQuery.stripMargin, expectedGen))
    val original = getTheWholePlannerQueryFrom(originalQuery.stripMargin, actualGen)

    val result = removeGeneratedNamesAndParamsOnTree(
      original.endoRewrite(fixedPoint(CancellationChecker.neverCancelled())(rewriter(actualGen)))
    )
    assert(
      result === expected,
      s"""$originalQuery
         |Was not rewritten correctly:
         |  Expected:
         |$expected
         |  But got:
         |$result""".stripMargin
    )
  }

  protected def assertRewriteMultiple(originalQuery: String, expectedQueries: String*): Unit = {
    val expected =
      expectedQueries.map { query =>
        val expectedGen = new AnonymousVariableNameGenerator()
        removeGeneratedNamesAndParamsOnTree(getTheWholePlannerQueryFrom(query.stripMargin, expectedGen))
      }
    val actualGen = new AnonymousVariableNameGenerator()
    val original = getTheWholePlannerQueryFrom(originalQuery.stripMargin, actualGen)

    val result = removeGeneratedNamesAndParamsOnTree(
      original.endoRewrite(fixedPoint(CancellationChecker.neverCancelled())(rewriter(actualGen)))
    )
    assert(
      expected.exists(_ === result),
      s"""$originalQuery
         |Was not rewritten correctly:
         |  Expected any of:
         |${expected.mkString("\n")}
         |  But got:
         |$result""".stripMargin
    )
  }

  protected def assertIsNotRewritten(query: String): Unit = {
    val actualGen = new AnonymousVariableNameGenerator()
    val plannerQuery = getTheWholePlannerQueryFrom(query.stripMargin, actualGen)
    val result = plannerQuery.endoRewrite(fixedPoint(CancellationChecker.neverCancelled())(rewriter(actualGen)))
    assert(result === plannerQuery, "\nShould not have been rewritten\n" + query)
  }

  private def getTheWholePlannerQueryFrom(
    query: String,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator
  ): PlannerQuery = {
    val exceptionFactory = Neo4jCypherExceptionFactory(query, Some(DummyPosition(0)))
    val astOriginal = parse(query.replace("\r\n", "\n"), exceptionFactory)
    val ast = rewriteAST(astOriginal, exceptionFactory, anonymousVariableNameGenerator)
    val onError = SyntaxExceptionCreator.throwOnError(exceptionFactory)
    val result = SemanticChecker.check(ast, SemanticState.clean)
    onError(result.errors)
    val table = SemanticTable(
      types = result.state.typeTable,
      recordedScopes = result.state.recordedScopes.view.mapValues(_.scope).toMap
    )
    StatementConverters.convertToPlannerQuery(
      ast.asInstanceOf[Query],
      table,
      anonymousVariableNameGenerator,
      CancellationChecker.NeverCancelled
    )
  }
}
