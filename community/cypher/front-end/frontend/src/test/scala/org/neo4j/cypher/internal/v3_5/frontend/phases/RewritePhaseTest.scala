/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.v3_5.frontend.phases

import org.neo4j.cypher.internal.v3_5.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.v3_5.ast.Statement
import org.neo4j.cypher.internal.v3_5.ast.StatementHelper._
import org.neo4j.cypher.internal.v3_5.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.v3_5.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.v3_5.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.v3_5.ast.semantics.SyntaxExceptionCreator
import org.neo4j.cypher.internal.v3_5.expressions.Expression
import org.neo4j.cypher.internal.v3_5.frontend.PlannerName
import org.neo4j.cypher.internal.v3_5.parser.ParserFixture.parser
import org.neo4j.cypher.internal.v3_5.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.v3_5.rewriting.rewriters.Never
import org.neo4j.cypher.internal.v3_5.rewriting.rewriters.normalizeWithAndReturnClauses
import org.neo4j.cypher.internal.v3_5.util.inSequence
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

trait RewritePhaseTest {
  self: CypherFunSuite with AstConstructionTestSupport =>

  def rewriterPhaseUnderTest: Transformer[BaseContext, BaseState, BaseState]

  def rewriterPhaseForExpected: Transformer[BaseContext, BaseState, BaseState] =
    new Transformer[BaseContext, BaseState, BaseState] {
      override def transform(from: BaseState,
                             context: BaseContext): BaseState = from

      override def name: String = "do nothing"
    }

  val prettifier = Prettifier(ExpressionStringifier(_.asCanonicalStringVal))

  private val plannerName = new PlannerName {
    override def name: String = "fake"
    override def toTextOutput: String = "fake"
    override def version: String = "fake"
  }

  val astRewriter = new ASTRewriter(RewriterStepSequencer.newValidating, Never, getDegreeRewriting = true)

  def assertNotRewritten(from: String): Unit = assertRewritten(from, from)

  def assertRewritten(from: String, to: String): Unit = assertRewritten(from, to, List.empty)

  def assertRewritten(from: String, to: String, semanticTableExpressions: List[Expression], features: SemanticFeature*): Unit = {
    val fromAst = parseAndRewrite(from, features: _*)
    val fromInState = InitialState(from, None, plannerName, maybeStatement = Some(fromAst), maybeSemantics = Some(fromAst.semanticState(features: _*)))
    val fromOutState = rewriterPhaseUnderTest.transform(fromInState, ContextHelper.create())

    val toAst = parseAndRewrite(to, features: _*)
    val toInState = InitialState(to, None, plannerName, maybeStatement = Some(toAst), maybeSemantics = Some(toAst.semanticState(features: _*)))
    val toOutState = rewriterPhaseForExpected.transform(toInState, ContextHelper.create())

    fromOutState.statement() should equal(toOutState.statement())
    semanticTableExpressions.foreach { e =>
      fromOutState.semanticTable().types.keys should contain(e)
    }
  }

  def parseAndRewrite(queryText: String, features: SemanticFeature*): Statement = {
    val parsedAst = parser.parse(queryText)
    val mkException = new SyntaxExceptionCreator(queryText, Some(pos))
    val cleanedAst = parsedAst.endoRewrite(inSequence(normalizeWithAndReturnClauses(mkException)))
    val (rewrittenAst, _, _) = astRewriter.rewrite(queryText, cleanedAst, cleanedAst.semanticState(features: _*))
    rewrittenAst
  }
}
