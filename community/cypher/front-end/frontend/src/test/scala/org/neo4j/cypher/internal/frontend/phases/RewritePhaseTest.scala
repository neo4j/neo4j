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
import org.neo4j.cypher.internal.CypherVersionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.frontend.PlannerName
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.Parse
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.PreparatoryRewriting
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.SemanticAnalysis
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.ScopeSurveyor
import org.neo4j.cypher.internal.rewriting.rewriters.factories.PreparatoryRewritingRewriterFactory
import org.neo4j.cypher.internal.rewriting.rewriters.preparatoryRewriters.NormalizeWithAndReturnClauses
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.inSequence
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.kernel.database.DatabaseReference
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.database.NormalizedCatalogEntry
import org.neo4j.kernel.database.NormalizedDatabaseName

import java.util.Optional
import java.util.UUID

case class PhaseTestConfig(
  excludedVersions: Set[CypherVersion] = Set.empty,
  checkSemantics: Boolean = true,
  semanticFeatures: Seq[SemanticFeature] = Seq.empty
)

trait RewritePhaseTest extends CypherVersionTestSupport {
  self: CypherFunSuite with AstConstructionTestSupport =>

  val prettifier: Prettifier = Prettifier(
    ExpressionStringifier((e: Expression) => e.asCanonicalStringVal)
  )

  private val plannerName = new PlannerName {
    override def name: String = "fake"
    override def toTextOutput: String = "fake"
    override def version: String = "fake"
  }

  def databaseReference: DatabaseReference = new DatabaseReference {
    override def alias(): NormalizedDatabaseName = ???
    override def namespace(): Optional[NormalizedDatabaseName] = ???
    override def isPrimary: Boolean = ???
    override def id(): UUID = ???
    override def namedDatabaseId(): NamedDatabaseId = ???
    override def toPrettyString: String = ???
    override def fullName(): NormalizedDatabaseName =
      new NormalizedDatabaseName(NormalizedDatabaseName.normalize("sessionDb"))
    override def isComposite: Boolean = true
    override def compareTo(o: DatabaseReference): Int = ???
    override def owningDatabaseName: String = ???
    override def catalogEntry(): NormalizedCatalogEntry = ???
    override def isShard: Boolean = false
  }

  def getExceptionFactory(query: String) = Neo4jCypherExceptionFactory(query, None)

  /**
   * To be able to just read Cypher when looking at a failure in a `RewritePhaseTest`,
   */
  case class StatementPrettifier(statement: Statement) {

    override def toString: String = {
      prettifier.asString(statement)
    }
  }

  val phaseTestConfig: PhaseTestConfig = PhaseTestConfig()

  def rewriterPhaseUnderTest: Transformer[BaseContext, BaseState, BaseState]

  // Override this to add additional rewrites needed for phases test.
  // PreProcessRewriters are applied to both expected and actual.
  def preProcessRewriters: Seq[PreparatoryRewritingRewriterFactory] = Seq(NormalizeWithAndReturnClauses)

  def preProcessRewriterSequence: Transformer[BaseContext, BaseState, BaseState] =
    new Transformer[BaseContext, BaseState, BaseState] {
      override def transform(from: BaseState, context: BaseContext): BaseState = {
        val rewriters =
          preProcessRewriters.map { _.getRewriter(context.cypherExceptionFactory, Some(context.cypherVersion)) }
        val rewrittenStatement = from.statement().endoRewrite(inSequence(rewriters: _*))
        from.withStatement(rewrittenStatement)
      }

      override def name: String = "PreProcessRewriter"
      override def postConditions: Set[StepSequencer.Condition] = Set.empty
    }

  // Override this to add additional transformers needed for phases test.
  // PreProcessTransformer is applied to both expected and actual.
  def preProcessTransformer: Transformer[BaseContext, BaseState, BaseState] = NoOp()

  private def preparationSteps(skipParse: Boolean): Transformer[BaseContext, BaseState, BaseState] = {
    if (skipParse)
      preProcessRewriterSequence andThen preProcessTransformer
    else
      Parse andThen preProcessRewriterSequence andThen preProcessTransformer
  }

  def rewriterPhaseForExpected: Transformer[BaseContext, BaseState, BaseState] = NoOp()

  def assertNotRewritten(
    from: String,
    excludedVersions: Set[CypherVersion] = phaseTestConfig.excludedVersions,
    invalidSemantics: Boolean = false
  ): Unit =
    assertRewritten(from, from, excludedVersions = excludedVersions, invalidSemantics = invalidSemantics)

  // additionalExpectedAstUpdates and additionalActualAstCleanup is for updating things that are changed in the rewriter but cannot be expressed in the query,
  // for example the AddedInRewriteGeneral flag on WITH
  def assertRewritten(
    from: String,
    to: String,
    additionalExpectedAstUpdates: Statement => Statement = identity,
    additionalActualAstCleanup: Statement => Statement = identity,
    semanticTableExpressions: List[Expression] = Nil,
    excludedVersions: Set[CypherVersion] = phaseTestConfig.excludedVersions,
    invalidSemantics: Boolean = false
  ): Unit =
    CypherVersion.values().filterNot(excludedVersions).foreach { version =>
      withClue(s"CYPHER $version\n")(
        assertRewrittenImpl(
          version,
          from,
          to,
          semanticTableExpressions,
          additionalExpectedAstUpdates,
          additionalActualAstCleanup,
          invalidSemantics
        )
      )
    }

  def assertRewrittenToStatement(
    from: String,
    to: Statement,
    semanticTableExpressions: List[Expression] = Nil,
    excludedVersions: Set[CypherVersion] = Set.empty
  ): Unit = CypherVersion.values().filterNot(excludedVersions).foreach { version =>
    assertRewrittenStmtImpl(version, from, to, semanticTableExpressions)
  }

  def rewriteAndAssert(
    q: String,
    verify: Statement => Unit,
    disabledVersions: Set[CypherVersion] = Set.empty
  ): Unit = {
    (CypherVersion.values().toSet -- disabledVersions).foreach { version =>
      withClue(s"CYPHER $version\n") {
        val state = prepareFrom(version, q, rewriterPhaseUnderTest)
        verify.apply(state.statement())
      }
    }
  }

  // additionalExpectedAstUpdates is for updating things that are changed in the rewriter but cannot be expressed in the query,
  // for example the AddedInRewriteGeneral flag on WITH
  // additionalActualAstCleanup is used to cleanup AST features that are impossible to
  // reproduce using parsing or simple rewriting of expected
  private def assertRewrittenImpl(
    version: CypherVersion,
    from: String,
    to: String,
    semanticTableExpressions: List[Expression],
    additionalExpectedAstUpdates: Statement => Statement,
    additionalActualAstCleanup: Statement => Statement,
    invalidSemantics: Boolean
  ): Unit = {

    val fromOutState = prepareFrom(version, from, rewriterPhaseUnderTest, invalidSemantics = invalidSemantics)
    val toOutState = prepareFrom(version, to, rewriterPhaseForExpected, invalidSemantics = invalidSemantics)

    val actualStatement = additionalActualAstCleanup(fromOutState.statement())
    val expectedStatement = additionalExpectedAstUpdates(toOutState.statement())

    compareStatements(expectedStatement, actualStatement, fromOutState, semanticTableExpressions)

  }

  def assertRewrittenStmtImpl(
    version: CypherVersion,
    from: String,
    expectedStatement: Statement,
    semanticTableExpressions: List[Expression],
    invalidSemantics: Boolean = false
  ): Unit = {
    val fromOutState = prepareFrom(version, from, rewriterPhaseUnderTest, invalidSemantics = invalidSemantics)

    val actualStatement = fromOutState.statement()

    compareStatements(expectedStatement, actualStatement, fromOutState, semanticTableExpressions)
  }

  protected def canonicalizeForComparison(stmt: Statement): Statement = stmt

  private def compareStatements(
    expectedStatement: Statement,
    actualStatement: Statement,
    state: BaseState,
    semanticTableExpressions: List[Expression]
  ): Unit = {

    StatementPrettifier(canonicalizeForComparison(actualStatement)) should equal(
      StatementPrettifier(canonicalizeForComparison(expectedStatement))
    )

    semanticTableExpressions.foreach { e =>
      state.semanticTable().types.keys.map(_.node) should contain(e)
    }
  }

  private def checkSemanticsTransformer: Transformer[BaseContext, BaseState, BaseState] =
    Parse andThen PreparatoryRewriting andThen SemanticAnalysis(Some(false)) andThen ScopeSurveyor

  def prepareFrom(
    version: CypherVersion,
    from: String,
    transformer: Transformer[BaseContext, BaseState, BaseState],
    statement: Option[Statement] = None,
    invalidSemantics: Boolean = false
  ): BaseState = {

    def initialState = InitialState(from, plannerName, new AnonymousVariableNameGenerator, maybeStatement = statement)

    if (!invalidSemantics && phaseTestConfig.checkSemantics) {
      val checkContext =
        ContextHelper.create(version, from, phaseTestConfig.semanticFeatures, databaseReference)
      checkSemanticsTransformer.transform(initialState, checkContext)
    }

    val testContext = ContextHelper.create(version, from, phaseTestConfig.semanticFeatures, databaseReference)

    val preparedState = preparationSteps(statement.isDefined).transform(initialState, testContext)

    preparedState.anonymousVariableNameGenerator.resetCounter()

    transformer.transform(preparedState, testContext)
  }
}
