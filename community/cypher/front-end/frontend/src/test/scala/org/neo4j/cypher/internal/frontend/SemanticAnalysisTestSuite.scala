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
package org.neo4j.cypher.internal.frontend

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.CypherVersionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.p
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticErrorDef
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.AutoExtractedParameter
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.frontend.SemanticAnalysisTestSuite.Pipeline
import org.neo4j.cypher.internal.frontend.SemanticAnalysisTestSuite.initialStateWithQuery
import org.neo4j.cypher.internal.frontend.helpers.ErrorCollectingContext
import org.neo4j.cypher.internal.frontend.helpers.NoPlannerName
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.AST_REWRITE
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.frontend.phases.OptionalTransformer
import org.neo4j.cypher.internal.frontend.phases.Phase
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.Parse
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.PreparatoryRewriting
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.SemanticAnalysis
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.SemanticTypeCheck
import org.neo4j.cypher.internal.notification.InternalNotification
import org.neo4j.cypher.internal.rewriting.rewriters.ProjectNamedPaths
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.ErrorMessageProvider
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.NotImplementedErrorMessageProvider
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.test_helpers.CaretPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.InputPositionFromCaret
import org.neo4j.cypher.internal.util.test_helpers.TestName
import org.neo4j.cypher.internal.util.test_helpers.TestNameWithCaretPosition
import org.neo4j.gqlstatus.ErrorGqlStatusObject
import org.scalatest.TryValues

import scala.util.Failure
import scala.util.Success
import scala.util.Try

case class SemanticAnalysisResult(context: ErrorCollectingContext, state: BaseState) {
  def errors: Seq[SemanticErrorDef] = context.errors
  def notifications: Set[InternalNotification] = state.semantics().notifications
  def errorMessages: Seq[String] = errors.map(_.msg)
  def semanticTable: SemanticTable = state.semanticTable()
}

object SemanticAnalysisTestSuite {
  type Pipeline = Transformer[BaseContext, BaseState, BaseState]

  def initialStateWithQuery(query: String): InitialState =
    InitialState(query, NoPlannerName, new AnonymousVariableNameGenerator)
}

trait SemanticAnalysisTestSuite extends CypherFunSuite with CypherVersionTestSupport with TryValues {
  // for debugging set to true; pipeline used in each test will be printed to sdtout
  private val printPipeline: Boolean = false

  private val defaultDatabaseName = "mock"

  def messageProvider: ErrorMessageProvider = NotImplementedErrorMessageProvider

  /**
   * Takes a queryWithCaret and extracts the caret's (^) position via `CaretPosition`.
   * This way, one can chain this with `hasErrorWithMarkedPosition`.
   */
  def runWithCarets(
    queryWithCaret: String,
    pipeline: Pipeline = semanticAnalysisTwice(),
    isComposite: Boolean = false,
    sessionDatabase: String = defaultDatabaseName,
    state: BaseState => BaseState = s => s,
    semanticFeatures: Seq[SemanticFeature] = Seq.empty,
    disabledVersions: Set[CypherVersion] = Set.empty
  ) = {
    val caretPosition = CaretPosition(queryWithCaret)
    val positions = caretPosition.positions.map {
      case InputPositionFromCaret.Simple(offset, line, column) =>
        InputPosition.Simple(offset, line, column)
      case InputPositionFromCaret.Range(offset, line, column, inputLength) =>
        InputPosition.Range(offset, line, column, inputLength)
    }
    run(
      caretPosition.cleanInput,
      Some(positions),
      pipeline,
      isComposite,
      sessionDatabase,
      state,
      semanticFeatures,
      disabledVersions
    )
  }

  def run(
    query: String,
    extractedPositions: Option[Seq[InputPosition]] = None,
    pipeline: Pipeline = semanticAnalysisTwice(),
    isComposite: Boolean = false,
    sessionDatabase: String = defaultDatabaseName,
    state: BaseState => BaseState = s => s,
    semanticFeatures: Seq[SemanticFeature] = Seq.empty,
    disabledVersions: Set[CypherVersion] = Set.empty
  ): AnalysisAssertions =
    analyse(
      query,
      extractedPositions,
      pipeline,
      isComposite,
      sessionDatabase,
      state,
      semanticFeatures,
      disabledVersions
    ).run

  def analyse(
    query: String,
    extractedPositions: Option[Seq[InputPosition]] = None,
    pipeline: Pipeline = semanticAnalysisTwice(),
    isComposite: Boolean = false,
    sessionDatabase: String = defaultDatabaseName,
    state: BaseState => BaseState = s => s,
    semanticFeatures: Seq[SemanticFeature] = Seq.empty,
    disabledVersions: Set[CypherVersion] = Set.empty
  ): Analyse =
    Analyse(
      query,
      extractedPositions,
      pipeline,
      isComposite,
      sessionDatabase,
      state,
      messageProvider,
      semanticFeatures,
      disabledVersions,
      printPipeline
    )

  // This test invokes SemanticAnalysis twice because that's what the production pipeline does
  def semanticAnalysisTwice(
    extraStepBefore: Option[Transformer[BaseContext, BaseState, BaseState]] = None,
    extraStepInBetween: Option[Transformer[BaseContext, BaseState, BaseState]] = None
  ): Pipeline = {
    PreparatoryRewriting andThen
      OptionalTransformer(extraStepBefore) andThen
      SemanticAnalysis(warn = Some(true)) andThen
      OptionalTransformer(extraStepInBetween) andThen
      SemanticAnalysis(warn = Some(false)) andThen
      SemanticTypeCheck
  }

  case class Analyse(
    query: String,
    extractedPositions: Option[Seq[InputPosition]],
    pipeline: Pipeline,
    isComposite: Boolean,
    sessionDatabase: String,
    stateTransform: BaseState => BaseState,
    messageProvider: ErrorMessageProvider,
    semanticFeatures: Seq[SemanticFeature],
    disabledVersions: Set[CypherVersion] = Set.empty,
    printPipeline: Boolean = false
  ) {
    def withPipeline(pipeline: Pipeline): Analyse = copy(pipeline = pipeline)

    def withParam(n: String, t: CypherType, e: Expression): Analyse =
      withParams(AutoExtractedParameter(n, t)(p(0, 0, 0)) -> e)
    def withParams(ps: (AutoExtractedParameter, Expression)*): Analyse = state(_.withParams(ps.toMap))
    def inComposite: Analyse = copy(isComposite = true)
    def withDb(name: String): Analyse = copy(sessionDatabase = name)
    def state(f: BaseState => BaseState): Analyse = copy(stateTransform = stateTransform.andThen(f))

    def run: AnalysisAssertions = {
      require(!pipeline.name.contains("Parse"))
      if (printPipeline) {
        println("------")
        pipeline.toStrings.foreach(println)
        println("------")
      }
      val results = CypherVersion.values().view
        .collect {
          case v if !disabledVersions.contains(v) =>
            v -> Try {
              val context = new ErrorCollectingContext(v, isComposite, sessionDatabase, query, semanticFeatures) {
                override def errorMessageProvider: ErrorMessageProvider = messageProvider
              }
              val state = (Parse andThen pipeline).transform(stateTransform(initialStateWithQuery(query)), context)
              SemanticAnalysisResult(context, state)
            }
        }
        .toMap
      AnalysisAssertions(this, results)
    }
  }

  case class AnalysisAssertions(
    private val analyse: Analyse,
    results: Map[CypherVersion, Try[SemanticAnalysisResult]]
  ) {
    private type Self = AnalysisAssertions
    private type Pos = InputPosition
    private type GqlError = ErrorGqlStatusObject
    private type Notification = InternalNotification

    private val versions: Seq[CypherVersion] = (CypherVersion.values().toSet -- analyse.disabledVersions).toSeq

    def hasNoErrors: Self = hasErrors()

    def hasErrorsWithMarkedPositionMapped(expected: IndexedSeq[InputPosition] => Seq[SemanticErrorDef]): Self =
      hasErrors(expected(analyse.extractedPositions.get.toIndexedSeq): _*)

    // this method should not be overloaded for convenience of usage
    def hasErrorsWithMarkedPosition(expected: (InputPosition => SemanticErrorDef)*): Self = {
      val extractedPositions = analyse.extractedPositions.getOrElse(Seq.empty)
      if (extractedPositions.size != expected.size)
        new IllegalArgumentException(
          s"Not the same number of extracted positions as error constructor function: ${extractedPositions.size} vs. ${expected.size}"
        )
      val expectedErrors = extractedPositions.zip(expected).map {
        case (pos, ex) => ex(pos)
      }
      hasErrors(expectedErrors: _*)
    }

    def hasErrors(expected: SemanticErrorDef*): Self =
      assert { result =>
        (result.errors, expected) match {
          case (Seq(error), Seq(expected)) =>
            withClue(s"""position: ${error.position.verboseString}
                        |expected position: ${expected.position.verboseString}
                        |""".stripMargin) {
              error shouldEqual expected
            }
          case (actual, expected) =>
            actual should contain theSameElementsAs expected
        }
      }

    def hasAtLeastErrorsWithMarkedPositionMapped(expected: IndexedSeq[InputPosition] => Seq[SemanticErrorDef]): Self =
      hasAtLeastErrors(expected(analyse.extractedPositions.get.toIndexedSeq): _*)

    // this method should not be overloaded for convenience of usage
    def hasAtLeastErrorsWithMarkedPosition(expected: (InputPosition => SemanticErrorDef)*): Self = {
      val extractedPositions = analyse.extractedPositions.getOrElse(Seq.empty)
      if (extractedPositions.size != expected.size)
        new IllegalArgumentException(
          s"Not the same number of extracted positions as error constructor function: ${extractedPositions.size} vs. ${expected.size}"
        )
      val expectedErrors = extractedPositions.zip(expected).map {
        case (pos, ex) => ex(pos)
      }
      hasAtLeastErrors(expectedErrors: _*)
    }

    def hasAtLeastErrors(expected: SemanticErrorDef*): Self =
      assert { result =>
        (result.errors, expected) match {
          case (Seq(error), Seq(expected)) =>
            withClue(s"""position: ${error.position.verboseString}
                        |expected position: ${expected.position.verboseString}
                        |""".stripMargin) {
              error shouldEqual expected
            }
          case (actual, expected) =>
            actual should contain allElementsOf expected
        }
      }

    def hasAllErrorsIn(expected: SemanticErrorDef*): Self =
      assert { result =>
        (result.errors, expected) match {
          case (Seq(error), Seq(expected)) =>
            withClue(s"""position: ${error.position.verboseString}
                        |expected position: ${expected.position.verboseString}
                        |""".stripMargin) {
              error shouldEqual expected
            }
          case (actual, expected) =>
            actual should contain allElementsOf expected
        }
      }

    def hasErrors(
      gql1: GqlError,
      msg1: String,
      p1: Pos,
      gql2: GqlError,
      msg2: String,
      p2: Pos,
      gql3: GqlError,
      msg3: String,
      p3: Pos,
      gql4: GqlError,
      msg4: String,
      p4: Pos
    ): Self = hasErrors(
      SemanticError(gql1, msg1, p1),
      SemanticError(gql2, msg2, p2),
      SemanticError(gql3, msg3, p3),
      SemanticError(gql4, msg4, p4)
    )

    def hasErrors(
      gql1: GqlError,
      msg1: String,
      p1: Pos,
      gql2: GqlError,
      msg2: String,
      p2: Pos,
      gql3: GqlError,
      msg3: String,
      p3: Pos
    ): Self = hasErrors(SemanticError(gql1, msg1, p1), SemanticError(gql2, msg2, p2), SemanticError(gql3, msg3, p3))

    def hasErrors(gql1: GqlError, msg1: String, p1: Pos, gql2: GqlError, msg2: String, p2: Pos): Self =
      hasErrors(SemanticError(gql1, msg1, p1), SemanticError(gql2, msg2, p2))

    def hasErrorWithMarkedPosition(
      gql1: InputPosition => GqlError,
      msg1: String,
      gql2: InputPosition => GqlError,
      msg2: String
    ): Self = {
      val pos1 = analyse.extractedPositions.get(0)
      val pos2 = analyse.extractedPositions.get(1)
      hasErrors(SemanticError(gql1(pos1), msg1, pos1), SemanticError(gql2(pos2), msg2, pos2))
    }

    def hasError(gql: GqlError, msg: String, pos: Pos): Self = hasErrors(SemanticError(gql, msg, pos))

    def hasErrorWithMarkedPosition(gql: InputPosition => GqlError, msg: String): Self = {
      val pos =
        analyse.extractedPositions.getOrElse(throw new IllegalStateException("No extracted positions found.")).head
      hasErrors(SemanticError(gql(pos), msg, pos))
    }

    def hasAtLeastError(gql: GqlError, msg: String, pos: Pos): Self = hasAtLeastErrors(SemanticError(gql, msg, pos))

    def hasAtLeastErrorWithMarkedPosition(gql: InputPosition => GqlError, msg: String): Self = {
      val pos =
        analyse.extractedPositions.getOrElse(throw new IllegalStateException("No extracted positions found.")).head
      hasAtLeastErrors(SemanticError(gql(pos), msg, pos))
    }

    def hasErrorMessages(expected: String*): Self =
      assert(_.errorMessages.distinct.map(normalizeNewLines) should contain theSameElementsAs expected)

    def hasAtLeastOneErrorMessageIn(expected: String*): Self =
      assert(_.errorMessages.distinct.map(normalizeNewLines) should contain atLeastOneElementOf expected)

    def hasNotifications(expected: Notification*): Self =
      assert(_.notifications should contain theSameElementsAs expected)

    def hasNotificationsWithMarkedPosition(expected: (InputPosition => Notification)*): Self = {
      val expectedWithPositions = expected.zipWithIndex.map {
        case (exp, i) =>
          val pos = analyse.extractedPositions.get.apply(i)
          exp(pos)
      }
      hasNotifications(expectedWithPositions: _*)
    }

    def hasNoNotifications: Any = hasNotifications()

    def hasNotificationsIn(f: CypherVersion => Seq[Notification]): Self =
      assertIn(v => r => r.notifications should contain theSameElementsAs f(v))

    def hasSemanticErrorsIn(f: CypherVersion => Seq[SemanticError]): Self =
      assertIn(v => r => r.errors should contain theSameElementsAs f(v))

    def hasErrorMessagesIn(f: CypherVersion => Seq[String]): Self =
      assertIn(v => r => r.errorMessages should contain theSameElementsAs f(v))

    def hasAllErrorMessagesIn(f: CypherVersion => Seq[String]): Self =
      assertIn(v => r => r.errorMessages should contain allElementsOf f(v))

    def hasGQLErrorsIn(f: CypherVersion => Seq[(GqlError, String, Pos)]): Self =
      assertIn(v => r => r.errors should contain theSameElementsAs f(v).map(toSemErr))

    def hasAllGQLErrorsIn(f: CypherVersion => Seq[(GqlError, String, Pos)]): Self =
      assertIn(v => r => r.errors should contain allElementsOf f(v).map(toSemErr))

    def hasAtLeastOneGqlErrorIn(f: CypherVersion => Seq[(GqlError, String, Pos)]): Self =
      assertIn(v => r => r.errors should contain atLeastOneElementOf f(v).map(toSemErr))

    def failsWithMessageContaining(msg: String): Any = assertTry { res =>
      res should be a Symbol("failure")
      normalizeNewLines(res.failed.get.getMessage) should include(msg)
    }

    def assert(assertion: SemanticAnalysisResult => Unit): AnalysisAssertions = {
      versions.foreach { version =>
        results(version) match {
          case Success(result) => withContext(version)(assertion(result))
          case Failure(t)      => fail(s"Expected to succeed in CYPHER $version: $t", t)
        }
      }
      this
    }

    def assertSemanticState(assertion: SemanticState => Unit): AnalysisAssertions = {
      assert((semanticAnalysisResult: SemanticAnalysisResult) => {
        semanticAnalysisResult.state.maybeSemantics match {
          case Some(semantics) => assertion(semantics)
          case None            => fail(s"Cannot get the SemanticState")
        }
      })
    }

    def assertTry(assertion: Try[SemanticAnalysisResult] => Unit): AnalysisAssertions = {
      versions.foreach(version => withContext(version)(assertion(results(version))))
      this
    }

    def assertTryIn(assertion: CypherVersion => Try[SemanticAnalysisResult] => Unit): AnalysisAssertions = {
      versions.foreach(version => withContext(version)(assertion(version)(results(version))))
      this
    }

    def assertIn(assertion: CypherVersion => SemanticAnalysisResult => Unit): AnalysisAssertions = {
      versions.foreach { version =>
        results(version) match {
          case Success(result) => withContext(version)(assertion(version)(result))
          case Failure(t)      => withContext(version)(fail(t))
        }
      }
      this
    }

    private def toSemErr(a: (GqlError, String, Pos)): SemanticError = SemanticError(a._1, a._2, a._3)

    private def withContext(version: CypherVersion)(f: => Unit): Unit = {
      withClue(
        s"""Version: CYPHER $version
           |Query: ${analyse.query}
           |""".stripMargin
      )(f)
    }
  }

  final case object ProjectNamedPathsPhase extends Phase[BaseContext, BaseState, BaseState] {
    override def phase: CompilationPhaseTracer.CompilationPhase = AST_REWRITE

    override def process(from: BaseState, context: BaseContext): BaseState = {
      from.withStatement(from.statement().endoRewrite(ProjectNamedPaths))
    }
    override def postConditions: Set[StepSequencer.Condition] = Set.empty
  }
}

trait SemanticAnalysisTestSuiteWithDefaultQuery extends SemanticAnalysisTestSuite {

  def defaultQuery: String

  // Set to None if non position extraction was performed
  def defaultPositions: Option[Seq[InputPosition]] = None

  def run(): AnalysisAssertions = runWith()

  def runWith(features: SemanticFeature*): AnalysisAssertions =
    runWith(defaultQuery, features: _*)

  def runWith(query: String, features: SemanticFeature*): AnalysisAssertions =
    run(query, semanticFeatures = features)

  def run(disabledCypherVersions: Set[CypherVersion]): AnalysisAssertions = runWith(disabledCypherVersions)

  def runWith(disabledCypherVersions: Set[CypherVersion], features: SemanticFeature*): AnalysisAssertions =
    runWith(defaultQuery, disabledCypherVersions: Set[CypherVersion], features: _*)

  def runWith(disabledCypherVersions: Set[CypherVersion], pipeline: Pipeline): AnalysisAssertions =
    run(defaultQuery, defaultPositions, pipeline, disabledVersions = disabledCypherVersions)

  def runWith(
    disabledCypherVersions: Set[CypherVersion],
    pipeline: Pipeline,
    features: SemanticFeature*
  ): AnalysisAssertions =
    run(
      defaultQuery,
      defaultPositions,
      pipeline,
      disabledVersions = disabledCypherVersions,
      semanticFeatures = features
    )

  def runWith(
    query: String,
    disabledCypherVersions: Set[CypherVersion],
    features: SemanticFeature*
  ): AnalysisAssertions =
    run(
      query,
      defaultPositions,
      semanticAnalysisTwice(),
      disabledVersions = disabledCypherVersions,
      semanticFeatures = features
    )
}

trait CheckGqlDisjunctionError extends SemanticAnalysisTestSuiteWithDefaultQuery {

  def checkGqlDisjunctionError(error: SemanticErrorDef, invalidSymbol: String): Unit = {
    val gqlError = error.gqlStatusObject
    gqlError.gqlStatus() shouldBe "42001"

    gqlError.cause() should not be empty
    val cause = gqlError.cause().get()
    cause.gqlStatus() shouldBe "42I20"
    cause.statusDescription() shouldBe
      s"""
         |error: syntax error or access rule violation - invalid symbol in expression.
         | Label expressions and relationship type expressions cannot contain '$invalidSymbol'.
         | To express a label disjunction use '|' instead.""".stripMargin.linesIterator.mkString
  }
}

trait NameBasedSemanticAnalysisTestSuite extends CheckGqlDisjunctionError with TestName {
  override def defaultQuery: String = testName
}

trait NameWithPositionCaretBasedSemanticAnalysisTestSuite extends CheckGqlDisjunctionError
    with TestNameWithCaretPosition {
  override def defaultQuery: String = testName

  override def defaultPositions: Option[Seq[InputPosition]] =
    Some(testPositions.map {
      case cp: InputPositionFromCaret.Simple => InputPosition.Simple(cp.offset, cp.line, cp.column)
      case cp: InputPositionFromCaret.Range  => InputPosition.Range(cp.offset, cp.line, cp.column, cp.inputLength)
    })
}

trait ErrorMessageProviderAdapter extends ErrorMessageProvider {

  override def createMissingPropertyLabelHintError(
    operatorDescription: String,
    hintStringification: String,
    missingThingDescription: String,
    foundThingsDescription: String,
    entityDescription: String,
    entityName: String,
    additionalInfo: String
  ): String = ???

  override def createSelfReferenceError(name: String, clauseName: String): String = ???

  override def createSelfReferenceError(name: String, variableType: String, clauseName: String): String = ???

  override def createUseClauseUnsupportedError(): String = ???

  override def createDynamicGraphReferenceUnsupportedError(graphName: String): String = ???

  override def createMultipleGraphReferencesError(graphName: String, transactionalDefault: Boolean = false): String =
    ???
}
