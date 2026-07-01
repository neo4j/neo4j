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
package org.neo4j.cypher.internal.frontend.scoping

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.ConditionalQueryBranch
import org.neo4j.cypher.internal.ast.ConditionalQueryWhen
import org.neo4j.cypher.internal.ast.Finish
import org.neo4j.cypher.internal.ast.GroupBy
import org.neo4j.cypher.internal.ast.LocalCallableDefinition
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.Search
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.ScopeQueries
import org.neo4j.cypher.internal.ast.semantics.scoping.CommonContext
import org.neo4j.cypher.internal.ast.semantics.scoping.Declarations
import org.neo4j.cypher.internal.ast.semantics.scoping.ExpressionResult
import org.neo4j.cypher.internal.ast.semantics.scoping.ExpressionScope
import org.neo4j.cypher.internal.ast.semantics.scoping.LocalCallableScopeSignature
import org.neo4j.cypher.internal.ast.semantics.scoping.NoResult
import org.neo4j.cypher.internal.ast.semantics.scoping.OmittedResult
import org.neo4j.cypher.internal.ast.semantics.scoping.PatternIncomingContext
import org.neo4j.cypher.internal.ast.semantics.scoping.PatternScope
import org.neo4j.cypher.internal.ast.semantics.scoping.ProjectionExpressionContext
import org.neo4j.cypher.internal.ast.semantics.scoping.ProjectionSpecification
import org.neo4j.cypher.internal.ast.semantics.scoping.References
import org.neo4j.cypher.internal.ast.semantics.scoping.RegularContext
import org.neo4j.cypher.internal.ast.semantics.scoping.Result
import org.neo4j.cypher.internal.ast.semantics.scoping.ScopeState.RecordedScopes
import org.neo4j.cypher.internal.ast.semantics.scoping.StatementScope
import org.neo4j.cypher.internal.ast.semantics.scoping.SymbolGroup
import org.neo4j.cypher.internal.ast.semantics.scoping.TableResult
import org.neo4j.cypher.internal.ast.semantics.scoping.TableResultWithNotYetKnownColumns
import org.neo4j.cypher.internal.ast.semantics.scoping.WorkingContext
import org.neo4j.cypher.internal.ast.semantics.scoping.WorkingScope
import org.neo4j.cypher.internal.expressions.AllReducePredicate.AllReduceScope
import org.neo4j.cypher.internal.expressions.AllReducePredicate.ReductionStepVariableScope
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.ExtractScope
import org.neo4j.cypher.internal.expressions.FilterScope
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternAtom
import org.neo4j.cypher.internal.expressions.PatternElement
import org.neo4j.cypher.internal.expressions.PatternPart
import org.neo4j.cypher.internal.expressions.ReduceScope
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.frontend.helpers.ErrorCollectingContext
import org.neo4j.cypher.internal.frontend.helpers.NoPlannerName
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.frontend.phases.NoOp
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.AggregationAnalysis
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.Parse
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.PreparatoryRewriting
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.ScopeSurveyor
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.VariableChecker
import org.neo4j.cypher.internal.label_expressions.LabelExpression
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.ErrorMessageProvider
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.NotImplementedErrorMessageProvider
import org.neo4j.cypher.internal.util.Ref
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.TestName
import org.neo4j.gqlstatus.ErrorGqlStatusObject
import org.scalatest.Assertion
import org.scalatest.BeforeAndAfterAll

import java.io.FileWriter
import java.io.Writer

import scala.annotation.tailrec
import scala.jdk.OptionConverters.RichOptional

trait VariableCheckingTestSuite extends CypherFunSuite with TestName with BeforeAndAfterAll {

  val unit = Set.empty[String]
  val noCallables = Set.empty[Callable]
  val noKeys = Set.empty[ExpectedGroupingKey]
  val feature: Set[SemanticFeature] = Set.empty

  val allCheckerTransformer: Transformer[BaseContext, BaseState, BaseState] =
    VariableChecker andThen AggregationAnalysis
  val checkersUnderTest: Seq[Transformer[BaseContext, BaseState, BaseState]] = Seq(VariableChecker)

  def varOf(name: String, offset: Int): Variable =
    Variable(name)(InputPosition(offset, 0, 0), isIsolated = false)

  sealed trait ExpectedCharacteristic

  case class Ast(astNodeString: String) extends ExpectedCharacteristic

  case class Callable(name: String, result: ExpectedCallableResult) extends ExpectedCharacteristic {
    lazy val names: List[String] = name.split('.').toList
    lazy val _name: String = names.last
    lazy val _namespace: List[String] = names.dropRight(1)
  }

  case class Incoming(
    constants: Set[String] = Set.empty,
    variables: Set[String] = Set.empty,
    localCallables: Set[Callable] = Set.empty
  ) extends ExpectedCharacteristic

  /**
   *  Structured incoming expectation for projection-clause expression scopes.
   */
  case class ProjectionIncoming(
    constants: Set[String] = Set.empty,
    variables: Set[String] = Set.empty,
    localCallables: Set[Callable] = Set.empty,
    groupingKeys: Set[ExpectedGroupingKey] = Set.empty
  ) extends ExpectedCharacteristic

  /**
   *  Identifies a grouping key by its user-visible name (the alias if the actual key
   *  has one, otherwise the stringified expression). Optionally pin the underlying
   *  expression too.
   */
  case class ExpectedGroupingKey(name: String, expression: Option[String] = None)

  def gk(name: String): ExpectedGroupingKey = ExpectedGroupingKey(name)
  def gk(name: String, ofExpression: String): ExpectedGroupingKey = ExpectedGroupingKey(name, Some(ofExpression))

  case class PatternIncoming(
    topology: Set[String] = Set.empty,
    predicate: Set[String] = Set.empty,
    path: Set[String] = Set.empty,
    localCallables: Set[Callable] = Set.empty
  ) extends ExpectedCharacteristic

  case class Referenced(variables: Set[String]) extends ExpectedCharacteristic

  /**
   * Opt-in richer form of `Referenced` that asserts the target (declaration) each caller
   * resolves to. Use when caller names alone aren't enough to pin the intent — e.g. QPP
   * singleton-to-column refs, union-mapping refs, chained references.
   *
   * Matched by mapping `ws.referenced.references` to `Map[String, String]` via
   * `(caller, decl) => caller.value.name -> decl.value.name`.
   */
  case class ReferencesTo(pairs: Map[String, String]) extends ExpectedCharacteristic

  /**
   * Further opt-in variant that includes positions, for the narrow set of tests where two
   * declarations share a name (e.g. QPP x-singleton vs x-group). Entries are
   * `(callerName, callerOffset, declName, declOffset)`.
   */
  case class ReferencesToAt(pairs: Seq[(String, Int, String, Int)]) extends ExpectedCharacteristic

  case class Declared(
    constants: Seq[String] = Seq.empty,
    variables: Seq[String] = Seq.empty,
    localCallables: Seq[Callable] = Seq.empty
  ) extends ExpectedCharacteristic

  case class Outgoing(
    constants: Set[String] = Set.empty,
    variables: Set[String] = Set.empty,
    localCallables: Set[Callable] = Set.empty
  ) extends ExpectedCharacteristic

  sealed trait ExpectedResult extends ExpectedCharacteristic
  sealed trait ExpectedCallableResult extends ExpectedResult

  object ExpectedResult {
    case class TableResult(columns: String*) extends ExpectedCallableResult

    case object TableResultWithNotYetKnownColumns extends ExpectedResult

    case object OmittedResult extends ExpectedCallableResult

    case object NoResult extends ExpectedResult

    case object ExpressionResult extends ExpectedCallableResult
  }

  case class InImportingWith(value: Boolean = true) extends ExpectedCharacteristic

  case class ExpectedSymbolGroup(declaration: LogicalVariable, uses: Seq[LogicalVariable], renaming: Option[String])
  case class ExpectedWorkingScope(expectedCharacteristics: ExpectedCharacteristic*) extends ExpectedCharacteristic

  object ExpectedWorkingScope {

    def varExp(
      name: String,
      incomingConstants: Set[String],
      incomingVariables: Set[String] = Set.empty,
      incomingCallables: Set[Callable] = Set.empty
    ): ExpectedWorkingScope =
      ExpectedWorkingScope(
        Ast(name),
        Incoming(constants = incomingConstants, variables = incomingVariables, localCallables = incomingCallables),
        Referenced(Set(name))
      )

    def varProjExp(
      name: String,
      incomingConstants: Set[String] = Set.empty,
      incomingVariables: Set[String] = Set.empty,
      incomingCallables: Set[Callable] = Set.empty,
      incomingKeys: Set[ExpectedGroupingKey] = Set.empty,
      referenced: Set[String] = Set.empty
    ): ExpectedWorkingScope =
      ExpectedWorkingScope(
        Ast(name),
        ProjectionIncoming(
          constants = incomingConstants,
          variables = incomingVariables,
          localCallables = incomingCallables,
          groupingKeys = incomingKeys
        ),
        if (referenced.isEmpty) Referenced(Set(name)) else Referenced(referenced)
      )

    def constExp(
      ast: String,
      incoming: Set[String] = Set.empty,
      incomingCallables: Set[Callable] = Set.empty
    ): ExpectedWorkingScope = {
      if (incoming.isEmpty && incomingCallables.isEmpty) {
        ExpectedWorkingScope(
          Ast(ast)
        )
      } else {
        ExpectedWorkingScope(
          Ast(ast),
          Incoming(constants = incoming, localCallables = incomingCallables)
        )
      }
    }

    def constProjExp(
      ast: String,
      incoming: Set[String] = Set.empty,
      incomingCallables: Set[Callable] = Set.empty,
      incomingKeys: Set[ExpectedGroupingKey] = Set.empty
    ): ExpectedWorkingScope = {
      if (incoming.isEmpty && incomingCallables.isEmpty) {
        ExpectedWorkingScope(
          Ast(ast)
        )
      } else {
        ExpectedWorkingScope(
          Ast(ast),
          ProjectionIncoming(
            constants = incoming,
            localCallables = incomingCallables,
            groupingKeys = incomingKeys
          )
        )
      }
    }
  }

  private val prettifier: Prettifier = Prettifier(ExpressionStringifier())

  def prettify(astNode: ASTNode): String = astNode match {
    case s: Statement               => prettifier.asString(s)
    case d: LocalCallableDefinition => prettifier.asString(d)
    case c: Clause                  => prettifier.asString(SingleQuery(Seq(c))(InputPosition.NONE))
    case g: GroupBy                 => prettifier.asString(g)
    case s: Search                  => prettifier.asString(s)
    case ExtractScope(v, pred, extract) =>
      s"[${v.name}${pred.fold("")(p =>
          s" WHERE ${prettifier.expr(p)}"
        )}${extract.fold("")(e => s" | ${prettifier.expr(e)}")}]"
    case FilterScope(v, pred) =>
      s"[${v.name}${pred.fold("")(p => s" WHERE ${prettifier.expr(p)}")}]"
    case ReduceScope(acc, v, e) =>
      s"reduceScope(${acc.name}, ${v.name} | ${prettifier.expr(e)})"
    case AllReduceScope(acc, redStep) =>
      s"allReduceScope(${acc.name}, ${prettify(redStep)})"
    case ReductionStepVariableScope(v, step, pred) =>
      s"reductionStep(${v.name} | ${prettifier.expr(step)}, ${prettifier.expr(pred)})"
    case ex: Expression         => prettifier.expr(ex)
    case p: Pattern             => prettifier.expr.patterns(p)
    case p: PatternPart         => prettifier.expr.patterns(p)
    case p: PatternElement      => prettifier.expr.patterns(p)
    case p: RelationshipPattern => prettifier.expr.patterns(p)
    case lex: LabelExpression   => prettifier.expr.stringifyLabelExpression(lex)
    case cqb @ ConditionalQueryBranch(Some(_), _) =>
      prettifier.asString(ConditionalQueryWhen(Seq(cqb), None)(InputPosition.NONE))
    case cqb @ ConditionalQueryBranch(None, _) =>
      prettifier.asString(ConditionalQueryWhen(Seq(), Some(cqb))(InputPosition.NONE))
    case x => x.toString
  }

  private def whitespaceNormalization(cypher: String): String =
    cypher.trim.replaceAll("\\s+", " ")

  private def messageProvider: ErrorMessageProvider = NotImplementedErrorMessageProvider

  private val testLog: Boolean = false
  private var log: Writer = _

  private val logPPrint = pprint.PPrinter.BlackWhite.copy(
    additionalHandlers = {
      case astNode: ASTNode => prepQueryText(prettify(astNode))
    }
  )

  private def prepQueryText(query: String) = {
    val singleLine = whitespaceNormalization(query)
    val shortenedLine =
      if (singleLine.length > 60) {
        singleLine.take(30) + " /* … */ " + singleLine.takeRight(20)
      } else {
        singleLine
      }
    pprint.Tree.Literal(s"\"$shortenedLine\"")
  }

  override def beforeAll(): Unit = {
    if (testLog) {
      try {
        log = new FileWriter(s"target/${getClass.getSimpleName}.log")
      } catch {
        case e: Throwable => new RuntimeException(e)
      }
    }
  }

  private def runStatement(
    statement: Statement,
    version: CypherVersion,
    skipVariableChecker: Boolean = false,
    withPrepRewriting: Boolean = false,
    withoutCachingCheck: Boolean = false
  ): Either[BaseState, Seq[SemanticError]] = {
    val context =
      new ErrorCollectingContext(version, semanticFeatures = Seq(ScopeQueries)) {
        override def errorMessageProvider: ErrorMessageProvider = messageProvider
      }
    // running the ScopeSurveyor twice in a row is a trivial test that its working scope caching is idempotent w.r.t the resulting working scope
    val scopeSurveyorPipe =
      if (withoutCachingCheck) ScopeSurveyor
      else ScopeSurveyor andThen ScopeSurveyor

    val transformers = {
      if (skipVariableChecker) scopeSurveyorPipe
      else if (withPrepRewriting) PreparatoryRewriting andThen scopeSurveyorPipe andThen allCheckerTransformer
      else {
        scopeSurveyorPipe andThen allCheckerTransformer
      }
    }
    val state = transformers.transform(initialStateWithStatement(statement), context)

    if (context.errors.isEmpty) {
      Left(state)
    } else {
      Right(context.errors.collect { case e: SemanticError => e })
    }
  }

  private def runStatementAndRewrittenStatement(
    statementBefore: Statement,
    statementAfter: Statement,
    version: CypherVersion,
    skipVariableChecker: Boolean = false,
    withPrepRewriting: Boolean = false,
    withoutCachingCheck: Boolean = false
  ): Either[BaseState, Seq[SemanticError]] = {
    val context =
      new ErrorCollectingContext(version, semanticFeatures = Seq(ScopeQueries)) {
        override def errorMessageProvider: ErrorMessageProvider = messageProvider
      }
    // running the ScopeSurveyor twice in a row is a trivial test that its working scope caching is idempotent w.r.t the resulting working scope
    val scopeSurveyorPipe =
      if (withoutCachingCheck) ScopeSurveyor
      else ScopeSurveyor andThen ScopeSurveyor

    val transformers = {
      if (skipVariableChecker) scopeSurveyorPipe
      else if (withPrepRewriting) PreparatoryRewriting andThen scopeSurveyorPipe andThen VariableChecker
      else {
        scopeSurveyorPipe andThen VariableChecker
      }
    }
    val stateFinalBefore = transformers.transform(initialStateWithStatement(statementBefore), context)
    val stateInitialAfter = stateFinalBefore.withStatement(statementAfter)
    val stateFinalAfter = transformers.transform(stateInitialAfter, context)

    if (context.errors.isEmpty) {
      Left(stateFinalAfter)
    } else {
      Right(context.errors.collect { case e: SemanticError => e })
    }
  }

  type WorkingScopeModification = WorkingScope => WorkingScope

  /**
   * Adds a marker child scope to a WorkingScope, leaving its own `astNode` intact.
   */
  def markWorkingScopeModified(marker: ASTNode): WorkingScopeModification = {
    val markerChild = StatementScope(
      astNode = marker,
      incoming = RegularContext.unit,
      referenced = References.empty,
      declared = Declarations.noDeclarations,
      outgoing = RegularContext.unit,
      result = NoResult,
      children = WorkingScope.noChildren
    )

    def withExtraChild(ws: WorkingScope): WorkingScope = ws.withChildren(ws.children :+ markerChild)

    {
      case s: StatementScope  => withExtraChild(s)
      case s: PatternScope    => withExtraChild(s)
      case s: ExpressionScope => withExtraChild(s)
      case s                  => s
    }
  }

  type CacheModificationKey = (Ref[ASTNode], WorkingContext)

  def shouldPickUpCacheModifications(
    statement: Statement,
    cacheModification: Map[CacheModificationKey, WorkingScopeModification],
    version: CypherVersion = CypherVersion.Cypher25
  ): Unit = testPickUpOfCacheModifications(statement, cacheModification, version, shouldPickUp = true)

  def shouldNotPickUpCacheModifications(
    statement: Statement,
    cacheModification: Map[CacheModificationKey, WorkingScopeModification],
    version: CypherVersion = CypherVersion.Cypher25
  ): Unit = testPickUpOfCacheModifications(statement, cacheModification, version, shouldPickUp = false)

  private def testPickUpOfCacheModifications(
    statement: Statement,
    cacheModification: Map[CacheModificationKey, WorkingScopeModification],
    version: CypherVersion,
    shouldPickUp: Boolean
  ): Unit = {
    val context =
      new ErrorCollectingContext(version, semanticFeatures = Seq(ScopeQueries)) {
        override def errorMessageProvider: ErrorMessageProvider = messageProvider
      }

    class ScopeTreeTransformation extends Transformer[BaseContext, BaseState, BaseState] {
      var newWorkingScopeOpt: Option[WorkingScope] = None
      var newRecordedScopesOpt: Option[RecordedScopes] = None // for debugging purposes

      override def transform(from: BaseState, context: BaseContext): BaseState = {
        val newScopeState = from.maybeScopeState match {
          case None =>
            throw new RuntimeException(s"${prettify(statement)} did not have a ScopeState before modifying the cache")
          case Some(scopeState) =>
            // modify the working scope and recorded scopes to what it will look like if it picks up the modified cache
            def modifyWorkingScopeTopDown(
              workingScope: WorkingScope,
              recordedScopes: RecordedScopes
            ): (WorkingScope, RecordedScopes, Boolean, Set[CacheModificationKey]) = {
              cacheModification.collectFirst {
                case modKey -> modifyWorkingScope
                  if modKey._1 == Ref(workingScope.astNode) &&
                    modKey._2 == workingScope.incoming =>
                  val newWorkingScope = modifyWorkingScope(workingScope)
                  val newRecordedScopes = recordedScopes + (modKey._1 -> newWorkingScope)
                  val modified = true
                  val expectedWorkingScope = if (shouldPickUp) newWorkingScope else workingScope
                  (expectedWorkingScope, newRecordedScopes, modified, Set(modKey))
              } match {
                // no modification found
                case None =>
                  // recurse to children
                  val (newChildWorkingScopes, newRecordedScopes, modified, usedModifications) =
                    workingScope.children.foldLeft((
                      Seq.empty[WorkingScope],
                      recordedScopes,
                      false,
                      Set.empty[CacheModificationKey]
                    )) {
                      case ((newChildWorkingScopes, recordedScopes, modified, usedModifications), child) =>
                        val (newChildWorkingScope, newRecordedScopes, newModified, moreUsedModifications) =
                          modifyWorkingScopeTopDown(child, recordedScopes)
                        val unitedUsedModifications = usedModifications union moreUsedModifications
                        (
                          newChildWorkingScopes :+ newChildWorkingScope,
                          newRecordedScopes,
                          modified || newModified,
                          unitedUsedModifications
                        )
                    }
                  // if any of the children is modified, we need to remove the parent from the cache
                  val cleanedNewRecordedScopes = if (modified) {
                    newRecordedScopes.removed(Ref(workingScope.astNode))
                  } else newRecordedScopes
                  val expectedWorkingScope =
                    if (shouldPickUp) workingScope.withChildren(newChildWorkingScopes) else workingScope
                  (expectedWorkingScope, cleanedNewRecordedScopes, modified, usedModifications)
                // modification found
                case Some(result) => result
              }
            }
            val (newWorkingScope, modifiedRecordedScopes, _, usedModifications) =
              modifyWorkingScopeTopDown(scopeState.workingScope, scopeState.recordedScopes)
            // add unused modification to recorded scopes
            val unusedModifications = cacheModification.filterNot(e => usedModifications contains e._1)
            val newRecordedScopes: RecordedScopes = unusedModifications.foldLeft(modifiedRecordedScopes) {
              case (recordedScopes, (astNodeRef, incoming) -> modifyWorkingScope) =>
                val modifiedScope = modifyWorkingScope(astNodeRef.value match {
                  case e: Expression =>
                    ExpressionScope(
                      e,
                      incoming.asInstanceOf[RegularContext],
                      References.empty,
                      Declarations.noDeclarations
                    )
                  case e: LabelExpression =>
                    ExpressionScope(
                      e,
                      incoming.asInstanceOf[RegularContext],
                      References.empty,
                      Declarations.noDeclarations
                    )
                  case p: Pattern => PatternScope(
                      p,
                      incoming.asInstanceOf[PatternIncomingContext],
                      References.empty,
                      Declarations.noDeclarations,
                      TableResult(Seq.empty)
                    )
                  case p: PatternPart => PatternScope(
                      p,
                      incoming.asInstanceOf[PatternIncomingContext],
                      References.empty,
                      Declarations.noDeclarations,
                      TableResult(Seq.empty)
                    )
                  case p: PatternElement => PatternScope(
                      p,
                      incoming.asInstanceOf[PatternIncomingContext],
                      References.empty,
                      Declarations.noDeclarations,
                      TableResult(Seq.empty)
                    )
                  case p: PatternAtom => PatternScope(
                      p,
                      incoming.asInstanceOf[PatternIncomingContext],
                      References.empty,
                      Declarations.noDeclarations,
                      TableResult(Seq.empty)
                    )
                  case x => StatementScope(
                      x,
                      incoming.asInstanceOf[RegularContext],
                      References.empty,
                      Declarations.noDeclarations,
                      RegularContext.unit,
                      TableResult(Seq.empty)
                    )
                })
                recordedScopes + (astNodeRef -> modifiedScope)
            }
            newWorkingScopeOpt = Some(newWorkingScope)
            newRecordedScopesOpt = Some(newRecordedScopes)
            // modify the recorded scope in the state but keep the unmodified working scope in the state
            scopeState.copy(recordedScopes = newRecordedScopes)
        }
        // set modified state
        from.withScopeState(newScopeState)
      }

      override def postConditions: Set[StepSequencer.Condition] = Set.empty

      override def name: String = "scope tree rewrite"
    }
    val scopeTreeTransformation = new ScopeTreeTransformation()

    val transformers =
      ScopeSurveyor.getTransformerWithoutCheck andThen
        scopeTreeTransformation andThen
        ScopeSurveyor.getTransformerWithoutCheck

    val stateAfter = transformers.transform(initialStateWithStatement(statement), context)
    val actualWorkingScope = stateAfter.maybeScopeState match {
      case None => throw new RuntimeException(s"${prettify(statement)} did not have a ScopeState at end of pipeline")
      case Some(scopeState) => scopeState.workingScope
    }
    val expectedWorkingScope = scopeTreeTransformation.newWorkingScopeOpt match {
      case None => throw new RuntimeException(s"${prettify(statement)} did not have a expectedWorkingScope")
      case Some(expectedWorkingScope) => expectedWorkingScope
    }
    val modifiedRecordedScopes = scopeTreeTransformation.newRecordedScopesOpt match {
      case None => throw new RuntimeException(s"${prettify(statement)} did not have a newRecordedScopes")
      case Some(modifiedRecordedScopes) => modifiedRecordedScopes
    }
    val actualDump = pprint.apply(actualWorkingScope).toString
    val expectedDump = pprint.apply(expectedWorkingScope).toString
    withClue(
      s"""scope tree overview (! marks differing rows):
         |
         |${VariableCheckingTestUtil.sideBySideScopeTreeDiff(actualWorkingScope, expectedWorkingScope)}
         |
         |given cache
         |
         |${pprint.apply(modifiedRecordedScopes)}
         |
         |working scope line diff (-actual / +expected):
         |
         |${VariableCheckingTestUtil.unifiedLineDiff(actualDump, expectedDump)}
         |
         |full actual:
         |
         |$actualDump
         |
         |full expected:
         |
         |$expectedDump
         |""".stripMargin
    ) {
      actualWorkingScope shouldBe expectedWorkingScope
    }
  }

  private def runQuery(
    query: String,
    version: CypherVersion,
    checker: Transformer[BaseContext, BaseState, BaseState],
    skipVariableChecker: Boolean = false,
    withPrepRewriting: Boolean = false,
    withoutCachingCheck: Boolean = false
  ): Either[BaseState, Seq[SemanticError]] = {
    val context =
      new ErrorCollectingContext(version, semanticFeatures = Seq(ScopeQueries)) {
        override def errorMessageProvider: ErrorMessageProvider = messageProvider
      }
    // running the ScopeSurveyor twice in a row is a trivial test that its working scope caching is idempotent w.r.t the resulting working scope
    val scopeSurveyorPipe =
      if (withoutCachingCheck) ScopeSurveyor.getTransformerWithoutCheck
      else ScopeSurveyor andThen ScopeSurveyor

    val transformers = {
      if (skipVariableChecker) Parse andThen scopeSurveyorPipe
      else if (withPrepRewriting)
        Parse andThen PreparatoryRewriting andThen scopeSurveyorPipe andThen checker
      else {
        Parse andThen scopeSurveyorPipe andThen checker
      }
    }
    val state = transformers.transform(initialStateWithQuery(query), context)

    if (context.errors.isEmpty) {
      Left(state)
    } else {
      Right(context.errors.collect { case e: SemanticError => e })
    }
  }

  def passes(version: CypherVersion): Unit = pass(testName, version)

  def passesExceptIn(excludedVersion: CypherVersion): Unit =
    passExceptIn(testName, excludedVersion)

  def passes(): Unit =
    pass(testName, Array(CypherVersion.Cypher25), withRewriting = true)

  def passes(withRewriting: Boolean): Unit =
    pass(testName, Array(CypherVersion.Cypher25), withRewriting)

  def passes(versions: Array[CypherVersion]): Unit =
    pass(testName, versions, withRewriting = true)

  def passes(versions: Array[CypherVersion], withRewriting: Boolean): Unit =
    pass(testName, versions, withRewriting)

  def pass(query: String, version: CypherVersion): Unit = pass(query, versions = Array(version))

  def passExceptIn(query: String, excludedVersion: CypherVersion): Unit =
    pass(query, versions = CypherVersion.values().filter(_ != excludedVersion), withRewriting = true)

  def pass(query: String): Unit =
    pass(query, Array(CypherVersion.Cypher25), withRewriting = true)

  def pass(query: String, withRewriting: Boolean): Unit =
    pass(query, Array(CypherVersion.Cypher25), withRewriting)

  def pass(query: String, versions: Array[CypherVersion]): Unit =
    pass(query, versions, withRewriting = true)

  def pass(query: String, versions: Array[CypherVersion], withRewriting: Boolean): Unit = {
    val rewriteOptions = if (withRewriting) Seq(false, true) else Seq(false)
    checkersUnderTest.foreach(checker => {
      versions.foreach(version => {
        rewriteOptions.foreach(rewrite => {
          val rewriteMsg = if (rewrite) " after rewrite" else ""
          runQuery(query, version, checker, withPrepRewriting = rewrite) match {
            case Left(state) =>
              state.maybeScopeState should not be empty

              if (testLog) {
                log.append(
                  s"""Version: $version $rewriteMsg
                     |Checker: ${checker.name}
                     |Query:
                     |
                     |$query
                     |
                     |passed without errors.
                     |----------
                     |""".stripMargin
                )
              }
            case Right(semanticErrors) =>
              fail(
                s"""Version: $version $rewriteMsg
                   |Checker: ${checker.name}
                   |Query:
                   |
                   |$query
                   |
                   |is expected to be successful, but
                   |
                   |actually threw errors: ${pprint.apply(semanticErrors)}""".stripMargin
              )
          }
        })

      })
    })
  }

  def errorsInAllVersions(
    expectedGqlStatusCode: String,
    msgContains: String
  ): Unit = errorInAllVersions(testName, expectedGqlStatusCode, msgContains)

  def errorsExceptIn(
    gqlError: GqlError,
    excludedVersion: CypherVersion
  ): Unit = errorsExceptIn(gqlError.num, gqlError.msg, excludedVersion)

  def errorsExceptIn(
    expectedGqlStatusCode: String,
    msgContains: String,
    excludedVersion: CypherVersion
  ): Unit = errorExceptIn(testName, expectedGqlStatusCode, msgContains, excludedVersion)

  def errors(
    expectedGqlStatusCode: String,
    msgContains: String,
    version: CypherVersion
  ): Unit = error(testName, expectedGqlStatusCode, msgContains, version)

  def errors(
    expectedGqlStatusCode: String,
    msgContains: String
  ): Unit = error(testName, expectedGqlStatusCode, msgContains, versions = Array(CypherVersion.Cypher25))

  def errors(
    expectedGqlStatusCode: String,
    msgContains: String,
    versions: Array[CypherVersion]
  ): Unit = error(testName, expectedGqlStatusCode, msgContains, versions)

  def errorInAllVersions(
    query: String,
    expectedGqlStatusCode: String,
    msgContains: String
  ): Unit = error(query, expectedGqlStatusCode, msgContains, versions = CypherVersion.values())

  def errorExceptIn(
    query: String,
    gqlError: GqlError,
    excludedVersion: CypherVersion
  ): Unit = error(query, gqlError.num, gqlError.msg, versions = CypherVersion.values().filter(_ != excludedVersion))

  def errorExceptIn(
    query: String,
    expectedGqlStatusCode: String,
    msgContains: String,
    excludedVersion: CypherVersion
  ): Unit =
    error(query, expectedGqlStatusCode, msgContains, versions = CypherVersion.values().filter(_ != excludedVersion))

  def error(
    query: String,
    expectedGqlStatusCode: String,
    msgContains: String,
    version: CypherVersion
  ): Unit = error(query, expectedGqlStatusCode, msgContains, versions = Array(version))

  def error(
    query: String,
    gqlError: GqlError,
    version: CypherVersion
  ): Unit = error(query, gqlError, versions = Array(version))

  def error(
    query: String,
    gqlError: GqlError
  ): Unit = error(query, gqlError, versions = Array(CypherVersion.Cypher25))

  def error(
    query: String,
    gqlError: GqlError,
    versions: Array[CypherVersion]
  ): Unit =
    assertError(
      query,
      gqlError.num,
      gqlError.assertMsg,
      s"${msgMatchVerb(gqlError)}:\n  ${gqlError.msg}",
      versions
    )

  // Infinitive verb describing an error's message-match mode, for failure clues ("... to equal/contain:").
  private def msgMatchVerb(e: GqlError): String = e.msgMatch match {
    case MsgMatch.Equals   => "equal"
    case MsgMatch.Contains => "contain"
  }

  // Walks an error's cause chain looking for a specific GQL status code.
  @tailrec
  private def findGqlStatus(
    gqlStatusObject: ErrorGqlStatusObject,
    expectedGqlStatusCode: String
  ): Option[ErrorGqlStatusObject] = gqlStatusObject match {
    case gqlStatusObject if gqlStatusObject.gqlStatus() == expectedGqlStatusCode => Some(gqlStatusObject)
    case gqlStatusObject: ErrorGqlStatusObject =>
      gqlStatusObject.cause().toScala match {
        case Some(cause) => findGqlStatus(cause, expectedGqlStatusCode)
        case None        => None
      }
  }

  // All GQL status codes appearing anywhere in an error's cause chain.
  private def chainCodes(error: SemanticError): Set[String] = {
    @tailrec
    def loop(obj: ErrorGqlStatusObject, acc: Set[String]): Set[String] = {
      val next = acc + obj.gqlStatus()
      obj.cause().toScala match {
        case Some(cause) => loop(cause, next)
        case None        => next
      }
    }
    loop(error.gqlStatusObject, Set.empty)
  }

  // The most-specific (deepest cause) status object of an error chain.
  @tailrec
  private def leafStatusObject(obj: ErrorGqlStatusObject): ErrorGqlStatusObject =
    obj.cause().toScala match {
      case Some(cause) => leafStatusObject(cause)
      case None        => obj
    }

  private def leafCode(error: SemanticError): String = leafStatusObject(error.gqlStatusObject).gqlStatus()

  // Renders the produced errors as `<leafCode>: <leaf description>` lines, for failure clues.
  private def renderProducedErrors(errors: Seq[SemanticError]): String =
    if (errors.isEmpty) "  (none)"
    else errors.map { e =>
      val leaf = leafStatusObject(e.gqlStatusObject)
      s"  - ${leaf.gqlStatus()}: ${leaf.statusDescription()}"
    }.mkString("\n")

  // Runs the query for one checker/version and returns the produced semantic errors (empty if none).
  private def producedErrors(
    query: String,
    checker: Transformer[BaseContext, BaseState, BaseState],
    version: CypherVersion
  ): Seq[SemanticError] = runQuery(query, version, checker) match {
    case Left(_)       => Seq.empty
    case Right(errors) => errors
  }

  def error(
    query: String,
    expectedGqlStatusCode: String,
    msgContains: String,
    versions: Array[CypherVersion]
  ): Unit =
    assertError(query, expectedGqlStatusCode, _.contains(msgContains), s"contain:\n  $msgContains", versions)

  private def assertError(
    query: String,
    expectedGqlStatusCode: String,
    msgMatches: String => Boolean,
    msgClue: String,
    versions: Array[CypherVersion]
  ): Unit = {
    def findGqlStatus(gqlStatusObject: ErrorGqlStatusObject): Option[ErrorGqlStatusObject] =
      this.findGqlStatus(gqlStatusObject, expectedGqlStatusCode)

    checkersUnderTest.foreach(checker => {
      versions.foreach(version => {
        runQuery(query, version, checker) match {
          case Left(_) =>
            fail(
              s"""Version: $version
                 |Checker: ${checker.name}
                 |Query:
                 |
                 |$query
                 |
                 |is expected to throw an error - $expectedGqlStatusCode, but
                 |
                 |actually was successful""".stripMargin
            )
          case Right(semanticErrors) =>
            semanticErrors.collectFirst(Function.unlift {
              (semanticError: SemanticError) => findGqlStatus(semanticError.gqlStatusObject)
            }) match {
              case Some(gqlStatusObject) =>
                gqlStatusObject.gqlStatus() shouldBe expectedGqlStatusCode
                withClue(
                  s"\nExpected message to $msgClue\nbut was:\n  ${gqlStatusObject.statusDescription()}\n"
                )(msgMatches(gqlStatusObject.statusDescription()) shouldBe true)

                if (testLog) {
                  log.append(
                    s"""Version: $version
                       |Checker: ${checker.name}
                       |Query:
                       |
                       |$query
                       |
                       |Error:
                       |
                       |${logPPrint(gqlStatusObject).plainText.trim}
                       |----------
                       |""".stripMargin
                  )
                }
              case None => fail(
                  s"""Version: $version
                     |Checker: ${checker.name}
                     |Query:
                     |
                     |$query
                     |
                     |is expected to throw gql status $expectedGqlStatusCode, but
                     |
                     |actually did not.
                     |
                     |Errors:
                     |${semanticErrors.map(x => logPPrint(x.gqlStatusObject).plainText.trim).mkString(
                      ", "
                    )}""".stripMargin
                )
            }
        }
      })
    })
  }

  def check(outcome: Outcome): Unit =
    check(testName, outcome, CypherVersion.values())

  def check(outcome: Outcome, versions: Array[CypherVersion]): Unit =
    check(testName, outcome, versions)

  def check(query: String, outcome: Outcome): Unit =
    check(query, outcome, CypherVersion.values())

  def check(query: String, outcome: Outcome, versions: Array[CypherVersion]): Unit =
    outcome match {
      case Ignore               => ()
      case Passes               => pass(query, versions)
      case e: GqlError          => error(query, e, versions)
      case AllOf(outcomes @ _*) => outcomes.foreach(o => check(query, o, versions))
      case Absent(codes @ _*)   => absent(query, codes.toSet, versions)
      case Exactly(errors @ _*) => exactly(query, errors, versions)
      case Versioned(default, cases @ _*) =>
        val versionsWithExceptions = cases.map(_._1).distinct
        val versionsForDefault = versions.filterNot(v => versionsWithExceptions contains v)
        check(query, default, versionsForDefault)
        for {
          (v, o) <- cases
          if versions contains v
        } {
          check(query, o, Array(v))
        }
    }

  // Header shown in failure clues: which run, the query, and every error it produced.
  private def producedErrorsClue(
    query: String,
    checker: Transformer[BaseContext, BaseState, BaseState],
    version: CypherVersion,
    errors: Seq[SemanticError]
  ): String =
    s"""Version: $version
       |Checker: ${checker.name}
       |Query:
       |$query
       |
       |Produced errors:
       |${renderProducedErrors(errors)}
       |""".stripMargin

  // Asserts that none of `codes` appears anywhere in any produced error's cause chain.
  def absent(query: String, codes: Set[String], versions: Array[CypherVersion]): Unit =
    checkersUnderTest.foreach(checker =>
      versions.foreach(version => {
        val errors = producedErrors(query, checker, version)
        val present = errors.flatMap(chainCodes).toSet intersect codes
        withClue(
          producedErrorsClue(query, checker, version, errors) +
            s"\nExpected NONE of ${codes.mkString(", ")}, but found: ${present.mkString(", ")}\n"
        )(present shouldBe empty)
      })
    )

  // Asserts that the produced errors' leaf (most-specific) status codes are exactly the codes of
  // `expectedErrors`, and that each expected error's message is present.
  def exactly(query: String, expectedErrors: Seq[GqlError], versions: Array[CypherVersion]): Unit = {
    val expectedCodes = expectedErrors.map(_.num).toSet
    checkersUnderTest.foreach(checker =>
      versions.foreach(version => {
        val errors = producedErrors(query, checker, version)
        val actualCodes = errors.map(leafCode).toSet
        withClue(producedErrorsClue(query, checker, version, errors)) {
          withClue(s"\nExpected exactly the leaf codes $expectedCodes, but got $actualCodes\n")(
            actualCodes shouldBe expectedCodes
          )
          expectedErrors.foreach { e =>
            val matched = errors.exists(err =>
              findGqlStatus(err.gqlStatusObject, e.num).exists(s => e.assertMsg(s.statusDescription()))
            )
            withClue(s"\nExpected an error ${e.num} whose message must ${msgMatchVerb(e)}:\n  ${e.msg}\n")(
              matched shouldBe true
            )
          }
        }
      })
    )
  }

  def hasScope(
    expected: ExpectedWorkingScope,
    version: CypherVersion,
    skipVariableChecker: Boolean
  ): Unit = hasScope(expected, Array(version), skipVariableChecker)

  def hasScope(
    expected: ExpectedWorkingScope,
    versions: Array[CypherVersion] = Array(CypherVersion.Cypher25),
    skipVariableChecker: Boolean = false
  ): Unit = {
    val query = testName
    versions.foreach(version => {
      runQuery(query, version, allCheckerTransformer, skipVariableChecker) match {
        case Left(state) =>
          state.maybeScopeState should not be empty
          val ss = state.maybeScopeState.get

          assertExpectation(ss.workingScope, expected)

          if (testLog) {
            log.append(
              s"""Query:
                 |
                 |$query
                 |
                 |Working scope:
                 |
                 |${logPPrint(ss.workingScope)}
                 |----------
                 |""".stripMargin
            )
          }
        case Right(semanticErrors) =>
          fail(
            s"""Version: $version
               |Query:
               |
               |$query
               |
               |is expected to be successful, but
               |
               |actually threw errors: ${pprint.apply(semanticErrors)}""".stripMargin
          )
      }

    })
  }

  def hasSymbolGroups(
    expected: Seq[ExpectedSymbolGroup],
    versions: Array[CypherVersion] = Array(CypherVersion.Cypher25)
  ): Unit = {
    val query = testName
    versions.foreach(version => {
      runQuery(query, version, NoOp()) match {
        case Left(state) =>
          state.maybeScopeState should not be empty
          val ss = state.maybeScopeState.get
          val anonVarGen = new AnonymousVariableNameGenerator()
          val symbolGroups = ss.workingScope.getSymbolGroups(anonVarGen)

          assertExpectedSymbolGroups(symbolGroups, expected)

          if (testLog) {
            log.append(
              s"""Query:
                 |
                 |$query
                 |
                 |Working scope:
                 |
                 |${logPPrint(ss.workingScope)}
                 |----------
                 |""".stripMargin
            )
          }
        case Right(semanticErrors) =>
          fail(
            s"""Version: $version
               |Query:
               |
               |$query
               |
               |is expected to be successful, but
               |
               |actually threw errors: ${pprint.apply(semanticErrors)}""".stripMargin
          )
      }

    })
  }

  private def assertExpectedSymbolGroups(symbolGroups: Seq[SymbolGroup], expected: Seq[ExpectedSymbolGroup]): Unit = {
    import VariableCheckingTestUtil._

    def actualCells(sg: SymbolGroup): SymbolGroupRow =
      (fmtVar(sg.declaration.value), fmtRen(sg.renaming), fmtUses(sg.uses.toSeq.map(_.value)))

    def expectedCells(eg: ExpectedSymbolGroup): SymbolGroupRow =
      (fmtVar(eg.declaration), fmtRen(eg.renaming), fmtUses(eg.uses))

    val table = sideBySideSymbolGroupTable(symbolGroups.map(actualCells), expected.map(expectedCells))

    val globalClue =
      s"""|
          |Symbol groups (actual vs expected):
          |$table
          |
          |Actual pastable:
          |${symbolGroups.map("  " + _.pastablePrint).mkString(",\n")}
          |""".stripMargin

    withClue(globalClue) {
      withClue(s"Wrong number of symbol groups (actual=${symbolGroups.size}, expected=${expected.size}).") {
        expected.size shouldEqual symbolGroups.size
      }

      symbolGroups.foreach {
        case sg @ SymbolGroup(declaration, uses, renaming) =>
          val comparableGroup = ExpectedSymbolGroup(declaration.value, uses.toSeq.map(_.value), renaming)
          val expectedElement = expected.find(_ == comparableGroup)

          val (aDecl, aRen, aUses) = actualCells(sg)
          withClue(s"\nRow under test: decl=$aDecl  renaming=$aRen  uses=$aUses\n") {
            withClue("Symbol Group missing from expected") {
              expectedElement should contain(comparableGroup)
            }

            withClue("Wrong position of declaration") {
              (
                expectedElement.get.declaration,
                expectedElement.get.declaration.position.offset
              ) shouldEqual (declaration.value, declaration.value.position.offset)
            }

            withClue("Wrong position of use") {
              expectedElement.get.uses.map(lv => (lv, lv.position.offset)) should contain allElementsOf uses.toSeq.map(
                rv =>
                  (rv.value, rv.value.position.offset)
              )
            }
          }
      }
    }
  }

  private def assertExpectation(ws: WorkingScope, expected: ExpectedWorkingScope): Unit = {
    val astNodeString = expected.expectedCharacteristics.collectFirst {
      case Ast(s) => s
    }.getOrElse("—no expected ast node string given—")
    val incoming = expected.expectedCharacteristics.collectFirst {
      case i: Incoming => i
    }.getOrElse(Incoming(unit, unit, noCallables))
    val projectionIncomingOpt = expected.expectedCharacteristics.collectFirst {
      case pi: ProjectionIncoming => pi
    }
    val patternIncoming = expected.expectedCharacteristics.collectFirst {
      case pi: PatternIncoming => pi
    }.getOrElse(PatternIncoming(unit, unit, unit, noCallables))
    val referenced = expected.expectedCharacteristics.collectFirst {
      case Referenced(refs) => refs
    }.getOrElse(unit)
    val referencesTo = expected.expectedCharacteristics.collectFirst {
      case ReferencesTo(pairs) => pairs
    }
    val referencesToAt = expected.expectedCharacteristics.collectFirst {
      case ReferencesToAt(pairs) => pairs
    }
    val declared = expected.expectedCharacteristics.collectFirst {
      case d: Declared => d
    }.getOrElse(Declared(Seq.empty, Seq.empty, Seq.empty))
    val outgoing = expected.expectedCharacteristics.collectFirst {
      case o: Outgoing => o
    }.getOrElse(ws.astNode match {
      // case _: Expression | _: LabelExpression => Outgoing(unit, unit)
      case _ => Outgoing(unit, unit, noCallables)
    })
    val result = expected.expectedCharacteristics.collectFirst {
      case r: ExpectedResult => r
    }.getOrElse(ws.astNode match {
      case _: Expression | _: LabelExpression => ExpectedResult.ExpressionResult
      case _                                  => ExpectedResult.NoResult
    })
    val children = expected.expectedCharacteristics.collect {
      case c: ExpectedWorkingScope => c
    }
    val inImportingWith = expected.expectedCharacteristics.collectFirst {
      case i: InImportingWith => i
    }.getOrElse(InImportingWith(false))

    def assertResult(actualResult: Result, expectedResult: ExpectedResult): Assertion = {
      (actualResult, result) match {
        case (NoResult, ExpectedResult.NoResult)                                                   => succeed
        case (ExpressionResult, ExpectedResult.ExpressionResult)                                   => succeed
        case (OmittedResult, ExpectedResult.OmittedResult)                                         => succeed
        case (TableResultWithNotYetKnownColumns, ExpectedResult.TableResultWithNotYetKnownColumns) => succeed
        case (TableResult(columns), ExpectedResult.TableResult(expectedColumns @ _*)) =>
          columns.map(_.name) should contain theSameElementsAs expectedColumns
        case (actual, exp) => actual shouldBe exp
      }
    }

    def assertLocalCallableSet(
      actualCallables: Set[LocalCallableScopeSignature],
      expectedCallables: Set[Callable]
    ): Unit = {
      assertLocalCallableSeq(
        actualCallables.toSeq.sortBy(c => (c.name.namespace.parts :+ c.name.name).mkString("`", "`.`", "`")),
        expectedCallables.toSeq.sortBy(c => (c._namespace :+ c._name).mkString("`", "`.`", "`"))
      )
    }

    def assertLocalCallableSeq(
      actualCallables: Seq[LocalCallableScopeSignature],
      expectedCallables: Seq[Callable]
    ): Unit = {
      withClue("[number]") {
        actualCallables.size shouldBe expectedCallables.size
      }
      (actualCallables zip expectedCallables) foreach {
        case (actualCallable, expectedCallable) => assertLocalCallable(actualCallable, expectedCallable)
      }
    }

    def assertLocalCallable(actualCallable: LocalCallableScopeSignature, expectedCallable: Callable): Assertion = {
      withClue(s"[callable ${expectedCallable.name}]") {
        actualCallable.name.namespace.parts should contain theSameElementsAs expectedCallable._namespace
        actualCallable.name.name shouldBe expectedCallable._name
      }
    }

    /**
     *  Assert the contents of a `ProjectionExpressionContext`.
     *  Grouping Keys can be recognized by either their alias or expression.
     */
    def assertProjectionIncoming(
      actualConstants: Set[LogicalVariable],
      actualVariables: Set[LogicalVariable],
      actualLocalCallables: Set[LocalCallableScopeSignature],
      specification: ProjectionSpecification
    ): Unit = {
      val stringifier = ExpressionStringifier()

      projectionIncomingOpt match {
        case Some(pi) =>
          withClue("[constants]") {
            actualConstants.map(_.name) should contain theSameElementsAs pi.constants
          }
          withClue("[variables]") {
            actualVariables.map(_.name) should contain theSameElementsAs pi.variables
          }
          withClue("[groupingKeys]") {
            val actualGroupingKeys = specification.groupingKeys.map { gk =>
              val exprText = stringifier(gk.expression)
              ExpectedGroupingKey(
                name = gk.alias.map(_.name).getOrElse(exprText),
                expression = Some(exprText)
              )
            }
            val matched = pi.groupingKeys.flatMap { expected =>
              actualGroupingKeys.find { actual =>
                actual.name == expected.name &&
                expected.expression.forall(e => actual.expression.contains(e))
              }
            }
            withClue(s"actual grouping keys: $actualGroupingKeys") {
              matched.size shouldBe pi.groupingKeys.size
              actualGroupingKeys.size shouldBe pi.groupingKeys.size
            }
          }
          withClue("[callables]") {
            assertLocalCallableSet(actualLocalCallables, pi.localCallables)
          }
          withClue("[invariance]") {
            (actualConstants.map(_.name) intersect actualVariables.map(_.name)) shouldBe empty
          }
        case None =>
          withClue("[constants]") {
            actualConstants.map(_.name) should contain theSameElementsAs incoming.constants
          }
          withClue("[variables]") {
            actualVariables.map(_.name) should contain theSameElementsAs incoming.variables
          }
          withClue("[callables]") {
            assertLocalCallableSet(actualLocalCallables, incoming.localCallables)
          }
          withClue("[invariance]") {
            (actualConstants.map(_.name) intersect actualVariables.map(_.name)) shouldBe empty
          }
      }
    }

    withClue(s"['${prettify(ws.astNode)}']") {
      withClue("[query]") {
        whitespaceNormalization(prettify(ws.astNode)) shouldBe whitespaceNormalization(astNodeString)
      }
      ws match {
        case StatementScope(
            _,
            CommonContext(constants, variables, localCallables),
            _,
            _,
            _,
            _,
            _,
            isInImportingWith
          ) =>
          withClue("[statement incoming]") {
            withClue("[constants]") {
              constants.map(_.name) should contain theSameElementsAs incoming.constants
            }
            withClue("[variables]") {
              variables.map(_.name) should contain theSameElementsAs incoming.variables
            }
            withClue("[invariance]") {
              (constants.map(_.name) intersect variables.map(_.name)) shouldBe empty
            }
            withClue("[callables]") {
              assertLocalCallableSet(localCallables, incoming.localCallables)
            }
          }
          withClue("[statement inImportingWith") {
            isInImportingWith shouldEqual inImportingWith.value
          }
        case StatementScope(
            _,
            ProjectionExpressionContext(constants, variables, localCallables, specification, _),
            _,
            _,
            _,
            _,
            _,
            isInImportingWith
          ) =>
          withClue("[statement projection incoming]") {
            assertProjectionIncoming(constants, variables, localCallables, specification)
          }
          withClue("[statement inImportingWith") {
            isInImportingWith shouldEqual inImportingWith.value
          }
        case PatternScope(_, PatternIncomingContext(topology, predicate, path, _, localCallables), _, _, _, _) =>
          withClue("[pattern incoming]") {
            withClue("[topology]") {
              topology.map(_.name) should contain theSameElementsAs patternIncoming.topology
            }
            withClue("[predicate]") {
              predicate.map(_.name) should contain theSameElementsAs patternIncoming.predicate
            }
            withClue("[path]") {
              path.map(_.name) should contain theSameElementsAs patternIncoming.path
            }
            withClue("[callables]") {
              assertLocalCallableSet(localCallables, patternIncoming.localCallables)
            }
            withClue("[invariance]") {
              (topology.map(_.name) intersect path.map(_.name)) shouldBe empty
              (predicate.map(_.name) intersect path.map(_.name)) shouldBe empty
            }
          }
        case ExpressionScope(_, CommonContext(constants, variables, localCallables), _, _, _) =>
          withClue("[expression incoming]") {
            withClue("[constants]") {
              constants.map(_.name) should contain theSameElementsAs incoming.constants
            }
            withClue("[variables]") {
              variables.map(_.name) should contain theSameElementsAs incoming.variables
            }
            withClue("[callables]") {
              assertLocalCallableSet(localCallables, incoming.localCallables)
            }
            withClue("[invariance]") {
              (constants.map(_.name) intersect variables.map(_.name)) shouldBe empty
            }
          }
        case ExpressionScope(
            _,
            ProjectionExpressionContext(constants, variables, localCallables, specification, _),
            _,
            _,
            _
          ) =>
          withClue("[expression projection incoming]") {
            assertProjectionIncoming(constants, variables, localCallables, specification)
          }
        case s => fail(s"unexpected type of scope: ${pprint.apply(s)}")
      }
      withClue("[children]") {
        withClue("[number]") {
          ws.children.size shouldBe children.size
        }
        ws.children.zip(children).foreach {
          case (childWs, childExpected) => assertExpectation(childWs, childExpected)
        }
      }
      withClue("[referenced]") {
        withClue("[elements]") {
          ws.referenced.getVariables.map(_.name).toSet shouldEqual referenced
        }

        referencesTo.foreach { expectedPairs =>
          val actualPairs: Map[String, String] =
            ws.referenced.references.iterator.map {
              case (caller, decl) => caller.value.name -> decl.value.name
            }.toMap
          withClue("[caller -> target]") {
            actualPairs should contain theSameElementsAs expectedPairs
          }
        }

        referencesToAt.foreach { expectedPairs =>
          val actualPairs: Seq[(String, Int, String, Int)] =
            ws.referenced.references.iterator.map {
              case (caller, decl) =>
                (caller.value.name, caller.value.position.offset, decl.value.name, decl.value.position.offset)
            }.toSeq
          withClue("[caller -> target with positions]") {
            actualPairs should contain theSameElementsAs expectedPairs
          }
        }

      }
      withClue("[declared]") {
        withClue("[constants]") {
          ws.declared.constants.map(_.name) should contain theSameElementsInOrderAs declared.constants
        }
        withClue("[variables]") {
          ws.declared.variables.map(_.name) should contain theSameElementsInOrderAs declared.variables
        }
        withClue("[invariance]") {
          (ws.declared.constants.map(_.name) intersect ws.declared.variables.map(_.name)) shouldBe empty
        }
        withClue("[callables]") {
          assertLocalCallableSeq(ws.declared.localCallables, declared.localCallables)
        }
      }
      withClue("[outgoing]") {
        withClue("[constants]") {
          ws.outgoing.constants.map(_.name) should contain theSameElementsAs outgoing.constants
        }
        withClue("[variables]") {
          ws.outgoing.variables.map(_.name) should contain theSameElementsAs outgoing.variables
        }
        withClue("[invariance]") {
          (ws.outgoing.constants.map(_.name) intersect ws.outgoing.variables.map(_.name)) shouldBe empty
        }
        withClue("[callables]") {
          assertLocalCallableSet(ws.outgoing.localCallables, outgoing.localCallables)
        }
        ws.astNode match {
          case _: Return | _: Finish =>
            withClue("[RETURN invariance]") {
              withClue("[constants]") {
                ws.outgoing.constants shouldBe empty
              }
              withClue("[callables]") {
                ws.outgoing.localCallables shouldBe empty
              }
            }
          case _: Clause /* implicitly: if !_.isInstanceOf[Return] && !_.isInstanceOf[Finish] */ =>
            withClue("[non-RETURN clause callable invariance]") {
              ws.outgoing.localCallables shouldBe ws.incoming.localCallables
            }
          case _: LocalCallableDefinition =>
            withClue("[local callable definition invariance]") {
              withClue("[number: outgoing = incoming + 1]") {
                ws.outgoing.localCallables.size shouldBe ws.incoming.localCallables.size + 1
              }
              withClue("[outgoing superset of incoming]") {
                (ws.outgoing.localCallables intersect ws.incoming.localCallables) should contain theSameElementsAs ws.incoming.localCallables
              }
            }
          case _ => succeed
        }
      }
      withClue("[result]") {
        assertResult(ws.result, result)
        ws.astNode match {
          case _: Return =>
            withClue("[RETURN invariance]") {
              ws.result shouldBe a[TableResult]
            }
          case _ => ()
        }
      }
    }
  }

  /* beforeRewrite -fictional rewrite-> query
   *  (test name)
   *  e.g.  a + b                       a * b
   *          v                           v
   *        scope    -no influence->    scope
   */
  def doesNotInfluence(
    beforeRewrite: Statement,
    query: Statement,
    versions: Array[CypherVersion] = Array(CypherVersion.Cypher25)
  ): Unit = {
    versions.foreach(version => {
      val directlyEither = runStatement(query, version)
      val rewrittenEither = runStatementAndRewrittenStatement(beforeRewrite, query, version)
      (directlyEither, rewrittenEither) match {
        case (Left(stateDirectly), Right(errorsRewritten)) =>
          stateDirectly.maybeScopeState should not be empty
          val workingScopeDirectly = stateDirectly.maybeScopeState.get.workingScope
          fail(
            s"""Version: $version
               |Query:
               |
               |${prettify(query)}
               |
               |Query directly was successful, but query rewritten threw errors.
               |---
               |Query directly with working scope:
               |
               |${pprint.apply(workingScopeDirectly)}
               |---
               |Query rewritten with errors:
               |
               |${pprint.apply(errorsRewritten)}
               |---""".stripMargin
          )
        case (Right(errorsDirectly), Left(stateRewritten)) =>
          stateRewritten.maybeScopeState should not be empty
          val workingScopeAfter = stateRewritten.maybeScopeState.get.workingScope
          fail(
            s"""Version: $version
               |Query:
               |
               |${prettify(query)}
               |
               |Query directly threw errors, but query rewritten was successful.
               |---
               |Query directly with errors:
               |
               |${pprint.apply(errorsDirectly)}
               |---
               |Query rewritten with working scope:
               |
               |${pprint.apply(workingScopeAfter)}
               |---""".stripMargin
          )
        case (Right(errorsDirectly), Right(errorsRewritten)) =>
          errorsDirectly should contain theSameElementsAs errorsRewritten
        case (Left(stateDirectly), Left(stateRewritten)) =>
          stateDirectly.maybeScopeState should not be empty
          val workingScopeDirectly = stateDirectly.maybeScopeState.get.workingScope
          stateRewritten.maybeScopeState should not be empty
          val workingScopeRewritten = stateRewritten.maybeScopeState.get.workingScope
          if (workingScopeDirectly == workingScopeRewritten) succeed
          else
            fail(
              s"""Version: $version
                 |Query:
                 |
                 |${prettify(query)}
                 |
                 |Working scopes directly and rewritten are not the same
                 |
                 |Working scope directly:
                 |
                 |${pprint.apply(workingScopeDirectly)}
                 |---
                 |Working scope rewritten:
                 |
                 |${pprint.apply(workingScopeRewritten)}
                 |---""".stripMargin
            )
      }
    })
  }

  override def afterAll(): Unit = {
    if (testLog) {
      log.close()
    }
  }

  private def initialStateWithQuery(query: String): InitialState =
    InitialState(query, NoPlannerName, new AnonymousVariableNameGenerator)

  private def initialStateWithStatement(statement: Statement): InitialState =
    InitialState(
      prettify(statement),
      NoPlannerName,
      new AnonymousVariableNameGenerator,
      maybeStatement = Some(statement)
    )
}
