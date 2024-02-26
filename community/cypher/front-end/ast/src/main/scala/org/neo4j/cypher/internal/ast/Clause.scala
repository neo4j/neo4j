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
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.ast.ASTSlicingPhrase.checkExpressionIsStaticInt
import org.neo4j.cypher.internal.ast.Match.hintPrettifier
import org.neo4j.cypher.internal.ast.ReturnItems.ReturnVariables
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorFail
import org.neo4j.cypher.internal.ast.connectedComponents.RichConnectedComponent
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.PatternStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.ast.semantics.Scope
import org.neo4j.cypher.internal.ast.semantics.SemanticAnalysisTooling
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck.fromState
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck.success
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck.when
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckResult
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckable
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticErrorDef
import org.neo4j.cypher.internal.ast.semantics.SemanticExpressionCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticExpressionCheck.FilteringExpressions
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticPatternCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticPatternCheck.error
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.ast.semantics.TypeGenerator
import org.neo4j.cypher.internal.ast.semantics.iterableOnceSemanticChecking
import org.neo4j.cypher.internal.ast.semantics.optionSemanticChecking
import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.Contains
import org.neo4j.cypher.internal.expressions.EndsWith
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Expression.SemanticContext
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.HasMappableExpressions
import org.neo4j.cypher.internal.expressions.HasTypes
import org.neo4j.cypher.internal.expressions.In
import org.neo4j.cypher.internal.expressions.InequalityExpression
import org.neo4j.cypher.internal.expressions.IsNotNull
import org.neo4j.cypher.internal.expressions.LabelOrRelTypeName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.MatchMode.DifferentRelationships
import org.neo4j.cypher.internal.expressions.MatchMode.MatchMode
import org.neo4j.cypher.internal.expressions.MatchMode.RepeatableElements
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.NonPrefixedPatternPart
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.Or
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.ParenthesizedPath
import org.neo4j.cypher.internal.expressions.PathConcatenation
import org.neo4j.cypher.internal.expressions.PathPatternPart
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternElement
import org.neo4j.cypher.internal.expressions.PatternPart
import org.neo4j.cypher.internal.expressions.PatternPart.Selector
import org.neo4j.cypher.internal.expressions.ProcedureName
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.QuantifiedPath
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.ScopeExpression
import org.neo4j.cypher.internal.expressions.ShortestPathsPatternPart
import org.neo4j.cypher.internal.expressions.SimplePattern
import org.neo4j.cypher.internal.expressions.StartsWith
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.containsAggregate
import org.neo4j.cypher.internal.expressions.functions.Function.isIdFunction
import org.neo4j.cypher.internal.expressions.functions.GraphByElementId
import org.neo4j.cypher.internal.expressions.functions.GraphByName
import org.neo4j.cypher.internal.label_expressions.LabelExpression
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Disjunctions
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Leaf
import org.neo4j.cypher.internal.label_expressions.LabelExpressionPredicate
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.CartesianProductNotification
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.helpers.StringHelper.RichString
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTDateTime
import org.neo4j.cypher.internal.util.symbols.CTDuration
import org.neo4j.cypher.internal.util.symbols.CTFloat
import org.neo4j.cypher.internal.util.symbols.CTGraphRef
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTPath
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.CypherType

import scala.annotation.tailrec
import scala.collection.immutable.ListSet

sealed trait Clause extends ASTNode with SemanticCheckable with SemanticAnalysisTooling {
  def name: String

  def returnVariables: ReturnVariables = ReturnVariables.empty

  final override def semanticCheck: SemanticCheck =
    clauseSpecificSemanticCheck chain
      fromState(checkIfMixingLabelExpressionWithOldSyntax) chain
      when(shouldRunQPPChecks) {
        checkIfMixingLegacyVarLengthWithQPPs chain
          checkIfMixingLegacyShortestWithPathSelectorOrMatchMode
      }

  protected def shouldRunQPPChecks: Boolean = true

  private val stringifier = ExpressionStringifier()

  object SetExtractor {
    def unapplySeq[T](s: Set[T]): Option[Seq[T]] = Some(s.toSeq)
  }

  private def checkIfMixingLabelExpressionWithOldSyntax(
    state: SemanticState
  ): SemanticCheck = {

    sealed trait UsageContext
    case object Read extends UsageContext
    case object Write extends UsageContext
    case object ReadWrite extends UsageContext

    case class LegacyLabelExpression(labelExpression: LabelExpression) {
      def replacementString: String = {
        val isOrColon = if (labelExpression.containsIs) "IS " else ":"
        isOrColon + stringifier.stringifyLabelExpression(labelExpression.replaceColonSyntax)
      }

      def position: InputPosition = labelExpression.position
    }

    case class LabelExpressionsPartitions(
      legacy: ListSet[LegacyLabelExpression] = ListSet.empty,
      gpm: ListSet[LabelExpression] = ListSet.empty
    ) {
      def withLegacyExpression(labelExpression: LabelExpression): LabelExpressionsPartitions =
        copy(legacy = legacy + LegacyLabelExpression(labelExpression))

      def withGPMExpression(labelExpression: LabelExpression): LabelExpressionsPartitions =
        copy(gpm = gpm + labelExpression)

      def semanticCheck: SemanticCheck = when(legacy.nonEmpty && gpm.nonEmpty) {
        // we prefer the new way, so we will only error on the "legacy" expressions
        val maybeExplanation = legacy.map { ls =>
          (ls.replacementString, ls.position)
        } match {
          case SetExtractor() => None
          case SetExtractor((singleExpression, pos)) =>
            Some((s"This expression could be expressed as $singleExpression.", pos))

          case set: Set[(String, InputPosition)] =>
            // we report all errors on the first position as we will later on throw away everything but the first error.
            val replacement = set.map(_._1)
            Some((s"These expressions could be expressed as ${replacement.mkString(", ")}.", set.head._2))
        }
        maybeExplanation match {
          case Some((explanation, pos)) =>
            // We may have multiple conflicts, both with IS and with label expression symbols.
            // We just look at the first GPM label expression and decide what conflict we report
            // based on whether it contains IS.
            val conflictWithIS = gpm.head.containsIs
            SemanticError(
              if (conflictWithIS) s"Mixing the IS keyword with colon (':') between labels is not allowed. $explanation"
              else
                s"Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') between labels is not allowed. Please only use one set of symbols. $explanation",
              pos
            )
          case None => SemanticCheck.success
        }
      }
    }

    case class Acc(
      readPartitions: LabelExpressionsPartitions = LabelExpressionsPartitions(),
      writePartitions: LabelExpressionsPartitions = LabelExpressionsPartitions(),
      usage: UsageContext = Read
    ) {

      def inReadContext(): Acc = copy(usage = Read)
      def inWriteContext(): Acc = copy(usage = Write)
      def inReadWriteContext(): Acc = copy(usage = ReadWrite)

      private def withLegacyExpression(labelExpression: LabelExpression): Acc = usage match {
        case Read  => copy(readPartitions = readPartitions.withLegacyExpression(labelExpression))
        case Write => copy(writePartitions = writePartitions.withLegacyExpression(labelExpression))
        case ReadWrite => copy(
            readPartitions = readPartitions.withLegacyExpression(labelExpression),
            writePartitions = writePartitions.withLegacyExpression(labelExpression)
          )
      }

      private def withGPMExpression(labelExpression: LabelExpression): Acc = usage match {
        case Read  => copy(readPartitions = readPartitions.withGPMExpression(labelExpression))
        case Write => copy(writePartitions = writePartitions.withGPMExpression(labelExpression))
        case ReadWrite => copy(
            readPartitions = readPartitions.withGPMExpression(labelExpression),
            writePartitions = writePartitions.withGPMExpression(labelExpression)
          )
      }

      def sortLabelExpressionIntoPartition(
        labelExpression: LabelExpression,
        isNode: Boolean
      ): Acc = {
        val acc = if (labelExpression.containsIs) {
          // Only allowed in GPM
          withGPMExpression(labelExpression)
        } else this

        acc.sortLabelExpressionIntoPartitionIgnoringIs(labelExpression, isNode)
      }

      private def sortLabelExpressionIntoPartitionIgnoringIs(
        labelExpression: LabelExpression,
        isNode: Boolean
      ): Acc = {
        labelExpression match {
          case _: Leaf =>
            // A leaf is both GPM and legacy syntax.
            // Thus not adding to any partition.
            this
          case Disjunctions(children, _) if !isNode && children.forall(_.isInstanceOf[Leaf]) =>
            // The disjunction for relationships is both GPM and legacy syntax.
            // Thus not adding to any partition.
            this
          case x if isNode && x.containsGpmSpecificLabelExpression    => withGPMExpression(x)
          case x if !isNode && x.containsGpmSpecificRelTypeExpression => withGPMExpression(x)
          case x                                                      => withLegacyExpression(x)
        }
      }
    }

    val Acc(readPartitions, writePartitions, _) = this.folder.treeFold(Acc()) {

      // Depending on the clause, update the usage context

      case _: Merge => acc =>
          val newAcc = acc.inReadWriteContext()
          TraverseChildren(newAcc)

      case _: UpdateClause => acc =>
          val newAcc = acc.inWriteContext()
          TraverseChildren(newAcc)

      case _: Clause => acc =>
          val newAcc = acc.inReadContext()
          TraverseChildren(newAcc)

      // Partition label expressions into legacy and gpm.

      case NodePattern(_, Some(le), _, _) => acc =>
          val partition =
            acc.sortLabelExpressionIntoPartition(le, isNode = true)
          TraverseChildren(partition)

      case LabelExpressionPredicate(entity, le) => acc =>
          val isNode = state.expressionType(entity).specified == CTNode.invariant
          val partition = Function.chain[Acc](Seq(
            _.inReadContext(),
            _.sortLabelExpressionIntoPartition(le, isNode = isNode)
          ))(acc)
          SkipChildren(partition)

      case RelationshipPattern(_, Some(le), _, _, _, _) => acc =>
          val partition =
            acc.sortLabelExpressionIntoPartition(le, isNode = false)
          TraverseChildren(partition)
    }

    readPartitions.semanticCheck chain
      writePartitions.semanticCheck
  }

  private def checkIfMixingLegacyVarLengthWithQPPs: SemanticCheck = {
    val legacyVarLengthRelationships = this.folder.treeFold(Seq.empty[RelationshipPattern]) {
      case r @ RelationshipPattern(_, _, Some(_), _, _, _) => acc =>
          TraverseChildren(acc :+ r)
        // We should traverse into subqeries to implement CIP-40 correctly.
        // We don't, because changing this would break backwards compatibility.
      // See the "GPM Sync Rolling Agenda" notes for Nov 23, 2023
      case _: SubqueryCall | _: FullSubqueryExpression => acc => SkipChildren(acc)
    }
    val hasQPP = this.folder.treeFold(false) {
      case _: QuantifiedPath => _ =>
          SkipChildren(true)
        // We should traverse into subqeries to implement CIP-40 correctly.
        // We don't, because changing this would break backwards compatibility.
      // See the "GPM Sync Rolling Agenda" notes for Nov 23, 2023
      case _: SubqueryCall | _: FullSubqueryExpression => acc => SkipChildren(acc)
      case _                                           => acc => if (acc) SkipChildren(acc) else TraverseChildren(acc)
    }

    when(hasQPP) {
      legacyVarLengthRelationships.foldSemanticCheck { legacyVarLengthRelationship =>
        error(
          "Mixing variable-length relationships ('-[*]-') with quantified relationships ('()-->*()') or quantified path patterns ('(()-->())*') is not allowed.",
          legacyVarLengthRelationship.position
        )
      }
    }
  }

  private def checkIfMixingLegacyShortestWithPathSelectorOrMatchMode: SemanticCheck = {
    val legacyShortest = this.folder.findAllByClass[ShortestPathsPatternPart]

    val hasPathSelectorOrMatchMode = this.folder.treeFold(false) {
      case s: Selector if s.isBounded   => _ => SkipChildren(true)
      case DifferentRelationships(true) =>
        // Allow implicit match mode
        acc => if (acc) SkipChildren(acc) else TraverseChildren(acc)
      case _: MatchMode =>
        // Forbid explicit match mode
        _ => SkipChildren(true)
      case _ => acc => if (acc) SkipChildren(acc) else TraverseChildren(acc)
    }

    when(hasPathSelectorOrMatchMode) {
      legacyShortest.foldSemanticCheck { legacyVarLengthRelationship =>
        error(
          "Mixing shortestPath/allShortestPaths with path selectors (e.g. 'ANY SHORTEST') or explicit match modes ('e.g. DIFFERENT RELATIONSHIPS') is not allowed.",
          legacyVarLengthRelationship.position
        )
      }
    }
  }

  def clauseSpecificSemanticCheck: SemanticCheck
}

sealed trait UpdateClause extends Clause with HasMappableExpressions[UpdateClause] {
  override def returnVariables: ReturnVariables = ReturnVariables.empty

  protected def mixingIsWithMultipleLabelsMessage(statement: String, replacement: String): String = {
    s"It is not supported to use the `IS` keyword together with multiple labels in `$statement`. Rewrite the expression as `$replacement`."
  }
}

sealed trait CreateOrInsert extends UpdateClause {
  def pattern: Pattern.ForUpdate
}

case class LoadCSV(
  withHeaders: Boolean,
  urlString: Expression,
  variable: Variable,
  fieldTerminator: Option[StringLiteral]
)(val position: InputPosition) extends Clause with SemanticAnalysisTooling {
  override def name: String = "LOAD CSV"

  override def clauseSpecificSemanticCheck: SemanticCheck =
    SemanticExpressionCheck.simple(urlString) chain
      expectType(CTString.covariant, urlString) chain
      checkFieldTerminator chain
      typeCheck

  private def checkFieldTerminator: SemanticCheck = {
    fieldTerminator match {
      case Some(literal) if literal.value.length != 1 =>
        error("CSV field terminator can only be one character wide", literal.position)
      case _ => success
    }
  }

  private def typeCheck: SemanticCheck = {
    val typ =
      if (withHeaders)
        CTMap
      else
        CTList(CTString)

    declareVariable(variable, typ)
  }
}

object LoadCSV {
  private val FtpUserPassConnectionStringRegex = """[\w]+://.+:.+@.+""".r

  def isSensitiveUrl(url: String): Boolean = {
    FtpUserPassConnectionStringRegex.matches(url)
  }

  def fromUrl(
    withHeaders: Boolean,
    source: Expression,
    variable: Variable,
    fieldTerminator: Option[StringLiteral]
  )(position: InputPosition): LoadCSV = {
    val sensitiveSource = source match {
      case x: StringLiteral if isSensitiveUrl(x.value) => x.asSensitiveLiteral
      case x                                           => x
    }
    LoadCSV(withHeaders, sensitiveSource, variable, fieldTerminator)(position)
  }
}

case class InputDataStream(variables: Seq[Variable])(val position: InputPosition) extends Clause
    with SemanticAnalysisTooling {

  override def name: String = "INPUT DATA STREAM"

  override def clauseSpecificSemanticCheck: SemanticCheck =
    variables.foldSemanticCheck(v => declareVariable(v, types(v)))
}

sealed trait GraphSelection extends Clause with SemanticAnalysisTooling {
  def graphReference: GraphReference
}

final case class UseGraph(graphReference: GraphReference)(val position: InputPosition) extends GraphSelection
    with ClauseAllowedOnSystem {
  override def name = "USE GRAPH"

  override def clauseSpecificSemanticCheck: SemanticCheck =
    whenState(_.features(SemanticFeature.UseAsMultipleGraphsSelector))(
      thenBranch = checkDynamicGraphSelector,
      elseBranch = whenState(_.features(SemanticFeature.UseAsSingleGraphSelector))(
        // On clause level, this feature means that only static graph references are allowed
        thenBranch = checkStaticGraphSelector,
        elseBranch = unsupported()
      )
    )

  private def unsupported(): SemanticCheck = SemanticCheck.fromFunctionWithContext { (semanticState, context) =>
    SemanticCheckResult.error(semanticState, context.errorMessageProvider.createUseClauseUnsupportedError(), position)
  }

  private def checkDynamicGraphSelector: SemanticCheck =
    graphReference match {
      case gr: GraphFunctionReference => gr.checkFunctionCall chain checkExpressions(gr.functionInvocation.args)
      case _: GraphDirectReference    => success
    }

  private def checkExpressions(expressions: Seq[Expression]): SemanticCheck =
    expressions.foldSemanticCheck(expr =>
      SemanticExpressionCheck.check(Expression.SemanticContext.Results, expr)
    )

  private def checkStaticGraphSelector: SemanticCheck = {
    graphReference match {
      case graphReference: GraphDirectReference => checkTargetGraph(graphReference)
      case _: GraphFunctionReference =>
        SemanticCheck.fromFunctionWithContext { (semanticState, context) =>
          SemanticCheckResult.error(
            semanticState,
            context.errorMessageProvider.createDynamicGraphReferenceUnsupportedError(graphReference.print),
            position
          )
        }
    }
  }

  private def checkTargetGraph(graphReference: GraphDirectReference): SemanticCheck = {
    SemanticCheck.fromFunctionWithContext { (semanticState, context) =>
      semanticState.targetGraph match {
        case Some(existingTarget) =>
          if (existingTarget.equals(graphReference.catalogName)) {
            SemanticCheckResult.success(semanticState)
          } else {
            SemanticCheckResult.error(
              semanticState,
              context.errorMessageProvider.createMultipleGraphReferencesError(graphReference.print),
              position
            )
          }
        case None =>
          val newState = semanticState.recordTargetGraph(graphReference.catalogName)
          SemanticCheckResult.success(newState)
      }
    }
  }
}

sealed trait GraphReference extends Expression with SemanticCheckable {
  override def semanticCheck: SemanticCheck = success
  def print: String
  def dependencies: Set[LogicalVariable]
}

final case class GraphDirectReference(catalogName: CatalogName)(val position: InputPosition) extends GraphReference {
  override def print: String = catalogName.qualifiedNameString

  override def dependencies: Set[LogicalVariable] = Set.empty
  override def isConstantForQuery: Boolean = true
}

final case class GraphFunctionReference(functionInvocation: FunctionInvocation)(
  val position: InputPosition
) extends GraphReference with SemanticAnalysisTooling {
  override def print: String = ExpressionStringifier(_.asCanonicalStringVal).apply(functionInvocation)

  override def dependencies: Set[LogicalVariable] = functionInvocation.dependencies

  def checkFunctionCall: SemanticCheck = {
    functionInvocation.function match {
      case GraphByName      => success
      case GraphByElementId => success
      case _ =>
        SemanticCheck.error(SemanticError(
          s"Type mismatch: USE clause must be given a ${CTGraphRef.toString}. Use either the name or alias of a database or the graph functions `graph.byName` and `graph.byElementId`.",
          functionInvocation.position
        ))
    }
  }
  override def isConstantForQuery: Boolean = false
}

trait SingleRelTypeCheck {
  self: Clause =>

  protected def checkRelTypes(patternPart: PatternPart): SemanticCheck =
    patternPart match {
      case PathPatternPart(element) => checkRelTypes(element)
      case _                        => success
    }

  protected def checkRelTypes(pattern: Pattern): SemanticCheck =
    pattern.patternParts.foldSemanticCheck(checkRelTypes)

  private def checkRelTypes(patternElement: PatternElement): SemanticCheck = {
    patternElement match {
      case RelationshipChain(element, rel, _) =>
        checkRelTypes(rel) chain checkRelTypes(element)
      case _ => success
    }
  }

  protected def checkRelTypes(rel: RelationshipPattern): SemanticCheck =
    rel.labelExpression match {
      case None => SemanticError(
          s"Exactly one relationship type must be specified for ${self.name}. Did you forget to prefix your relationship type with a ':'?",
          rel.position
        )
      case Some(Leaf(RelTypeName(_), _)) => success
      case Some(other) =>
        val types = other.flatten.distinct
        val (maybePlain, exampleString) =
          if (types.size == 1) ("plain ", s"like `:${types.head.name}` ")
          else ("", "")
        SemanticError(
          s"A single ${maybePlain}relationship type ${exampleString}must be specified for ${self.name}",
          rel.position
        )
    }
}

object Match {
  protected val hintPrettifier: Prettifier = Prettifier(ExpressionStringifier())
}

case class Match(
  optional: Boolean,
  matchMode: MatchMode,
  pattern: Pattern.ForMatch,
  hints: Seq[UsingHint],
  where: Option[Where]
)(val position: InputPosition) extends Clause with SemanticAnalysisTooling {
  override def name = "MATCH"

  override def clauseSpecificSemanticCheck: SemanticCheck =
    noImplicitJoinsInQuantifiedPathPatterns chain
      SemanticPatternCheck.check(Pattern.SemanticContext.Match, pattern) ifOkChain {
        hints.semanticCheck chain
          uniqueHints chain
          checkMatchMode chain
          where.semanticCheck chain
          checkHints chain
          checkForCartesianProducts
      }

  /**
   * Ensure that the node and relationship variables defined inside the quantified path patterns contained in this MATCH clause do not form any implicit joins.
   * It must run before checking the pattern itself – as it relies on variables defined in previous clauses to pre-empt some of the errors.
   * It checks for three scenarios:
   *   - a variable is defined in two or more quantified path patterns inside this MATCH clause
   *   - a variable is defined in a quantified path pattern and in a non-quantified node or relationship pattern inside this MATCH clause
   *   - a variable is defined in a quantified path pattern inside this MATCH clause and also appears in a previous MATCH clause
   */
  private def noImplicitJoinsInQuantifiedPathPatterns: SemanticCheck =
    SemanticCheck.fromState { state =>
      val (quantifiedPaths, simplePatterns) = partitionPatternElements(pattern.patternParts.map(_.element).toList)

      val allVariablesInQuantifiedPaths: List[(LogicalVariable, QuantifiedPath)] =
        for {
          quantifiedPath <- quantifiedPaths
          variable <- quantifiedPath.allVariables
        } yield variable -> quantifiedPath

      val quantifiedPathsPerVariable: Map[LogicalVariable, List[QuantifiedPath]] =
        allVariablesInQuantifiedPaths.groupMap(_._1)(_._2)

      val allVariablesInSimplePatterns: Set[LogicalVariable] =
        simplePatterns.flatMap(_.allVariables).toSet

      val semanticErrors =
        quantifiedPathsPerVariable.flatMap { case (variable, paths) =>
          List(
            Option.when(paths.size > 1) {
              s"The variable `${variable.name}` occurs in multiple quantified path patterns and needs to be renamed."
            },
            Option.when(allVariablesInSimplePatterns.contains(variable)) {
              s"The variable `${variable.name}` occurs both inside and outside a quantified path pattern and needs to be renamed."
            },
            Option.when(state.symbol(variable.name).isDefined) {
              // Because one cannot refer to a variable defined in a subsequent clause, if the variable exists in the semantic state, then it must have been defined in a previous clause.
              s"The variable `${variable.name}` is already defined in a previous clause, it cannot be referenced as a node or as a relationship variable inside of a quantified path pattern."
            }
          ).flatten.map { errorMessage =>
            SemanticError(errorMessage, variable.position)
          }
        }

      SemanticCheck.error(semanticErrors)
    }

  /**
   * Recursively partition sub-elements into quantified paths and "simple" patterns (nodes and relationships).
   * @param patternElements the list of elements to break down and partition
   * @param quantifiedPaths accumulator for quantified paths
   * @param simplePatterns accumulator for simple patterns
   * @return the list of quantified paths and the list of simple patterns
   */
  @tailrec
  private def partitionPatternElements(
    patternElements: List[PatternElement],
    quantifiedPaths: List[QuantifiedPath] = Nil,
    simplePatterns: List[SimplePattern] = Nil
  ): (List[QuantifiedPath], List[SimplePattern]) =
    patternElements match {
      case Nil => (quantifiedPaths.reverse, simplePatterns.reverse)
      case element :: otherElements =>
        element match {
          case PathConcatenation(factors) =>
            partitionPatternElements(factors.toList ++ otherElements, quantifiedPaths, simplePatterns)
          case ParenthesizedPath(part, _) =>
            partitionPatternElements(part.element :: otherElements, quantifiedPaths, simplePatterns)
          case quantifiedPath: QuantifiedPath =>
            partitionPatternElements(otherElements, quantifiedPath :: quantifiedPaths, simplePatterns)
          case simplePattern: SimplePattern =>
            partitionPatternElements(otherElements, quantifiedPaths, simplePattern :: simplePatterns)
        }
    }

  private def uniqueHints: SemanticCheck = {
    val errors = hints.collect {
      case h: UsingJoinHint => h.variables.toIndexedSeq
    }.flatten
      .groupBy(identity)
      .collect {
        case (variable, identHints) if identHints.size > 1 =>
          SemanticError("Multiple join hints for same variable are not supported", variable.position)
      }.toVector

    (state: SemanticState) => semantics.SemanticCheckResult(state, errors)
  }

  private def checkForCartesianProducts: SemanticCheck = (state: SemanticState) => {
    val expressionStringifier = ExpressionStringifier(preferSingleQuotes = true)
    val patternStringifier = PatternStringifier(expressionStringifier)
    lazy val patternString =
      try {
        patternStringifier(pattern)
      } catch {
        case _: StackOverflowError => "<StackOverflowError>"
      }
    val cc = connectedComponents(pattern.patternParts)
    // if we have multiple connected components we will have
    // a cartesian product
    val newState = cc.drop(1).foldLeft(state) { (innerState, component) =>
      innerState.addNotification(CartesianProductNotification(position, component.variables.map(_.name), patternString))
    }

    semantics.SemanticCheckResult(newState, Seq.empty)
  }

  private def checkMatchMode: SemanticCheck = {
    whenState(!_.features.contains(SemanticFeature.MatchModes)) {
      matchMode match {
        case mode: DifferentRelationships if mode.implicitlyCreated => SemanticCheckResult.success(_)
        case _ => error(s"Match modes such as `${matchMode.prettified}` are not supported yet.", matchMode.position)
      }
    } ifOkChain {
      matchMode match {
        case _: RepeatableElements     => checkRepeatableElements(_)
        case _: DifferentRelationships => checkDifferentRelationships(_)
      }
    }
  }

  private def checkRepeatableElements(state: SemanticState): SemanticCheckResult = {
    val errors = pattern.patternParts.collect {
      case part if !part.isBounded =>
        SemanticError(
          "The pattern may yield an infinite number of rows under match mode REPEATABLE ELEMENTS, " +
            "perhaps use a path selector or add an upper bound to your quantified path patterns.",
          part.position
        )
    }
    semantics.SemanticCheckResult(state, errors)
  }

  /**
   * Iff we are operating under a DIFFERENT RELATIONSHIPS match mode, then a selective selector
   * (any other selector than ALL) would imply an order of evaluation of the different path patterns.
   * Therefore, once there is at least one path pattern with a selective selector, then we need to make sure
   * that there is no other path pattern beside it.
   */
  private def checkDifferentRelationships(state: SemanticState): SemanticCheckResult = {
    // Let's only mention match modes when that is an available feature
    def errorMessage: String = if (state.features.contains(SemanticFeature.MatchModes)) {
      "Multiple path patterns cannot be used in the same clause in combination with a selective path selector. " +
        "You may want to use multiple MATCH clauses, or you might want to consider using the REPEATABLE ELEMENTS match mode."
    } else {
      "Multiple path patterns cannot be used in the same clause in combination with a selective path selector."
    }

    val errors = if (pattern.patternParts.size > 1) {
      pattern.patternParts
        .find(_.isSelective)
        .map(selectivePattern =>
          SemanticError(
            errorMessage,
            selectivePattern.position
          )
        )
        .toSeq
    } else {
      Seq.empty
    }
    semantics.SemanticCheckResult(state, errors)
  }

  private def checkHints: SemanticCheck = SemanticCheck.fromFunctionWithContext { (semanticState, context) =>
    def getMissingEntityKindError(variable: String, labelOrRelTypeName: String, hint: NodeHint): String = {
      val isNode = semanticState.isNode(variable)
      val typeName = if (isNode) "label" else "relationship type"
      val functionName = if (isNode) "labels" else "type"
      val operatorDescription = hint match {
        case _: UsingIndexHint => "index"
        case _: UsingScanHint  => s"$typeName scan"
        case _: UsingJoinHint  => "join"
      }
      val typePredicates = getLabelAndRelTypePredicates(variable).distinct
      val foundTypePredicatesDescription = typePredicates match {
        case Seq()              => s"no $typeName was"
        case Seq(typePredicate) => s"only the $typeName `$typePredicate` was"
        case typePredicates     => s"only the ${typeName}s `${typePredicates.mkString("`, `")}` were"
      }

      getHintErrorForVariable(
        operatorDescription,
        hint,
        s"$typeName `$labelOrRelTypeName`",
        foundTypePredicatesDescription,
        variable,
        s"""Predicates must include the $typeName literal `$labelOrRelTypeName`.
            | That is, the function `$functionName()` is not compatible with indexes.""".stripLinesAndMargins
      )
    }

    def getMissingPropertyError(hint: UsingIndexHint): String = {
      val variable = hint.variable.name
      val propertiesInHint = hint.properties
      val plural = propertiesInHint.size > 1
      val foundPropertiesDescription = getPropertyPredicates(variable) match {
        case Seq()         => "none was"
        case Seq(property) => s"only `$property` was"
        case properties    => s"only `${properties.mkString("`, `")}` were"
      }
      val missingPropertiesNames = propertiesInHint.map(prop => s"`${prop.name}`").mkString(", ")
      val missingPropertiesDescription = s"the ${if (plural) "properties" else "property"} $missingPropertiesNames"

      getHintErrorForVariable(
        "index",
        hint,
        missingPropertiesDescription,
        foundPropertiesDescription,
        variable,
        """Supported predicates are:
          | equality comparison, inequality (range) comparison, `STARTS WITH`,
          | `IN` condition or checking property existence.
          | The comparison cannot be performed between two property values.""".stripLinesAndMargins
      )
    }

    def getHintErrorForVariable(
      operatorDescription: String,
      hint: NodeHint,
      missingThingDescription: String,
      foundThingsDescription: String,
      variable: String,
      additionalInfo: String
    ): String = {
      val isNode = semanticState.isNode(variable)
      val entityName = if (isNode) "node" else "relationship"

      getHintError(
        operatorDescription,
        hint,
        missingThingDescription,
        foundThingsDescription,
        s"the $entityName `$variable`",
        entityName,
        additionalInfo
      )
    }

    def getHintError(
      operatorDescription: String,
      hint: NodeHint,
      missingThingDescription: String,
      foundThingsDescription: String,
      entityDescription: String,
      entityName: String,
      additionalInfo: String
    ): String = {
      context.errorMessageProvider.createMissingPropertyLabelHintError(
        operatorDescription,
        hintPrettifier.asString(hint),
        missingThingDescription,
        foundThingsDescription,
        entityDescription,
        entityName,
        additionalInfo
      )
    }

    val error: Option[SemanticErrorDef] = hints.collectFirst {
      case hint @ UsingIndexHint(Variable(variable), LabelOrRelTypeName(labelOrRelTypeName), _, _, _)
        if !containsLabelOrRelTypePredicate(variable, labelOrRelTypeName) =>
        SemanticError(getMissingEntityKindError(variable, labelOrRelTypeName, hint), hint.position)
      case hint @ UsingIndexHint(Variable(variable), LabelOrRelTypeName(_), properties, _, _)
        if !containsPropertyPredicates(variable, properties) =>
        SemanticError(getMissingPropertyError(hint), hint.position)
      case hint @ UsingScanHint(Variable(variable), LabelOrRelTypeName(labelOrRelTypeName))
        if !containsLabelOrRelTypePredicate(variable, labelOrRelTypeName) =>
        SemanticError(getMissingEntityKindError(variable, labelOrRelTypeName, hint), hint.position)
      case hint @ UsingJoinHint(_) if pattern.length == 0 =>
        SemanticError("Cannot use join hint for single node pattern.", hint.position)
    }
    SemanticCheckResult(semanticState, error.toSeq)
  }

  private[ast] def containsPropertyPredicates(variable: String, propertiesInHint: Seq[PropertyKeyName]): Boolean = {
    val propertiesInPredicates: Seq[String] = getPropertyPredicates(variable)

    propertiesInHint.forall(p => propertiesInPredicates.contains(p.name))
  }

  private def getPropertyPredicates(variable: String): Seq[String] = {
    where.map(w => collectPropertiesInPredicates(variable, w.expression)).getOrElse(Seq.empty[String]) ++
      pattern.folder.treeFold(Seq.empty[String]) {
        case NodePattern(Some(Variable(id)), _, properties, predicate) if variable == id =>
          acc =>
            SkipChildren(acc ++ collectPropertiesInPropertyMap(properties) ++ predicate.map(
              collectPropertiesInPredicates(variable, _)
            ).getOrElse(Seq.empty[String]))
        case RelationshipPattern(Some(Variable(id)), _, _, properties, predicate, _) if variable == id =>
          acc =>
            SkipChildren(acc ++ collectPropertiesInPropertyMap(properties) ++ predicate.map(
              collectPropertiesInPredicates(variable, _)
            ).getOrElse(Seq.empty[String]))
      }
  }

  private def collectPropertiesInPropertyMap(properties: Option[Expression]): Seq[String] =
    properties match {
      case Some(MapExpression(prop)) => prop.map(_._1.name)
      case _                         => Seq.empty[String]
    }

  private def collectPropertiesInPredicates(variable: String, whereExpression: Expression): Seq[String] =
    whereExpression.folder.treeFold(Seq.empty[String]) {
      case Equals(Property(Variable(`variable`), PropertyKeyName(name)), other) if applicable(other) =>
        acc => SkipChildren(acc :+ name)
      case Equals(other, Property(Variable(`variable`), PropertyKeyName(name))) if applicable(other) =>
        acc => SkipChildren(acc :+ name)
      case In(Property(Variable(`variable`), PropertyKeyName(name)), _) =>
        acc => SkipChildren(acc :+ name)
      case IsNotNull(Property(Variable(`variable`), PropertyKeyName(name))) =>
        acc => SkipChildren(acc :+ name)
      case IsTyped(Property(Variable(`variable`), PropertyKeyName(name)), typeName) if !typeName.isNullable =>
        acc => SkipChildren(acc :+ name)
      case IsNormalized(Property(Variable(`variable`), PropertyKeyName(name)), _) =>
        acc => SkipChildren(acc :+ name)
      case StartsWith(Property(Variable(`variable`), PropertyKeyName(name)), _) =>
        acc => SkipChildren(acc :+ name)
      case EndsWith(Property(Variable(`variable`), PropertyKeyName(name)), _) =>
        acc => SkipChildren(acc :+ name)
      case Contains(Property(Variable(`variable`), PropertyKeyName(name)), _) =>
        acc => SkipChildren(acc :+ name)
      case FunctionInvocation(
          Namespace(List(namespace)),
          FunctionName(functionName),
          _,
          Seq(Property(Variable(`variable`), PropertyKeyName(name)), _, _),
          _,
          _
        ) if namespace.equalsIgnoreCase("point") && functionName.equalsIgnoreCase("withinBBox") =>
        acc => SkipChildren(acc :+ name)
      case expr: InequalityExpression =>
        acc =>
          val newAcc: Seq[String] = Seq(expr.lhs, expr.rhs).foldLeft(acc) { (acc, expr) =>
            expr match {
              case Property(Variable(`variable`), PropertyKeyName(name)) =>
                acc :+ name
              case FunctionInvocation(
                  Namespace(List(namespace)),
                  FunctionName(functionName),
                  _,
                  Seq(Property(Variable(id), PropertyKeyName(name)), _),
                  _,
                  _
                )
                if id == variable && namespace.equalsIgnoreCase("point") && functionName.equalsIgnoreCase("distance") =>
                acc :+ name
              case _ =>
                acc
            }
          }
          SkipChildren(newAcc)
      case _: Where | _: And | _: Ands | _: Set[_] | _: Seq[_] | _: Or | _: Ors | _: Not =>
        acc => TraverseChildren(acc)
      case _ =>
        acc => SkipChildren(acc)
    }

  /**
   * Checks validity of the other side, X, of expressions such as
   * `USING INDEX ON n:Label(prop) WHERE n.prop = X (or X = n.prop)`
   *
   * Returns true if X is a valid expression in this context, otherwise false.
   */
  private def applicable(other: Expression): Boolean = {
    other match {
      case f: FunctionInvocation => !isIdFunction(f)
      case _                     => true
    }
  }

  private[ast] def containsLabelOrRelTypePredicate(variable: String, labelOrRelType: String): Boolean =
    getLabelAndRelTypePredicates(variable).contains(labelOrRelType)

  private def getLabelsFromLabelExpression(labelExpression: LabelExpression) = {
    labelExpression.flatten.map(_.name)
  }

  private def getLabelAndRelTypePredicates(variable: String): Seq[String] = {
    val inlinedRelTypes = pattern.folder.fold(Seq.empty[String]) {
      case RelationshipPattern(Some(Variable(id)), Some(labelExpression), _, _, _, _) if variable == id =>
        list => list ++ getLabelsFromLabelExpression(labelExpression)
    }

    val labelExpressionLabels: Seq[String] = pattern.folder.fold(Seq.empty[String]) {
      case NodePattern(Some(Variable(id)), Some(labelExpression), _, _) if variable == id =>
        list => list ++ getLabelsFromLabelExpression(labelExpression)
    }

    val (predicateLabels, predicateRelTypes) = where match {
      case Some(innerWhere) => innerWhere.folder.treeFold((Seq.empty[String], Seq.empty[String])) {
          // These are predicates from the match pattern that were rewritten
          case HasLabels(Variable(id), predicateLabels) if id == variable => {
            case (ls, rs) => SkipChildren((ls ++ predicateLabels.map(_.name), rs))
          }
          case HasTypes(Variable(id), predicateRelTypes) if id == variable => {
            case (ls, rs) => SkipChildren((ls, rs ++ predicateRelTypes.map(_.name)))
          }
          // These are predicates in the where clause that have not been rewritten yet.
          case LabelExpressionPredicate(Variable(id), labelExpression) if id == variable => {
            case (ls, rs) =>
              val labelOrRelTypes = getLabelsFromLabelExpression(labelExpression)
              SkipChildren((ls ++ labelOrRelTypes, rs ++ labelOrRelTypes))
          }
          case _: Where | _: And | _: Ands | _: Set[_] | _: Seq[_] | _: Or | _: Ors =>
            acc => TraverseChildren(acc)
          case _ =>
            acc => SkipChildren(acc)
        }
      case None => (Seq.empty, Seq.empty)
    }

    val allLabels = labelExpressionLabels ++ predicateLabels
    val allRelTypes = inlinedRelTypes ++ predicateRelTypes
    allLabels ++ allRelTypes
  }

  def allExportedVariables: Set[LogicalVariable] = pattern.patternParts.folder.treeFold(Set.empty[LogicalVariable]) {
    case _: ScopeExpression          => acc => SkipChildren(acc)
    case logicalVar: LogicalVariable => acc => TraverseChildren(acc ++ Set(logicalVar))
  }
}

case class Merge(pattern: NonPrefixedPatternPart, actions: Seq[MergeAction], where: Option[Where] = None)(
  val position: InputPosition
) extends UpdateClause with SingleRelTypeCheck {

  override def name = "MERGE"

  override protected def shouldRunQPPChecks: Boolean = false

  override def mapExpressions(f: Expression => Expression): UpdateClause =
    copy(pattern.mapExpressions(f), actions.map(_.mapExpressions(f)), where.map(_.mapExpressions(f)))(this.position)

  private def checkNoSubqueryInMerge: SemanticCheck = {
    val hasSubqueryExpression = Seq(pattern, actions).folder.treeCollect {
      case e: FullSubqueryExpression => e
    }

    hasSubqueryExpression match {
      case subquery +: _ =>
        SemanticCheck.error(SemanticError("Subquery expressions are not allowed in a MERGE clause.", subquery.position))
      case _ => success
    }
  }

  override def clauseSpecificSemanticCheck: SemanticCheck =
    SemanticPatternCheck.check(Pattern.SemanticContext.Merge, Pattern.ForUpdate(Seq(pattern))(pattern.position)) chain
      actions.semanticCheck chain
      checkRelTypes(pattern) chain
      where.semanticCheck chain
      SemanticCheck.fromState { state =>
        // Only check checkNoSubqueryInMerge the first time.
        // Afterwards we can have rewritten PatternComprehensions to COLLECT subqueries which would now fail this check.
        if (state.semanticCheckHasRunOnce) success
        else checkNoSubqueryInMerge
      }
}

case class Create(pattern: Pattern.ForUpdate)(val position: InputPosition) extends CreateOrInsert
    with SingleRelTypeCheck {
  override def name = "CREATE"

  override def clauseSpecificSemanticCheck: SemanticCheck =
    SemanticPatternCheck.check(Pattern.SemanticContext.Create, pattern) chain
      checkRelTypes(pattern) chain
      SemanticState.recordCurrentScope(pattern)

  override protected def shouldRunQPPChecks: Boolean = false

  override def mapExpressions(f: Expression => Expression): UpdateClause =
    copy(pattern.mapExpressions(f))(this.position)
}

case class Insert(pattern: Pattern.ForUpdate)(val position: InputPosition) extends CreateOrInsert
    with SingleRelTypeCheck {
  override def name = "INSERT"

  override def clauseSpecificSemanticCheck: SemanticCheck =
    SemanticPatternCheck.check(Pattern.SemanticContext.Insert, pattern) chain
      SemanticState.recordCurrentScope(pattern)

  override protected def shouldRunQPPChecks: Boolean = false

  override def mapExpressions(f: Expression => Expression): UpdateClause =
    copy(pattern.mapExpressions(f))(this.position)
}

case class SetClause(items: Seq[SetItem])(val position: InputPosition) extends UpdateClause {
  override def name = "SET"

  override def clauseSpecificSemanticCheck: SemanticCheck =
    items.semanticCheck chain fromState(checkIfMixingIsWithMultipleLabels)

  override def mapExpressions(f: Expression => Expression): UpdateClause =
    copy(items.map(_.mapExpressions(f)))(this.position)

  private def checkIfMixingIsWithMultipleLabels(state: SemanticState): SemanticCheck = {
    // Check for the IS keyword
    val containsIs = this.folder.treeExists {
      case _ @SetLabelItem(_, _, true) => true
    }

    // Check for multiple labels in the same item
    val multipleLabels = this.folder.treeExists {
      case _ @SetLabelItem(_, labels, _) if labels.size > 1 => true
    }

    // If both were present, throw error with improvement suggestion: n:A:B => n IS A, n IS B
    when(containsIs && multipleLabels) {
      val prettifier = Prettifier(ExpressionStringifier())
      val setItems: Seq[SetItem] = this.items.flatMap {
        case s @ SetLabelItem(variable, labels, _) =>
          labels.map(label => SetLabelItem(variable, Seq(label), containsIs = true)(s.position))
        case x => Seq(x)
      }
      val replacement = prettifier.prettifySetItems(setItems)
      SemanticError(mixingIsWithMultipleLabelsMessage(name, replacement), position)
    }
  }
}

case class Delete(expressions: Seq[Expression], forced: Boolean)(val position: InputPosition) extends UpdateClause {
  override def name = "DELETE"

  override def clauseSpecificSemanticCheck: SemanticCheck =
    SemanticExpressionCheck.simple(expressions) chain
      warnAboutDeletingLabels chain
      expectType(CTNode.covariant | CTRelationship.covariant | CTPath.covariant, expressions)

  private def warnAboutDeletingLabels =
    expressions.filter(e => e.isInstanceOf[LabelExpressionPredicate]) map {
      e => SemanticError("DELETE doesn't support removing labels from a node. Try REMOVE.", e.position)
    }

  override def mapExpressions(f: Expression => Expression): UpdateClause = copy(expressions.map(f))(this.position)
}

case class Remove(items: Seq[RemoveItem])(val position: InputPosition) extends UpdateClause {
  override def name = "REMOVE"

  override def clauseSpecificSemanticCheck: SemanticCheck =
    items.semanticCheck chain checkIfMixingIsWithMultipleLabels

  override def mapExpressions(f: Expression => Expression): UpdateClause =
    copy(items.map(_.mapExpressions(f)))(this.position)

  private def checkIfMixingIsWithMultipleLabels(): SemanticCheck = {
    // Check for the IS keyword
    val containsIs = this.folder.treeExists {
      case _ @RemoveLabelItem(_, _, true) => true
    }

    // Check for multiple labels in the same item
    val multipleLabels = this.folder.treeExists {
      case _ @RemoveLabelItem(_, labels, _) if labels.size > 1 => true
    }

    // If both were present, throw error with improvement suggestion: n:A:B => n IS A, n IS B
    when(containsIs && multipleLabels) {
      val prettifier = Prettifier(ExpressionStringifier())
      val removeItems: Seq[RemoveItem] = this.items.flatMap {
        case s @ RemoveLabelItem(variable, labels, _) =>
          labels.map(label => RemoveLabelItem(variable, Seq(label), containsIs = true)(s.position))
        case x => Seq(x)
      }
      val replacement = prettifier.prettifyRemoveItems(removeItems)
      SemanticError(mixingIsWithMultipleLabelsMessage(name, replacement), position)
    }
  }
}

case class Foreach(
  variable: Variable,
  expression: Expression,
  updates: Seq[Clause]
)(val position: InputPosition) extends UpdateClause {
  override def name = "FOREACH"

  override def mapExpressions(f: Expression => Expression): UpdateClause = {
    val mappedUpdates = updates.map {
      case uc: UpdateClause => uc.mapExpressions(f)
      case _                => throw new IllegalStateException("Foreach is expected to only have updating sub-clauses.")
    }
    copy(
      f(variable).asInstanceOf[Variable],
      f(expression),
      mappedUpdates
    )(this.position)
  }

  override def clauseSpecificSemanticCheck: SemanticCheck =
    SemanticExpressionCheck.simple(expression) chain
      expectType(CTList(CTAny).covariant, expression) chain
      updates.filter(!_.isInstanceOf[UpdateClause]).map(c =>
        SemanticError(s"Invalid use of ${c.name} inside FOREACH", c.position)
      ) ifOkChain
      withScopedState {
        val possibleInnerTypes: TypeGenerator = types(expression)(_).unwrapLists
        declareVariable(variable, possibleInnerTypes) chain updates.semanticCheck
      }
}

case class Unwind(
  expression: Expression,
  variable: Variable
)(val position: InputPosition) extends Clause with SemanticAnalysisTooling {
  override def name = "UNWIND"

  override def clauseSpecificSemanticCheck: SemanticCheck =
    SemanticExpressionCheck.check(SemanticContext.Results, expression) chain
      expectType(CTList(CTAny).covariant | CTAny.covariant, expression) ifOkChain
      FilteringExpressions.failIfAggregating(expression) chain {
        val possibleInnerTypes: TypeGenerator = types(expression)(_).unwrapPotentialLists
        declareVariable(variable, possibleInnerTypes)
      }
}

abstract class CallClause extends Clause {
  override def name = "CALL"

  def containsNoUpdates: Boolean

  def yieldAll: Boolean
}

case class UnresolvedCall(
  procedureNamespace: Namespace,
  procedureName: ProcedureName,
  // None: No arguments given
  declaredArguments: Option[Seq[Expression]] = None,
  // None: No results declared  (i.e. no "YIELD" part or "YIELD *")
  declaredResult: Option[ProcedureResult] = None,
  // YIELD *
  override val yieldAll: Boolean = false
)(val position: InputPosition) extends CallClause {

  override def returnVariables: ReturnVariables =
    ReturnVariables(
      includeExisting = false,
      declaredResult.map(_.items.map(_.variable).toList).getOrElse(List.empty)
    )

  override def clauseSpecificSemanticCheck: SemanticCheck = {
    val argumentCheck = declaredArguments.map(
      SemanticExpressionCheck.check(SemanticContext.Results, _)
    ).getOrElse(success)
    val resultsCheck = declaredResult.map(_.semanticCheck).getOrElse(success)
    val invalidExpressionsCheck = declaredArguments.map(_.map {
      case arg if arg.containsAggregate =>
        SemanticCheck.error(
          SemanticError(
            """Procedure call cannot take an aggregating function as argument, please add a 'WITH' to your statement.
              |For example:
              |    MATCH (n:Person) WITH collect(n.name) AS names CALL proc(names) YIELD value RETURN value""".stripMargin,
            position
          )
        )
      case _ => success
    }.foldLeft(success)(_ chain _)).getOrElse(success)

    argumentCheck chain resultsCheck chain invalidExpressionsCheck
  }

  // At this stage we can't know this, so we assume the CALL is non updating,
  // it should be rechecked when the call is resolved
  override def containsNoUpdates = true
}

sealed trait HorizonClause extends Clause with SemanticAnalysisTooling {
  override def clauseSpecificSemanticCheck: SemanticCheck = SemanticState.recordCurrentScope(this)

  def semanticCheckContinuation(previousScope: Scope, outerScope: Option[Scope] = None): SemanticCheck
}

object ProjectionClause {

  def unapply(arg: ProjectionClause)
    : Option[(Boolean, ReturnItems, Option[OrderBy], Option[Skip], Option[Limit], Option[Where])] = {
    arg match {
      case With(distinct, ri, orderBy, skip, limit, where, _) => Some((distinct, ri, orderBy, skip, limit, where))
      case Return(distinct, ri, orderBy, skip, limit, _, _)   => Some((distinct, ri, orderBy, skip, limit, None))
      case Yield(ri, orderBy, skip, limit, where)             => Some((false, ri, orderBy, skip, limit, where))
    }
  }

  def checkAliasedReturnItems(returnItems: ReturnItems, clauseName: String): SemanticState => Seq[SemanticError] =
    state =>
      returnItems match {
        case li: ReturnItems =>
          li.items.filter(item => item.alias.isEmpty).map(i =>
            SemanticError(s"Expression in $clauseName must be aliased (use AS)", i.position)
          )
        case _ => Seq()
      }
}

sealed trait ProjectionClause extends HorizonClause {
  def distinct: Boolean

  def returnItems: ReturnItems

  def orderBy: Option[OrderBy]

  def where: Option[Where]

  def skip: Option[Skip]

  def limit: Option[Limit]

  final def isWith: Boolean = !isReturn

  def isReturn: Boolean = false

  def name: String

  def copyProjection(
    distinct: Boolean = this.distinct,
    returnItems: ReturnItems = this.returnItems,
    orderBy: Option[OrderBy] = this.orderBy,
    skip: Option[Skip] = this.skip,
    limit: Option[Limit] = this.limit,
    where: Option[Where] = this.where
  ): ProjectionClause = {
    this match {
      case w: With   => w.copy(distinct, returnItems, orderBy, skip, limit, where)(this.position)
      case r: Return => r.copy(distinct, returnItems, orderBy, skip, limit, r.excludedNames)(this.position)
      case y: Yield  => y.copy(returnItems, orderBy, skip, limit, where)(this.position)
    }
  }

  /**
   * @return copy of this ProjectionClause with new return items
   */
  def withReturnItems(items: Seq[ReturnItem]): ProjectionClause

  override def clauseSpecificSemanticCheck: SemanticCheck =
    returnItems.semanticCheck

  override def semanticCheckContinuation(previousScope: Scope, outerScope: Option[Scope] = None): SemanticCheck =
    SemanticCheck.fromState {
      (state: SemanticState) =>
        /**
       * scopeToImportVariablesFrom will provide the scope to bring over only the variables that are needed from the
       * previous scope
       */
        def runChecks(scopeToImportVariablesFrom: Scope): SemanticCheck = {
          returnItems.declareVariables(scopeToImportVariablesFrom) chain
            orderBy.semanticCheck chain
            limit.semanticCheck chain
            skip.semanticCheck chain
            where.semanticCheck
        }

        // The two clauses ORDER BY and WHERE, following a WITH clause where there is no DISTINCT nor aggregation, have a special scope such that they
        // can see both variables from before the WITH and variables introduced by the WITH
        // (SKIP and LIMIT clauses are not allowed to access variables anyway, so they do not need to be included in this condition even when they are standalone)
        val specialScopeForSubClausesNeeded = orderBy.isDefined || where.isDefined
        val canSeePreviousScope =
          (!(returnItems.containsAggregate || distinct || isInstanceOf[Yield])) || returnItems.includeExisting

        val check: SemanticCheck =
          if (specialScopeForSubClausesNeeded && canSeePreviousScope) {
            /*
             * We have `WITH ... WHERE` or `WITH ... ORDER BY` with no aggregation nor distinct meaning we can
             *  see things from previous scopes when we are done here
             *  (incoming-scope)
             *        |      \
             *        |     (child scope) <-  semantic checking of `ORDER BY` and `WHERE` discarded, only used for errors
             *        |
             *  (outgoing-scope)
             *        |
             *       ...
             */

            for {
              // Special scope for ORDER BY and WHERE (SKIP and LIMIT are also checked in isolated scopes)
              _ <- SemanticCheck.setState(state.newChildScope)
              checksResult <- runChecks(previousScope)
              // New sibling scope for the WITH/RETURN clause itself and onwards.
              // Re-declare projected variables in the new scope since the sub-scope is discarded
              // (We do not need to check warnOnAccessToRestrictedVariableInOrderByOrWhere here since that only applies when we have distinct or aggregation)
              returnState <- SemanticCheck.setState(checksResult.state.popScope.newSiblingScope)
              finalResult <- returnItems.declareVariables(state.currentScope.scope)
            } yield {
              SemanticCheckResult(finalResult.state, checksResult.errors ++ finalResult.errors)
            }
          } else if (specialScopeForSubClausesNeeded) {
            /*
             *  We have `WITH ... WHERE` or `WITH ... ORDER BY` with an aggregation or a distinct meaning we cannot
             *  see things from previous scopes after the aggregation (or distinct).
             *
             *  (incoming-scope)
             *         |
             *  (outgoing-scope)
             *         |      \
             *         |      (child-scope) <- semantic checking of `ORDER BY` and `WHERE` discarded only used for errors
             *        ...
             */

            // Introduce a new sibling scope first, and then a new child scope from that one
            // this child scope is used for errors only and will later be discarded.
            val siblingState = state.newSiblingScope
            val stateForSubClauses = siblingState.newChildScope

            for {
              _ <- SemanticCheck.setState(stateForSubClauses)
              checksResult <- runChecks(previousScope)
              // By popping the scope we will discard the special scope used for subclauses
              returnResult <- SemanticCheck.setState(checksResult.state.popScope)
              // Re-declare projected variables in the new scope since the sub-scope is discarded
              finalResult <-
                returnItems.declareVariables(previousScope)
            } yield {
              // Re-declare projected variables in the new scope since the sub-scope is discarded
              val niceErrors = (checksResult.errors ++ finalResult.errors).map(
                warnOnAccessToRestrictedVariableInOrderByOrWhere(state.currentScope.symbolNames)
              )
              SemanticCheckResult(finalResult.state, niceErrors)
            }
          } else {
            for {
              _ <- SemanticCheck.setState(state.newSiblingScope)
              checksResult <- runChecks(previousScope)
            } yield {
              val niceErrors = checksResult.errors.map(
                warnOnAccessToRestrictedVariableInOrderByOrWhere(state.currentScope.symbolNames)
              )
              SemanticCheckResult(checksResult.state, niceErrors)

            }
          }

        (isReturn, outerScope) match {
          case (true, Some(outer)) => check.map { result =>
              val outerScopeSymbolNames = outer.symbolNames
              val outputSymbolNames = result.state.currentScope.scope.symbolNames
              val alreadyDeclaredNames = outputSymbolNames.intersect(outerScopeSymbolNames)
              val explicitReturnVariablesByName =
                returnItems.returnVariables.explicitVariables.map(v => v.name -> v).toMap
              val errors = alreadyDeclaredNames.map { name =>
                val position = explicitReturnVariablesByName.getOrElse(name, returnItems).position
                SemanticError(s"Variable `$name` already declared in outer scope", position)
              }

              SemanticCheckResult(result.state, result.errors ++ errors)
            }

          case _ =>
            check
        }
    }

  /**
   * If you access a previously defined variable in a WITH/RETURN with DISTINCT or aggregation, that is not OK. Example:
   * MATCH (a) RETURN sum(a.age) ORDER BY a.name
   *
   * This method takes the "Variable not defined" errors we get from the semantic analysis and provides a more helpful
   * error message
   * @param previousScopeVars all variables defined in the previous scope.
   * @param error the error
   * @return an error with a possibly better error message
   */
  private[ast] def warnOnAccessToRestrictedVariableInOrderByOrWhere(previousScopeVars: Set[String])(
    error: SemanticErrorDef
  ): SemanticErrorDef = {
    previousScopeVars.collectFirst {
      case name if error.msg.equals(s"Variable `$name` not defined") =>
        error.withMsg(
          s"In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access variables declared before the WITH/RETURN: $name"
        )
    }.getOrElse(error)
  }

  def verifyOrderByAggregationUse(fail: (String, InputPosition) => Nothing): Unit = {
    val aggregationInProjection = returnItems.containsAggregate
    val aggregationInOrderBy = orderBy.exists(_.sortItems.map(_.expression).exists(containsAggregate))
    if (!aggregationInProjection && aggregationInOrderBy)
      fail(s"Cannot use aggregation in ORDER BY if there are no aggregate expressions in the preceding $name", position)
  }
}

// used for SHOW/TERMINATE commands
sealed trait WithType
case object DefaultWith extends WithType
case object ParsedAsYield extends WithType
case object AddedInRewrite extends WithType

object With {

  def apply(returnItems: ReturnItems)(pos: InputPosition): With =
    With(distinct = false, returnItems, None, None, None, None)(pos)
}

case class With(
  distinct: Boolean,
  returnItems: ReturnItems,
  orderBy: Option[OrderBy],
  skip: Option[Skip],
  limit: Option[Limit],
  where: Option[Where],
  withType: WithType = DefaultWith
)(val position: InputPosition) extends ProjectionClause {

  override def name = "WITH"

  override def clauseSpecificSemanticCheck: SemanticCheck =
    super.clauseSpecificSemanticCheck chain
      ProjectionClause.checkAliasedReturnItems(returnItems, name) chain
      SemanticPatternCheck.checkValidPropertyKeyNamesInReturnItems(returnItems)

  override def withReturnItems(items: Seq[ReturnItem]): With =
    this.copy(returnItems = ReturnItems(returnItems.includeExisting, items)(returnItems.position))(this.position)
}

object Return {

  def apply(returnItems: ReturnItems)(pos: InputPosition): Return =
    Return(distinct = false, returnItems, None, None, None)(pos)
}

case class Return(
  distinct: Boolean,
  returnItems: ReturnItems,
  orderBy: Option[OrderBy],
  skip: Option[Skip],
  limit: Option[Limit],
  excludedNames: Set[String] = Set.empty,
  addedInRewrite: Boolean = false // used for SHOW/TERMINATE commands
)(val position: InputPosition) extends ProjectionClause with ClauseAllowedOnSystem {

  override def name = "RETURN"

  override def isReturn: Boolean = true

  override def where: Option[Where] = None

  override def returnVariables: ReturnVariables = returnItems.returnVariables

  override def clauseSpecificSemanticCheck: SemanticCheck =
    super.clauseSpecificSemanticCheck chain
      checkVariableScope chain
      ProjectionClause.checkAliasedReturnItems(returnItems, "CALL { RETURN ... }") chain
      SemanticPatternCheck.checkValidPropertyKeyNamesInReturnItems(returnItems)

  override def withReturnItems(items: Seq[ReturnItem]): Return =
    this.copy(returnItems = ReturnItems(returnItems.includeExisting, items)(returnItems.position))(this.position)

  def withReturnItems(returnItems: ReturnItems): Return =
    this.copy(returnItems = returnItems)(this.position)

  private def checkVariableScope: SemanticState => Seq[SemanticError] = s =>
    returnItems match {
      case ReturnItems(star, _, _) if star && s.currentScope.isEmpty =>
        Seq(SemanticError("RETURN * is not allowed when there are no variables in scope", position))
      case _ =>
        Seq.empty
    }
}

case class Yield(
  returnItems: ReturnItems,
  orderBy: Option[OrderBy],
  skip: Option[Skip],
  limit: Option[Limit],
  where: Option[Where]
)(val position: InputPosition) extends ProjectionClause with ClauseAllowedOnSystem {
  override def distinct: Boolean = false

  override def name: String = "YIELD"

  override def withReturnItems(items: Seq[ReturnItem]): Yield =
    this.copy(returnItems = ReturnItems(returnItems.includeExisting, items)(returnItems.position))(this.position)

  def withReturnItems(returnItems: ReturnItems): Yield =
    this.copy(returnItems = returnItems)(this.position)

  override def warnOnAccessToRestrictedVariableInOrderByOrWhere(previousScopeVars: Set[String])(error: SemanticErrorDef)
    : SemanticErrorDef = error
}

object SubqueryCall {

  final case class InTransactionsBatchParameters(batchSize: Expression)(val position: InputPosition) extends ASTNode
      with SemanticCheckable {

    override def semanticCheck: SemanticCheck =
      checkExpressionIsStaticInt(batchSize, "OF ... ROWS", acceptsZero = false)
  }

  final case class InTransactionsConcurrencyParameters(concurrency: Option[Expression])(val position: InputPosition)
      extends ASTNode
      with SemanticCheckable {

    override def semanticCheck: SemanticCheck = {
      if (concurrency.isEmpty) {
        return SemanticCheck.success
      }
      checkExpressionIsStaticInt(concurrency.get, "IN ... CONCURRENT", acceptsZero = false)
    }
  }

  final case class InTransactionsReportParameters(reportAs: LogicalVariable)(val position: InputPosition)
      extends ASTNode with SemanticCheckable with SemanticAnalysisTooling {

    override def semanticCheck: SemanticCheck =
      declareVariable(reportAs, CTMap) chain specifyType(CTMap, reportAs)
  }

  final case class InTransactionsErrorParameters(behaviour: InTransactionsOnErrorBehaviour)(
    val position: InputPosition
  ) extends ASTNode

  sealed trait InTransactionsOnErrorBehaviour

  object InTransactionsOnErrorBehaviour {
    case object OnErrorContinue extends InTransactionsOnErrorBehaviour
    case object OnErrorBreak extends InTransactionsOnErrorBehaviour
    case object OnErrorFail extends InTransactionsOnErrorBehaviour
  }

  final case class InTransactionsParameters(
    batchParams: Option[InTransactionsBatchParameters],
    concurrencyParams: Option[InTransactionsConcurrencyParameters],
    errorParams: Option[InTransactionsErrorParameters],
    reportParams: Option[InTransactionsReportParameters]
  )(val position: InputPosition) extends ASTNode with SemanticCheckable {

    override def semanticCheck: SemanticCheck = {
      val checkBatchParams = batchParams.foldSemanticCheck(_.semanticCheck)
      val checkConcurrencyParams = concurrencyParams.foldSemanticCheck(_.semanticCheck)
      val checkReportParams = reportParams.foldSemanticCheck(_.semanticCheck)

      val checkErrorReportCombination: SemanticCheck = (errorParams, reportParams) match {
        case (None, Some(reportParams)) =>
          error(
            "REPORT STATUS can only be used when specifying ON ERROR CONTINUE or ON ERROR BREAK",
            reportParams.position
          )
        case (Some(InTransactionsErrorParameters(OnErrorFail)), Some(reportParams)) =>
          error(
            "REPORT STATUS can only be used when specifying ON ERROR CONTINUE or ON ERROR BREAK",
            reportParams.position
          )
        case _ => SemanticCheck.success
      }

      checkBatchParams chain checkConcurrencyParams chain checkReportParams chain checkErrorReportCombination
    }
  }

  def isTransactionalSubquery(clause: SubqueryCall): Boolean = clause.inTransactionsParameters.isDefined

  def findTransactionalSubquery(node: ASTNode): Option[SubqueryCall] =
    node.folder.treeFind[SubqueryCall] { case s if isTransactionalSubquery(s) => true }
}

case class SubqueryCall(innerQuery: Query, inTransactionsParameters: Option[SubqueryCall.InTransactionsParameters])(
  val position: InputPosition
) extends HorizonClause with SemanticAnalysisTooling {

  override def name: String = "CALL"

  override def clauseSpecificSemanticCheck: SemanticCheck = {
    checkSubquery chain
      inTransactionsParameters.foldSemanticCheck {
        _.semanticCheck chain
          checkNoNestedCallInTransactions
      } chain
      checkNoCallInTransactionsInsideRegularCall
  }

  def reportParams: Option[SubqueryCall.InTransactionsReportParameters] =
    inTransactionsParameters.flatMap(_.reportParams)

  def checkSubquery: SemanticCheck = {
    for {
      outerStateWithImports <- innerQuery.checkImportingWith
      // Create empty scope under root
      _ <- SemanticCheck.setState(outerStateWithImports.state.newBaseScope)
      // Check inner query. Allow it to import from outer scope
      innerChecked <- innerQuery.semanticCheckInSubqueryContext(outerStateWithImports.state)
      _ <- returnToOuterScope(outerStateWithImports.state.currentScope)
      // Declare variables that are in output from subquery
      merged <- declareOutputVariablesInOuterScope(innerChecked.state.currentScope.scope)
    } yield {
      val importingWithErrors = outerStateWithImports.errors

      // Avoid double errors if inner has errors
      val allErrors = importingWithErrors ++
        (if (innerChecked.errors.nonEmpty) innerChecked.errors else merged.errors)

      // Keep errors from inner check and from variable declarations
      SemanticCheckResult(merged.state, allErrors)
    }
  }

  private def returnToOuterScope(outerScopeLocation: SemanticState.ScopeLocation): SemanticCheck = {
    SemanticCheck.fromFunction { innerState =>
      val innerCurrentScope = innerState.currentScope.scope

      // Keep working from the latest state
      val after: SemanticState = innerState
        // but jump back to scope tree of outerStateWithImports
        .copy(currentScope = outerScopeLocation)
        // Copy in the scope tree from inner query (needed for Namespacer)
        .insertSiblingScope(innerCurrentScope)
        // Import variables from scope before subquery
        .newSiblingScope
        .importValuesFromScope(outerScopeLocation.scope)

      SemanticCheckResult.success(after)
    }
  }

  override def semanticCheckContinuation(previousScope: Scope, outerScope: Option[Scope] = None): SemanticCheck = {
    (s: SemanticState) =>
      SemanticCheckResult(s.importValuesFromScope(previousScope), Vector())
  }

  private def declareOutputVariablesInOuterScope(rootScope: Scope): SemanticCheck = {
    when(innerQuery.isReturning) {
      val scopeForDeclaringVariables = innerQuery.finalScope(rootScope)
      declareVariables(scopeForDeclaringVariables.symbolTable.values)
    }
  }

  private def checkNoNestedCallInTransactions: SemanticCheck = {
    val nestedCallInTransactions = SubqueryCall.findTransactionalSubquery(innerQuery)
    nestedCallInTransactions.foldSemanticCheck { nestedCallInTransactions =>
      error("Nested CALL { ... } IN TRANSACTIONS is not supported", nestedCallInTransactions.position)
    }
  }

  private def checkNoCallInTransactionsInsideRegularCall: SemanticCheck = {
    val nestedCallInTransactions =
      if (inTransactionsParameters.isEmpty) {
        SubqueryCall.findTransactionalSubquery(innerQuery)
      } else
        None

    nestedCallInTransactions.foldSemanticCheck { nestedCallInTransactions =>
      error("CALL { ... } IN TRANSACTIONS nested in a regular CALL is not supported", nestedCallInTransactions.position)
    }
  }
}

// Show and terminate command clauses

sealed trait CommandClause extends Clause with SemanticAnalysisTooling {
  def unfilteredColumns: DefaultOrAllShowColumns

  // Yielded columns or yield *
  def yieldItems: List[CommandResultItem]
  def yieldAll: Boolean

  // Original columns before potential rename or filtering in YIELD
  protected def originalColumns: List[ShowAndTerminateColumn]

  // Used for semantic check
  private lazy val columnsAsMap: Map[String, CypherType] =
    originalColumns.map(column => column.name -> column.cypherType).toMap[String, CypherType]

  override def clauseSpecificSemanticCheck: SemanticCheck =
    if (yieldItems.nonEmpty) yieldItems.foldSemanticCheck(_.semanticCheck(columnsAsMap))
    else semanticCheckFold(unfilteredColumns.columns)(sc => declareVariable(sc.variable, sc.cypherType))

  def where: Option[Where]

  def moveWhereToProjection: CommandClause
}

object CommandClause {

  def unapply(cc: CommandClause): Option[(List[ShowColumn], Option[Where])] =
    Some((cc.unfilteredColumns.columns, cc.where))

  // Update variables in ORDER BY and WHERE to alias if renamed in YIELD
  // To allow YIELD x AS y ORDER BY x WHERE x = 'something' (which is allowed in WITH)
  // No need to update SKIP and LIMIT as those can only take integer values (for commands)
  // This method lives here to not need to be duplicated in Neo4jASTFactory and AstGenerator
  def updateAliasedVariablesFromYieldInOrderByAndWhere(yieldClause: Yield): (Option[OrderBy], Option[Where]) = {
    val returnAliasesMap = yieldClause.returnItems.items.map(ri => (ri.expression, ri.alias)).toMap

    def updateExpression(e: Expression): Expression = {
      returnAliasesMap.filter {
        // They should all be variables as that is what we allow parsing
        // Only need replacing if there is a differing replacement value
        // (parsing seems to add replacement even for unaliased columns (`YIELD x`))
        // (also skips replacing `YIELD x AS x`)
        // Avoid renaming variables if another variable has been renamed to the old name
        // (`YIELD x AS y, z AS x`)
        case (ov: LogicalVariable, Some(nv)) =>
          !nv.equals(ov) && !returnAliasesMap.valuesIterator.contains(Some(ov))
        case _ => false
      }.map {
        // we know the key is a LogicalVariable and that the value exists based on previous filtering
        case (key, value) => (key.asInstanceOf[LogicalVariable], value.get)
      }.foldLeft(e) {
        case (acc, (oldV, newV)) =>
          // Computed dependencies aren't available before semantic analysis
          acc.replaceAllOccurrencesBy(oldV, newV.copyId, skipExpressionsWithComputedDependencies = true)
      }
    }

    val orderBy = yieldClause.orderBy.map(ob => {
      val updatedSortItems = ob.sortItems.map(si => si.mapExpression(updateExpression))
      ob.copy(updatedSortItems)(ob.position)
    })
    val where = yieldClause.where.map(w => w.mapExpressions(updateExpression))

    (orderBy, where)
  }
}

// Yield columns: keeps track of the original name and the yield variable (either same name or renamed)
case class CommandResultItem(originalName: String, aliasedVariable: LogicalVariable)(val position: InputPosition)
    extends ASTNode with SemanticAnalysisTooling {

  def semanticCheck(columns: Map[String, CypherType]): SemanticCheck =
    columns
      .get(originalName)
      .map { typ => declareVariable(aliasedVariable, typ): SemanticCheck }
      .getOrElse(error(s"Trying to YIELD non-existing column: `$originalName`", position))
}

// Column name together with the column type
// Used to create the ShowColumns but without keeping variables
// (as having undeclared variables in the ast caused issues with namespacer)
case class ShowAndTerminateColumn(name: String, cypherType: CypherType = CTString)

// Command clauses which can take strings or string expressions
// For example, transaction ids or setting names
sealed trait CommandClauseWithNames extends CommandClause {
  // Either:
  // - a list of strings
  // - a single expression (resolving to a single string or a list of strings)
  def names: Either[List[String], Expression]

  // Semantic check:
  private def expressionCheck: SemanticCheck = names match {
    case Right(e) => SemanticExpressionCheck.simple(e)
    case _        => SemanticCheck.success
  }

  override def clauseSpecificSemanticCheck: SemanticCheck =
    expressionCheck chain super.clauseSpecificSemanticCheck
}

// For a query to be allowed to run on system it needs to consist of:
// - only ClauseAllowedOnSystem clauses (or the WITH that was parsed as YIELD/added in rewriter for transaction commands)
// - at least one CommandClauseAllowedOnSystem clause
sealed trait ClauseAllowedOnSystem
sealed trait CommandClauseAllowedOnSystem extends ClauseAllowedOnSystem

case class ShowIndexesClause(
  briefConstraintColumns: List[ShowAndTerminateColumn],
  allConstraintColumns: List[ShowAndTerminateColumn],
  indexType: ShowIndexType,
  brief: Boolean,
  verbose: Boolean,
  where: Option[Where],
  yieldItems: List[CommandResultItem],
  yieldAll: Boolean
)(val position: InputPosition) extends CommandClause {
  override def name: String = "SHOW INDEXES"

  private val useAllColumns = yieldItems.nonEmpty || yieldAll

  val originalColumns: List[ShowAndTerminateColumn] =
    if (useAllColumns) allConstraintColumns else briefConstraintColumns

  private val briefColumns = briefConstraintColumns.map(c => ShowColumn(c.name, c.cypherType)(position))
  private val allColumns = allConstraintColumns.map(c => ShowColumn(c.name, c.cypherType)(position))

  val unfilteredColumns: DefaultOrAllShowColumns =
    DefaultOrAllShowColumns(useAllColumns, briefColumns, allColumns)

  override def moveWhereToProjection: CommandClause = copy(where = None)(position)

  override def clauseSpecificSemanticCheck: SemanticCheck =
    if (brief || verbose)
      error(
        """`SHOW INDEXES` no longer allows the `BRIEF` and `VERBOSE` keywords,
          |please omit `BRIEF` and use `YIELD *` instead of `VERBOSE`.""".stripMargin,
        position
      )
    else if (indexType == BtreeIndexes) error("Invalid index type b-tree, please omit the `BTREE` filter.", position)
    else super.clauseSpecificSemanticCheck
}

object ShowIndexesClause {
  val idColumn = "id"
  val nameColumn = "name"
  val stateColumn = "state"
  val populationPercentColumn = "populationPercent"
  val typeColumn = "type"
  val entityTypeColumn = "entityType"
  val labelsOrTypesColumn = "labelsOrTypes"
  val propertiesColumn = "properties"
  val indexProviderColumn = "indexProvider"
  val owningConstraintColumn = "owningConstraint"
  val lastReadColumn = "lastRead"
  val readCountColumn = "readCount"
  val trackedSinceColumn = "trackedSince"
  val optionsColumn = "options"
  val failureMessageColumn = "failureMessage"
  val createStatementColumn = "createStatement"

  def apply(
    indexType: ShowIndexType,
    brief: Boolean,
    verbose: Boolean,
    where: Option[Where],
    yieldItems: List[CommandResultItem],
    yieldAll: Boolean
  )(position: InputPosition): ShowIndexesClause = {
    val briefCols = List(
      ShowAndTerminateColumn(idColumn, CTInteger),
      ShowAndTerminateColumn(nameColumn),
      ShowAndTerminateColumn(stateColumn),
      ShowAndTerminateColumn(populationPercentColumn, CTFloat),
      ShowAndTerminateColumn(typeColumn),
      ShowAndTerminateColumn(entityTypeColumn),
      ShowAndTerminateColumn(labelsOrTypesColumn, CTList(CTString)),
      ShowAndTerminateColumn(propertiesColumn, CTList(CTString)),
      ShowAndTerminateColumn(indexProviderColumn),
      ShowAndTerminateColumn(owningConstraintColumn),
      ShowAndTerminateColumn(lastReadColumn, CTDateTime),
      ShowAndTerminateColumn(readCountColumn, CTInteger)
    )
    val verboseCols = List(
      ShowAndTerminateColumn(trackedSinceColumn, CTDateTime),
      ShowAndTerminateColumn(optionsColumn, CTMap),
      ShowAndTerminateColumn(failureMessageColumn),
      ShowAndTerminateColumn(createStatementColumn)
    )

    ShowIndexesClause(
      briefCols,
      briefCols ++ verboseCols,
      indexType,
      brief,
      verbose,
      where,
      yieldItems,
      yieldAll
    )(position)
  }
}

case class ShowConstraintsClause(
  briefConstraintColumns: List[ShowAndTerminateColumn],
  allConstraintColumns: List[ShowAndTerminateColumn],
  constraintType: ShowConstraintType,
  brief: Boolean,
  verbose: Boolean,
  where: Option[Where],
  yieldItems: List[CommandResultItem],
  yieldAll: Boolean
)(val position: InputPosition) extends CommandClause {
  override def name: String = "SHOW CONSTRAINTS"

  private val useAllColumns = yieldItems.nonEmpty || yieldAll

  val originalColumns: List[ShowAndTerminateColumn] =
    if (useAllColumns) allConstraintColumns else briefConstraintColumns

  private val briefColumns = briefConstraintColumns.map(c => ShowColumn(c.name, c.cypherType)(position))
  private val allColumns = allConstraintColumns.map(c => ShowColumn(c.name, c.cypherType)(position))

  val unfilteredColumns: DefaultOrAllShowColumns =
    DefaultOrAllShowColumns(useAllColumns, briefColumns, allColumns)

  override def moveWhereToProjection: CommandClause = copy(where = None)(position)

  val existsErrorMessage =
    "`SHOW CONSTRAINTS` no longer allows the `EXISTS` keyword, please use `EXIST` or `PROPERTY EXISTENCE` instead."

  override def clauseSpecificSemanticCheck: SemanticCheck = constraintType match {
    case ExistsConstraints(RemovedSyntax)     => error(existsErrorMessage, position)
    case NodeExistsConstraints(RemovedSyntax) => error(existsErrorMessage, position)
    case RelExistsConstraints(RemovedSyntax)  => error(existsErrorMessage, position)
    case _ if brief || verbose =>
      error(
        """`SHOW CONSTRAINTS` no longer allows the `BRIEF` and `VERBOSE` keywords,
          |please omit `BRIEF` and use `YIELD *` instead of `VERBOSE`.""".stripMargin,
        position
      )
    case _ => super.clauseSpecificSemanticCheck
  }
}

object ShowConstraintsClause {
  val idColumn = "id"
  val nameColumn = "name"
  val typeColumn = "type"
  val entityTypeColumn = "entityType"
  val labelsOrTypesColumn = "labelsOrTypes"
  val propertiesColumn = "properties"
  val ownedIndexColumn = "ownedIndex"
  val propertyTypeColumn = "propertyType"
  val optionsColumn = "options"
  val createStatementColumn = "createStatement"

  def apply(
    constraintType: ShowConstraintType,
    brief: Boolean,
    verbose: Boolean,
    where: Option[Where],
    yieldItems: List[CommandResultItem],
    yieldAll: Boolean
  )(position: InputPosition): ShowConstraintsClause = {
    val briefCols = List(
      ShowAndTerminateColumn(idColumn, CTInteger),
      ShowAndTerminateColumn(nameColumn),
      ShowAndTerminateColumn(typeColumn),
      ShowAndTerminateColumn(entityTypeColumn),
      ShowAndTerminateColumn(labelsOrTypesColumn, CTList(CTString)),
      ShowAndTerminateColumn(propertiesColumn, CTList(CTString)),
      ShowAndTerminateColumn(ownedIndexColumn),
      ShowAndTerminateColumn(propertyTypeColumn)
    )
    val verboseCols = List(
      ShowAndTerminateColumn(optionsColumn, CTMap),
      ShowAndTerminateColumn(createStatementColumn)
    )

    ShowConstraintsClause(
      briefCols,
      briefCols ++ verboseCols,
      constraintType,
      brief,
      verbose,
      where,
      yieldItems,
      yieldAll
    )(position)
  }
}

case class ShowProceduresClause(
  briefProcedureColumns: List[ShowAndTerminateColumn],
  allProcedureColumns: List[ShowAndTerminateColumn],
  executable: Option[ExecutableBy],
  where: Option[Where],
  yieldItems: List[CommandResultItem],
  yieldAll: Boolean
)(val position: InputPosition) extends CommandClause with CommandClauseAllowedOnSystem {
  override def name: String = "SHOW PROCEDURES"

  private val useAllColumns = yieldItems.nonEmpty || yieldAll

  val originalColumns: List[ShowAndTerminateColumn] =
    if (useAllColumns) allProcedureColumns else briefProcedureColumns

  private val briefColumns = briefProcedureColumns.map(c => ShowColumn(c.name, c.cypherType)(position))
  private val allColumns = allProcedureColumns.map(c => ShowColumn(c.name, c.cypherType)(position))

  val unfilteredColumns: DefaultOrAllShowColumns =
    DefaultOrAllShowColumns(useAllColumns, briefColumns, allColumns)

  override def moveWhereToProjection: CommandClause = copy(where = None)(position)
}

object ShowProceduresClause {
  val nameColumn = "name"
  val descriptionColumn = "description"
  val modeColumn = "mode"
  val worksOnSystemColumn = "worksOnSystem"
  val signatureColumn = "signature"
  val argumentDescriptionColumn = "argumentDescription"
  val returnDescriptionColumn = "returnDescription"
  val adminColumn = "admin"
  val rolesExecutionColumn = "rolesExecution"
  val rolesBoostedExecutionColumn = "rolesBoostedExecution"
  val isDeprecatedColumn = "isDeprecated"
  val optionColumn = "option"

  def apply(
    executable: Option[ExecutableBy],
    where: Option[Where],
    yieldItems: List[CommandResultItem],
    yieldAll: Boolean
  )(position: InputPosition): ShowProceduresClause = {
    val briefCols = List(
      ShowAndTerminateColumn(nameColumn),
      ShowAndTerminateColumn(descriptionColumn),
      ShowAndTerminateColumn(modeColumn),
      ShowAndTerminateColumn(worksOnSystemColumn, CTBoolean)
    )
    val verboseCols = List(
      ShowAndTerminateColumn(signatureColumn),
      ShowAndTerminateColumn(argumentDescriptionColumn, CTList(CTMap)),
      ShowAndTerminateColumn(returnDescriptionColumn, CTList(CTMap)),
      ShowAndTerminateColumn(adminColumn, CTBoolean),
      ShowAndTerminateColumn(rolesExecutionColumn, CTList(CTString)),
      ShowAndTerminateColumn(rolesBoostedExecutionColumn, CTList(CTString)),
      ShowAndTerminateColumn(isDeprecatedColumn, CTBoolean),
      ShowAndTerminateColumn(optionColumn, CTMap)
    )

    ShowProceduresClause(
      briefCols,
      briefCols ++ verboseCols,
      executable,
      where,
      yieldItems,
      yieldAll
    )(position)
  }
}

case class ShowFunctionsClause(
  briefFunctionColumns: List[ShowAndTerminateColumn],
  allFunctionColumns: List[ShowAndTerminateColumn],
  functionType: ShowFunctionType,
  executable: Option[ExecutableBy],
  where: Option[Where],
  yieldItems: List[CommandResultItem],
  yieldAll: Boolean
)(val position: InputPosition) extends CommandClause with CommandClauseAllowedOnSystem {
  override def name: String = "SHOW FUNCTIONS"

  private val useAllColumns = yieldItems.nonEmpty || yieldAll

  val originalColumns: List[ShowAndTerminateColumn] =
    if (useAllColumns) allFunctionColumns else briefFunctionColumns

  private val briefColumns = briefFunctionColumns.map(c => ShowColumn(c.name, c.cypherType)(position))
  private val allColumns = allFunctionColumns.map(c => ShowColumn(c.name, c.cypherType)(position))

  val unfilteredColumns: DefaultOrAllShowColumns =
    DefaultOrAllShowColumns(useAllColumns, briefColumns, allColumns)

  override def moveWhereToProjection: CommandClause = copy(where = None)(position)
}

object ShowFunctionsClause {
  val nameColumn = "name"
  val categoryColumn = "category"
  val descriptionColumn = "description"
  val signatureColumn = "signature"
  val isBuiltInColumn = "isBuiltIn"
  val argumentDescriptionColumn = "argumentDescription"
  val returnDescriptionColumn = "returnDescription"
  val aggregatingColumn = "aggregating"
  val rolesExecutionColumn = "rolesExecution"
  val rolesBoostedExecutionColumn = "rolesBoostedExecution"
  val isDeprecatedColumn = "isDeprecated"

  def apply(
    functionType: ShowFunctionType,
    executable: Option[ExecutableBy],
    where: Option[Where],
    yieldItems: List[CommandResultItem],
    yieldAll: Boolean
  )(position: InputPosition): ShowFunctionsClause = {
    val briefCols = List(
      ShowAndTerminateColumn(nameColumn),
      ShowAndTerminateColumn(categoryColumn),
      ShowAndTerminateColumn(descriptionColumn)
    )
    val verboseCols = List(
      ShowAndTerminateColumn(signatureColumn),
      ShowAndTerminateColumn(isBuiltInColumn, CTBoolean),
      ShowAndTerminateColumn(argumentDescriptionColumn, CTList(CTMap)),
      ShowAndTerminateColumn(returnDescriptionColumn),
      ShowAndTerminateColumn(aggregatingColumn, CTBoolean),
      ShowAndTerminateColumn(rolesExecutionColumn, CTList(CTString)),
      ShowAndTerminateColumn(rolesBoostedExecutionColumn, CTList(CTString)),
      ShowAndTerminateColumn(isDeprecatedColumn, CTBoolean)
    )

    ShowFunctionsClause(
      briefCols,
      briefCols ++ verboseCols,
      functionType,
      executable,
      where,
      yieldItems,
      yieldAll
    )(position)
  }
}

sealed trait TransactionsCommandClause extends CommandClauseWithNames with CommandClauseAllowedOnSystem

case class ShowTransactionsClause(
  briefTransactionColumns: List[ShowAndTerminateColumn],
  allTransactionColumns: List[ShowAndTerminateColumn],
  names: Either[List[String], Expression],
  where: Option[Where],
  yieldItems: List[CommandResultItem],
  yieldAll: Boolean
)(val position: InputPosition) extends TransactionsCommandClause {

  override def name: String = "SHOW TRANSACTIONS"

  private val useAllColumns = yieldItems.nonEmpty || yieldAll

  val originalColumns: List[ShowAndTerminateColumn] =
    if (useAllColumns) allTransactionColumns else briefTransactionColumns

  private val briefColumns = briefTransactionColumns.map(c => ShowColumn(c.name, c.cypherType)(position))
  private val allColumns = allTransactionColumns.map(c => ShowColumn(c.name, c.cypherType)(position))

  val unfilteredColumns: DefaultOrAllShowColumns =
    DefaultOrAllShowColumns(useAllColumns, briefColumns, allColumns)

  override def moveWhereToProjection: CommandClause = copy(where = None)(position)
}

object ShowTransactionsClause {
  val databaseColumn = "database"
  val transactionIdColumn = "transactionId"
  val currentQueryIdColumn = "currentQueryId"
  val outerTransactionIdColumn = "outerTransactionId"
  val connectionIdColumn = "connectionId"
  val clientAddressColumn = "clientAddress"
  val usernameColumn = "username"
  val metaDataColumn = "metaData"
  val currentQueryColumn = "currentQuery"
  val parametersColumn = "parameters"
  val plannerColumn = "planner"
  val runtimeColumn = "runtime"
  val indexesColumn = "indexes"
  val startTimeColumn = "startTime"
  val currentQueryStartTimeColumn = "currentQueryStartTime"
  val protocolColumn = "protocol"
  val requestUriColumn = "requestUri"
  val statusColumn = "status"
  val currentQueryStatusColumn = "currentQueryStatus"
  val statusDetailsColumn = "statusDetails"
  val resourceInformationColumn = "resourceInformation"
  val activeLockCountColumn = "activeLockCount"
  val currentQueryActiveLockCountColumn = "currentQueryActiveLockCount"
  val elapsedTimeColumn = "elapsedTime"
  val cpuTimeColumn = "cpuTime"
  val waitTimeColumn = "waitTime"
  val idleTimeColumn = "idleTime"
  val currentQueryElapsedTimeColumn = "currentQueryElapsedTime"
  val currentQueryCpuTimeColumn = "currentQueryCpuTime"
  val currentQueryWaitTimeColumn = "currentQueryWaitTime"
  val currentQueryIdleTimeColumn = "currentQueryIdleTime"
  val currentQueryAllocatedBytesColumn = "currentQueryAllocatedBytes"
  val allocatedDirectBytesColumn = "allocatedDirectBytes"
  val estimatedUsedHeapMemoryColumn = "estimatedUsedHeapMemory"
  val pageHitsColumn = "pageHits"
  val pageFaultsColumn = "pageFaults"
  val currentQueryPageHitsColumn = "currentQueryPageHits"
  val currentQueryPageFaultsColumn = "currentQueryPageFaults"
  val initializationStackTraceColumn = "initializationStackTrace"

  def apply(
    ids: Either[List[String], Expression],
    where: Option[Where],
    yieldItems: List[CommandResultItem],
    yieldAll: Boolean
  )(position: InputPosition): ShowTransactionsClause = {
    val columns = List(
      // (column, brief)
      (ShowAndTerminateColumn(databaseColumn), true),
      (ShowAndTerminateColumn(transactionIdColumn), true),
      (ShowAndTerminateColumn(currentQueryIdColumn), true),
      (ShowAndTerminateColumn(outerTransactionIdColumn), false),
      (ShowAndTerminateColumn(connectionIdColumn), true),
      (ShowAndTerminateColumn(clientAddressColumn), true),
      (ShowAndTerminateColumn(usernameColumn), true),
      (ShowAndTerminateColumn(metaDataColumn, CTMap), false),
      (ShowAndTerminateColumn(currentQueryColumn), true),
      (ShowAndTerminateColumn(parametersColumn, CTMap), false),
      (ShowAndTerminateColumn(plannerColumn), false),
      (ShowAndTerminateColumn(runtimeColumn), false),
      (ShowAndTerminateColumn(indexesColumn, CTList(CTMap)), false),
      (ShowAndTerminateColumn(startTimeColumn), true),
      (ShowAndTerminateColumn(currentQueryStartTimeColumn), false),
      (ShowAndTerminateColumn(protocolColumn), false),
      (ShowAndTerminateColumn(requestUriColumn), false),
      (ShowAndTerminateColumn(statusColumn), true),
      (ShowAndTerminateColumn(currentQueryStatusColumn), false),
      (ShowAndTerminateColumn(statusDetailsColumn), false),
      (ShowAndTerminateColumn(resourceInformationColumn, CTMap), false),
      (ShowAndTerminateColumn(activeLockCountColumn, CTInteger), false),
      (ShowAndTerminateColumn(currentQueryActiveLockCountColumn, CTInteger), false),
      (ShowAndTerminateColumn(elapsedTimeColumn, CTDuration), true),
      (ShowAndTerminateColumn(cpuTimeColumn, CTDuration), false),
      (ShowAndTerminateColumn(waitTimeColumn, CTDuration), false),
      (ShowAndTerminateColumn(idleTimeColumn, CTDuration), false),
      (ShowAndTerminateColumn(currentQueryElapsedTimeColumn, CTDuration), false),
      (ShowAndTerminateColumn(currentQueryCpuTimeColumn, CTDuration), false),
      (ShowAndTerminateColumn(currentQueryWaitTimeColumn, CTDuration), false),
      (ShowAndTerminateColumn(currentQueryIdleTimeColumn, CTDuration), false),
      (ShowAndTerminateColumn(currentQueryAllocatedBytesColumn, CTInteger), false),
      (ShowAndTerminateColumn(allocatedDirectBytesColumn, CTInteger), false),
      (ShowAndTerminateColumn(estimatedUsedHeapMemoryColumn, CTInteger), false),
      (ShowAndTerminateColumn(pageHitsColumn, CTInteger), false),
      (ShowAndTerminateColumn(pageFaultsColumn, CTInteger), false),
      (ShowAndTerminateColumn(currentQueryPageHitsColumn, CTInteger), false),
      (ShowAndTerminateColumn(currentQueryPageFaultsColumn, CTInteger), false),
      (ShowAndTerminateColumn(initializationStackTraceColumn), false)
    )
    val briefColumns = columns.filter(_._2).map(_._1)
    val allColumns = columns.map(_._1)

    ShowTransactionsClause(
      briefColumns,
      allColumns,
      ids,
      where,
      yieldItems,
      yieldAll
    )(position)
  }
}

case class TerminateTransactionsClause(
  originalColumns: List[ShowAndTerminateColumn],
  names: Either[List[String], Expression],
  yieldItems: List[CommandResultItem],
  yieldAll: Boolean,
  wherePos: Option[InputPosition]
)(val position: InputPosition) extends TransactionsCommandClause {

  override def name: String = "TERMINATE TRANSACTIONS"

  private val columns = originalColumns.map(c => ShowColumn(c.name, c.cypherType)(position))

  val unfilteredColumns: DefaultOrAllShowColumns =
    DefaultOrAllShowColumns(useAllColumns = yieldItems.nonEmpty || yieldAll, columns, columns)

  override def clauseSpecificSemanticCheck: SemanticCheck = when(names match {
    case Left(ls) => ls.size < 1
    case Right(_) => false // expression list length needs to be checked at runtime
  }) {
    error("Missing transaction id to terminate, the transaction id can be found using `SHOW TRANSACTIONS`", position)
  } chain when(wherePos.isDefined) {
    error(
      "`WHERE` is not allowed by itself, please use `TERMINATE TRANSACTION ... YIELD ... WHERE ...` instead",
      wherePos.get
    )
  } chain super.clauseSpecificSemanticCheck

  override def where: Option[Where] = None
  override def moveWhereToProjection: CommandClause = this
}

object TerminateTransactionsClause {
  val transactionIdColumn = "transactionId"
  val usernameColumn = "username"
  val messageColumn = "message"

  def apply(
    ids: Either[List[String], Expression],
    yieldItems: List[CommandResultItem],
    yieldAll: Boolean,
    wherePos: Option[InputPosition]
  )(position: InputPosition): TerminateTransactionsClause = {
    // All columns are currently default
    val columns = List(
      ShowAndTerminateColumn(transactionIdColumn),
      ShowAndTerminateColumn(usernameColumn),
      ShowAndTerminateColumn(messageColumn)
    )

    TerminateTransactionsClause(
      columns,
      ids,
      yieldItems,
      yieldAll,
      wherePos
    )(position)
  }
}

case class ShowSettingsClause(
  briefSettingColumns: List[ShowAndTerminateColumn],
  allSettingColumns: List[ShowAndTerminateColumn],
  names: Either[List[String], Expression],
  where: Option[Where],
  yieldItems: List[CommandResultItem],
  yieldAll: Boolean
)(val position: InputPosition) extends CommandClauseWithNames with CommandClauseAllowedOnSystem {

  override def name: String = "SHOW SETTINGS"

  private val useAllColumns = yieldItems.nonEmpty || yieldAll

  val originalColumns: List[ShowAndTerminateColumn] =
    if (useAllColumns) allSettingColumns else briefSettingColumns

  private val briefColumns = briefSettingColumns.map(c => ShowColumn(c.name, c.cypherType)(position))
  private val allColumns = allSettingColumns.map(c => ShowColumn(c.name, c.cypherType)(position))

  val unfilteredColumns: DefaultOrAllShowColumns =
    DefaultOrAllShowColumns(useAllColumns, briefColumns, allColumns)

  override def moveWhereToProjection: CommandClause = copy(where = None)(position)

  override def clauseSpecificSemanticCheck: SemanticCheck = {
    requireFeatureSupport(
      s"The `$name` clause",
      SemanticFeature.ShowSetting,
      position
    ) chain super.clauseSpecificSemanticCheck
  }
}

object ShowSettingsClause {
  val nameColumn = "name"
  val valueColumn = "value"
  val isDynamicColumn = "isDynamic"
  val defaultValueColumn = "defaultValue"
  val descriptionColumn = "description"
  val startupValueColumn = "startupValue"
  val isExplicitlySetColumn = "isExplicitlySet"
  val validValuesColumn = "validValues"
  val isDeprecatedColumn = "isDeprecated"

  def apply(
    names: Either[List[String], Expression],
    where: Option[Where],
    yieldItems: List[CommandResultItem],
    yieldAll: Boolean
  )(position: InputPosition): ShowSettingsClause = {
    val defaultCols = List(
      ShowAndTerminateColumn(nameColumn),
      ShowAndTerminateColumn(valueColumn),
      ShowAndTerminateColumn(isDynamicColumn, CTBoolean),
      ShowAndTerminateColumn(defaultValueColumn),
      ShowAndTerminateColumn(descriptionColumn)
    )
    val verboseCols = List(
      ShowAndTerminateColumn(startupValueColumn),
      ShowAndTerminateColumn(isExplicitlySetColumn, CTBoolean),
      ShowAndTerminateColumn(validValuesColumn),
      ShowAndTerminateColumn(isDeprecatedColumn, CTBoolean)
    )

    ShowSettingsClause(
      defaultCols,
      defaultCols ++ verboseCols,
      names,
      where,
      yieldItems,
      yieldAll
    )(position)
  }
}
